/*
 * Copyright 2018-present HiveMQ and the HiveMQ Community
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hivemq.client.internal.mqtt.handler.quic;

import com.hivemq.client.internal.mqtt.MqttClientConfig;
import com.hivemq.client.internal.mqtt.MqttClientSslConfigImpl;
import com.hivemq.client.internal.mqtt.MqttClientTransportConfigImpl;
import com.hivemq.client.internal.mqtt.MqttQuicConfigImpl;
import com.hivemq.client.internal.mqtt.codec.encoder.MqttEncoder;
import com.hivemq.client.internal.mqtt.handler.auth.MqttAuthHandler;
import com.hivemq.client.internal.mqtt.handler.connect.MqttConnAckFlow;
import com.hivemq.client.internal.mqtt.handler.connect.MqttConnAckSingle;
import com.hivemq.client.internal.mqtt.handler.connect.MqttConnectHandler;
import com.hivemq.client.internal.mqtt.handler.disconnect.MqttDisconnectHandler;
import com.hivemq.client.internal.mqtt.ioc.ConnectionScope;
import com.hivemq.client.internal.mqtt.message.connect.MqttConnect;
import com.hivemq.client.internal.netty.NettyEventLoopProvider;
import com.hivemq.client.mqtt.exceptions.ConnectionFailedException;
import com.hivemq.client.mqtt.lifecycle.MqttDisconnectSource;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.quic.Quic;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.codec.quic.QuicStreamType;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.DefaultAddressResolverGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connects a MQTT client via a QUIC transport:
 * <ol>
 *   <li>binds a datagram channel with a QUIC client codec
 *   <li>connects a {@link QuicChannel} (includes the TLS 1.3 handshake)
 *   <li>opens a single bidirectional stream channel which carries the MQTT session
 *   <li>adds the basic MQTT handlers to the stream channel:
 *       Encoder, AuthHandler, ConnectHandler, DisconnectHandler
 * </ol>
 *
 * @author HiveMQ
 */
@ConnectionScope
public class MqttQuicInitializer {

    private static final @NotNull String ENDPOINT_IDENTIFICATION_ALGORITHM = "HTTPS";
    private static final @NotNull String ALPN = "mqtt";

    private final @NotNull MqttClientConfig clientConfig;
    private final @NotNull MqttConnect connect;
    private final @NotNull MqttConnAckFlow connAckFlow;

    private final @NotNull MqttEncoder encoder;
    private final @NotNull MqttConnectHandler connectHandler;
    private final @NotNull MqttDisconnectHandler disconnectHandler;
    private final @NotNull MqttAuthHandler authHandler;

    @Inject
    MqttQuicInitializer(
            final @NotNull MqttClientConfig clientConfig,
            final @NotNull MqttConnect connect,
            final @NotNull MqttConnAckFlow connAckFlow,
            final @NotNull MqttEncoder encoder,
            final @NotNull MqttConnectHandler connectHandler,
            final @NotNull MqttDisconnectHandler disconnectHandler,
            final @NotNull MqttAuthHandler authHandler) {

        this.clientConfig = clientConfig;
        this.connect = connect;
        this.connAckFlow = connAckFlow;
        this.encoder = encoder;
        this.connectHandler = connectHandler;
        this.disconnectHandler = disconnectHandler;
        this.authHandler = authHandler;
    }

    public void connect(final @NotNull EventLoop eventLoop) {
        if (!Quic.isAvailable()) {
            onError(eventLoop, new UnsupportedOperationException(
                    "QUIC is not available. Add the platform specific netty-codec-native-quic dependency."));
            return;
        }

        final MqttClientTransportConfigImpl transportConfig = clientConfig.getCurrentTransportConfig();
        final MqttQuicConfigImpl quicConfig = transportConfig.getRawQuicConfig();
        final MqttClientSslConfigImpl sslConfig = transportConfig.getRawSslConfig();
        assert (quicConfig != null) && (sslConfig != null);

        final QuicSslContext sslContext;
        final ChannelHandler codec;
        try {
            sslContext = createQuicSslContext(sslConfig);
            // the default sslEngineProvider creates engines without peer host (no SNI / hostname verification),
            // so create engines with the peer host explicitly
            final String serverHost = transportConfig.getServerAddress().getHostString();
            final int serverPort = transportConfig.getServerAddress().getPort();
            codec = new QuicClientCodecBuilder().sslEngineProvider(
                            channel -> sslContext.newEngine(channel.alloc(), serverHost, serverPort))
                    .maxIdleTimeout(quicConfig.getMaxIdleTimeoutMs(), TimeUnit.MILLISECONDS)
                    .initialMaxData(quicConfig.getInitialMaxData())
                    .initialMaxStreamDataBidirectionalLocal(quicConfig.getInitialMaxStreamDataBidirectionalLocal())
                    .initialMaxStreamDataBidirectionalRemote(quicConfig.getInitialMaxStreamDataBidirectionalRemote())
                    .initialMaxStreamsBidirectional(quicConfig.getInitialMaxStreamsBidirectional())
                    .build();
        } catch (final Throwable t) {
            onError(eventLoop, t);
            return;
        }

        final InetSocketAddress localAddress = transportConfig.getRawLocalAddress();
        final Bootstrap bootstrap = new Bootstrap().group(eventLoop)
                .channelFactory(NettyEventLoopProvider.INSTANCE.getDatagramChannelFactory())
                .handler(codec);

        // the QUIC native code requires a resolved remote address (unlike TCP connect)
        resolve(transportConfig.getServerAddress(), eventLoop).addListener(resolveFuture -> {
            if (resolveFuture.isSuccess()) {
                final InetSocketAddress serverAddress = (InetSocketAddress) resolveFuture.getNow();
                bootstrap.bind((localAddress == null) ? new InetSocketAddress(0) : localAddress)
                        .addListener(bindFuture -> {
                            if (bindFuture.isSuccess()) {
                                connectQuic(((ChannelFuture) bindFuture).channel(), serverAddress, transportConfig,
                                        sslConfig, eventLoop);
                            } else {
                                onError(eventLoop, bindFuture.cause());
                            }
                        });
            } else {
                onError(eventLoop, resolveFuture.cause());
            }
        });
    }

    private static @NotNull Future<InetSocketAddress> resolve(
            final @NotNull InetSocketAddress address, final @NotNull EventLoop eventLoop) {

        if (!address.isUnresolved()) {
            return eventLoop.newSucceededFuture(address);
        }

        final Promise<InetSocketAddress> promise = eventLoop.newPromise();
        // DefaultAddressResolverGroup caches one resolver per event loop; do not close it
        final AddressResolver<InetSocketAddress> resolver = DefaultAddressResolverGroup.INSTANCE.getResolver(eventLoop);
        resolver.resolve(address).addListener(resolveFuture -> {
            if (resolveFuture.isSuccess()) {
                final InetSocketAddress resolved = (InetSocketAddress) resolveFuture.getNow();
                try {
                    // keep the hostname for SNI and hostname verification
                    final InetAddress inetAddress =
                            InetAddress.getByAddress(address.getHostString(), resolved.getAddress().getAddress());
                    promise.setSuccess(new InetSocketAddress(inetAddress, resolved.getPort()));
                } catch (final Throwable t) {
                    promise.setFailure(t);
                }
            } else {
                promise.setFailure(resolveFuture.cause());
            }
        });
        return promise;
    }

    private void connectQuic(
            final @NotNull Channel datagramChannel,
            final @NotNull InetSocketAddress serverAddress,
            final @NotNull MqttClientTransportConfigImpl transportConfig,
            final @NotNull MqttClientSslConfigImpl sslConfig,
            final @NotNull EventLoop eventLoop) {

        final AtomicBoolean done = new AtomicBoolean();

        final Future<QuicChannel> connectFuture = QuicChannel.newBootstrap(datagramChannel)
                .handler(new ChannelInboundHandlerAdapter() {})
                // the server never opens streams for MQTT, close them immediately if it does
                .streamHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(final @NotNull Channel channel) {
                        channel.close();
                    }
                })
                .remoteAddress(serverAddress)
                .connect();

        final int connectTimeoutMs = transportConfig.getSocketConnectTimeoutMs();
        final ScheduledFuture<?> timeoutFuture;
        if (connectTimeoutMs > 0) {
            timeoutFuture = eventLoop.schedule(() -> {
                if (done.compareAndSet(false, true)) {
                    datagramChannel.close();
                    onError(eventLoop,
                            new ConnectException("QUIC connect timeout: " + serverAddress));
                }
            }, connectTimeoutMs, TimeUnit.MILLISECONDS);
        } else {
            timeoutFuture = null;
        }

        connectFuture.addListener(future -> {
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
            }
            if (!done.compareAndSet(false, true)) {
                quicCloseQuietly(future);
                return;
            }
            if (future.isSuccess()) {
                final QuicChannel quicChannel = (QuicChannel) future.getNow();
                quicChannel.closeFuture().addListener(closed -> datagramChannel.close());
                onQuicConnected(quicChannel, transportConfig, sslConfig, eventLoop);
            } else {
                datagramChannel.close();
                onError(eventLoop, future.cause());
            }
        });
    }

    private void onQuicConnected(
            final @NotNull QuicChannel quicChannel,
            final @NotNull MqttClientTransportConfigImpl transportConfig,
            final @NotNull MqttClientSslConfigImpl sslConfig,
            final @NotNull EventLoop eventLoop) {

        final HostnameVerifier hostnameVerifier = sslConfig.getRawHostnameVerifier();
        if (hostnameVerifier != null) {
            final String host = transportConfig.getServerAddress().getHostString();
            final boolean verified;
            try {
                verified = hostnameVerifier.verify(host, quicChannel.sslEngine().getSession());
            } catch (final Throwable t) {
                quicChannel.close();
                onError(eventLoop, t);
                return;
            }
            if (!verified) {
                quicChannel.close();
                onError(eventLoop, new SSLException("Hostname verification failed for: " + host));
                return;
            }
        }

        quicChannel.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInitializer<Channel>() {
            @Override
            protected void initChannel(final @NotNull Channel channel) {
                channel.pipeline()
                        .addLast(MqttEncoder.NAME, encoder)
                        .addLast(MqttAuthHandler.NAME, authHandler)
                        .addLast(MqttConnectHandler.NAME, connectHandler)
                        .addLast(MqttDisconnectHandler.NAME, disconnectHandler);
            }
        }).addListener((Future<QuicStreamChannel> streamFuture) -> {
            if (streamFuture.isSuccess()) {
                final QuicStreamChannel streamChannel = streamFuture.getNow();
                streamChannel.closeFuture().addListener(closed -> quicChannel.close());
            } else {
                quicChannel.close();
                onError(eventLoop, streamFuture.cause());
            }
        });
    }

    private void quicCloseQuietly(final @NotNull Future<?> future) {
        if (future.isSuccess()) {
            ((QuicChannel) future.getNow()).close();
        }
    }

    private void onError(final @NotNull EventLoop eventLoop, final @NotNull Throwable cause) {
        MqttConnAckSingle.reconnect(clientConfig, MqttDisconnectSource.CLIENT, new ConnectionFailedException(cause),
                connect, connAckFlow, eventLoop);
    }

    static @NotNull QuicSslContext createQuicSslContext(final @NotNull MqttClientSslConfigImpl sslConfig)
            throws SSLException {

        return QuicSslContextBuilder.forClient()
                .trustManager(sslConfig.getRawTrustManagerFactory())
                .keyManager(sslConfig.getRawKeyManagerFactory(), null)
                .applicationProtocols(ALPN)
                .endpointIdentificationAlgorithm(
                        (sslConfig.getRawHostnameVerifier() == null) ? ENDPOINT_IDENTIFICATION_ALGORITHM : null)
                .build();
    }
}
