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
import com.hivemq.client.internal.mqtt.handler.MqttChannelInitializer;
import com.hivemq.client.internal.mqtt.handler.connect.MqttConnAckFlow;
import com.hivemq.client.internal.mqtt.handler.connect.MqttConnAckSingle;
import com.hivemq.client.internal.mqtt.message.connect.MqttConnect;
import com.hivemq.client.internal.netty.NettyEventLoopProvider;
import com.hivemq.client.mqtt.exceptions.ConnectionFailedException;
import com.hivemq.client.mqtt.lifecycle.MqttDisconnectSource;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.handler.codec.quic.Quic;
import io.netty.handler.codec.quic.QuicChannel;
import io.netty.handler.codec.quic.QuicClientCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslEngine;
import io.netty.handler.codec.quic.QuicStreamType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Connects MQTT over a single bidirectional QUIC stream.
 *
 * @author Silvio Giebl
 */
public final class MqttQuicConnector {

    private static final long DEFAULT_INITIAL_MAX_DATA = 10_000_000L;
    private static final long DEFAULT_INITIAL_MAX_STREAM_DATA = 1_000_000L;
    private static final long MIN_DERIVED_IDLE_TIMEOUT_MS = 30_000L;
    private static final long FALLBACK_IDLE_TIMEOUT_MS = 300_000L;

    public static void connect(
            final @NotNull MqttClientConfig clientConfig,
            final @NotNull MqttConnect connect,
            final @NotNull MqttConnAckFlow flow,
            final @NotNull EventLoop eventLoop,
            final @NotNull MqttChannelInitializer channelInitializer) {

        if (!Quic.isAvailable()) {
            fail(clientConfig, connect, flow, eventLoop, new IllegalStateException(
                    "QUIC native library is not available.", Quic.unavailabilityCause()));
            return;
        }

        final MqttClientTransportConfigImpl transportConfig = clientConfig.getCurrentTransportConfig();
        final MqttQuicConfigImpl quicConfig = transportConfig.getRawQuicConfig();
        assert quicConfig != null;
        final MqttClientSslConfigImpl sslConfig =
                (transportConfig.getRawSslConfig() == null) ? MqttClientSslConfigImpl.DEFAULT :
                        transportConfig.getRawSslConfig();
        final InetSocketAddress serverAddress;
        try {
            serverAddress = resolve(transportConfig.getServerAddress());
        } catch (final UnknownHostException e) {
            fail(clientConfig, connect, flow, eventLoop, e);
            return;
        }

        final QuicSslContext sslContext;
        try {
            sslContext = MqttQuicSsl.sslContext(clientConfig, sslConfig, quicConfig);
        } catch (final Throwable t) {
            fail(clientConfig, connect, flow, eventLoop, t);
            return;
        }

        final QuicClientCodecBuilder codecBuilder = new QuicClientCodecBuilder().sslContext(sslContext)
                .sslEngineProvider(quicChannel -> (QuicSslEngine) sslContext.newEngine(quicChannel.alloc(),
                        serverAddress.getHostString(), serverAddress.getPort()))
                .initialMaxData(DEFAULT_INITIAL_MAX_DATA)
                .initialMaxStreamDataBidirectionalLocal(DEFAULT_INITIAL_MAX_STREAM_DATA)
                .initialMaxStreamDataBidirectionalRemote(DEFAULT_INITIAL_MAX_STREAM_DATA)
                .initialMaxStreamsBidirectional(0);
        final long idleTimeoutMs = idleTimeoutMs(quicConfig, connect);
        if (idleTimeoutMs > 0) {
            codecBuilder.maxIdleTimeout(idleTimeoutMs, TimeUnit.MILLISECONDS);
        }

        final AtomicBoolean done = new AtomicBoolean();
        final int timeoutMs = connectTimeoutMs(transportConfig, quicConfig);
        final io.netty.util.concurrent.ScheduledFuture<?> timeoutFuture;
        if (timeoutMs > 0) {
            timeoutFuture = eventLoop.schedule(() -> {
                if (done.compareAndSet(false, true)) {
                    fail(clientConfig, connect, flow, eventLoop,
                            new SSLException("QUIC connect timed out after " + timeoutMs + " ms."));
                }
            }, timeoutMs, TimeUnit.MILLISECONDS);
        } else {
            timeoutFuture = null;
        }

        final SocketAddress bindAddress = bindAddress(transportConfig);
        new Bootstrap().group(eventLoop)
                .channelFactory(NettyEventLoopProvider.INSTANCE.getDatagramChannelFactory())
                .handler(codecBuilder.build())
                .bind(bindAddress)
                .addListener((ChannelFuture bindFuture) -> {
                    if (done.get()) {
                        if (bindFuture.isSuccess()) {
                            bindFuture.channel().close();
                        }
                        return;
                    }
                    if (!bindFuture.isSuccess()) {
                        complete(done, timeoutFuture);
                        fail(clientConfig, connect, flow, eventLoop, bindFuture.cause());
                        return;
                    }
                    connectQuic(bindFuture.channel(), serverAddress, sslConfig, channelInitializer, clientConfig,
                            connect, flow, eventLoop, done, timeoutFuture);
                });
    }

    private static void connectQuic(
            final @NotNull Channel datagramChannel,
            final @NotNull InetSocketAddress serverAddress,
            final @NotNull MqttClientSslConfigImpl sslConfig,
            final @NotNull MqttChannelInitializer channelInitializer,
            final @NotNull MqttClientConfig clientConfig,
            final @NotNull MqttConnect connect,
            final @NotNull MqttConnAckFlow flow,
            final @NotNull EventLoop eventLoop,
            final @NotNull AtomicBoolean done,
            final @Nullable io.netty.util.concurrent.ScheduledFuture<?> timeoutFuture) {

        QuicChannel.newBootstrap(datagramChannel)
                .handler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelInactive(final @NotNull ChannelHandlerContext ctx) {
                        ctx.fireChannelInactive();
                        final Channel parent = ctx.channel().parent();
                        if (parent != null) {
                            parent.close();
                        }
                    }
                })
                .streamHandler(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelActive(final @NotNull ChannelHandlerContext ctx) {
                        ctx.close();
                    }
                })
                .remoteAddress(serverAddress)
                .connect()
                .addListener(quicFuture -> {
                    if (done.get()) {
                        datagramChannel.close();
                        if (quicFuture.isSuccess()) {
                            ((Channel) quicFuture.getNow()).close();
                        }
                        return;
                    }
                    if (!quicFuture.isSuccess()) {
                        complete(done, timeoutFuture);
                        datagramChannel.close();
                        fail(clientConfig, connect, flow, eventLoop, quicFuture.cause());
                        return;
                    }
                    final QuicChannel quicChannel = (QuicChannel) quicFuture.getNow();
                    if (!verifyHostname(sslConfig, serverAddress, quicChannel)) {
                        complete(done, timeoutFuture);
                        quicChannel.close();
                        datagramChannel.close();
                        fail(clientConfig, connect, flow, eventLoop,
                                new SSLException("Hostname verification failed for " + serverAddress.getHostString()));
                        return;
                    }
                    createStream(quicChannel, datagramChannel, channelInitializer, clientConfig, connect, flow,
                            eventLoop, done, timeoutFuture);
                });
    }

    private static void createStream(
            final @NotNull QuicChannel quicChannel,
            final @NotNull Channel datagramChannel,
            final @NotNull MqttChannelInitializer channelInitializer,
            final @NotNull MqttClientConfig clientConfig,
            final @NotNull MqttConnect connect,
            final @NotNull MqttConnAckFlow flow,
            final @NotNull EventLoop eventLoop,
            final @NotNull AtomicBoolean done,
            final @Nullable io.netty.util.concurrent.ScheduledFuture<?> timeoutFuture) {

        quicChannel.createStream(QuicStreamType.BIDIRECTIONAL, new ChannelInboundHandlerAdapter() {
            @Override
            public void handlerAdded(final @NotNull ChannelHandlerContext ctx) {
                ctx.pipeline().remove(this);
                ctx.pipeline().addLast(MqttQuicParentCloser.NAME, new MqttQuicParentCloser());
                channelInitializer.initMqtt(ctx.channel());
            }
        }).addListener(streamFuture -> {
            if (done.get()) {
                quicChannel.close();
                datagramChannel.close();
                if (streamFuture.isSuccess()) {
                    ((Channel) streamFuture.getNow()).close();
                }
                return;
            }
            if (!streamFuture.isSuccess()) {
                complete(done, timeoutFuture);
                quicChannel.close();
                datagramChannel.close();
                fail(clientConfig, connect, flow, eventLoop, streamFuture.cause());
                return;
            }
            complete(done, timeoutFuture);
        });
    }

    private static boolean verifyHostname(
            final @NotNull MqttClientSslConfigImpl sslConfig,
            final @NotNull InetSocketAddress serverAddress,
            final @NotNull QuicChannel quicChannel) {

        final HostnameVerifier hostnameVerifier = sslConfig.getRawHostnameVerifier();
        if (hostnameVerifier == null) {
            return true;
        }
        final SSLEngine sslEngine = quicChannel.sslEngine();
        return (sslEngine != null) && hostnameVerifier.verify(serverAddress.getHostString(), sslEngine.getSession());
    }

    private static @NotNull InetSocketAddress resolve(final @NotNull InetSocketAddress address)
            throws UnknownHostException {
        if (!address.isUnresolved() && (address.getAddress() != null)) {
            return address;
        }
        final InetSocketAddress resolved = new InetSocketAddress(address.getHostString(), address.getPort());
        if (resolved.isUnresolved() || (resolved.getAddress() == null)) {
            throw new UnknownHostException(address.getHostString());
        }
        return resolved;
    }

    private static @NotNull SocketAddress bindAddress(final @NotNull MqttClientTransportConfigImpl transportConfig) {
        final InetSocketAddress localAddress = transportConfig.getRawLocalAddress();
        if (localAddress != null) {
            return localAddress;
        }
        return new InetSocketAddress(0);
    }

    private static int connectTimeoutMs(
            final @NotNull MqttClientTransportConfigImpl transportConfig, final @NotNull MqttQuicConfigImpl quicConfig) {

        return Math.max(transportConfig.getSocketConnectTimeoutMs(), quicConfig.getHandshakeTimeoutMs());
    }

    private static long idleTimeoutMs(final @NotNull MqttQuicConfigImpl quicConfig, final @NotNull MqttConnect connect) {
        if (quicConfig.getMaxIdleTimeoutMs() > 0) {
            return quicConfig.getMaxIdleTimeoutMs();
        }
        final int keepAlive = connect.getKeepAlive();
        if (keepAlive > 0) {
            return Math.max(MIN_DERIVED_IDLE_TIMEOUT_MS, keepAlive * 2L * 1000L);
        }
        return FALLBACK_IDLE_TIMEOUT_MS;
    }

    private static void complete(
            final @NotNull AtomicBoolean done,
            final @Nullable io.netty.util.concurrent.ScheduledFuture<?> timeoutFuture) {

        done.set(true);
        if (timeoutFuture != null) {
            timeoutFuture.cancel(false);
        }
    }

    private static void fail(
            final @NotNull MqttClientConfig clientConfig,
            final @NotNull MqttConnect connect,
            final @NotNull MqttConnAckFlow flow,
            final @NotNull EventLoop eventLoop,
            final @NotNull Throwable cause) {

        final ConnectionFailedException e = new ConnectionFailedException(cause);
        if (eventLoop.inEventLoop()) {
            MqttConnAckSingle.reconnect(clientConfig, MqttDisconnectSource.CLIENT, e, connect, flow, eventLoop);
        } else {
            eventLoop.execute(
                    () -> MqttConnAckSingle.reconnect(clientConfig, MqttDisconnectSource.CLIENT, e, connect, flow,
                            eventLoop));
        }
    }

    private MqttQuicConnector() {}
}
