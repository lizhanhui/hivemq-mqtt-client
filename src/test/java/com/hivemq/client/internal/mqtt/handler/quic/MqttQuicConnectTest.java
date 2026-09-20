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

import com.hivemq.client.internal.util.ClassUtil;
import com.hivemq.client.mqtt.MqttClientExecutorConfig;
import com.hivemq.client.mqtt.MqttClientSslConfig;
import com.hivemq.client.mqtt.MqttClientState;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.MultithreadEventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.Quic;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.TrustManagerFactory;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Loopback test: connects a MQTT client over QUIC to an in-process QUIC server and verifies the CONNECT/CONNACK
 * handshake (including TLS 1.3 with default hostname verification) and a QoS 0 publish.
 *
 * @author HiveMQ
 */
class MqttQuicConnectTest {

    // MQTT 5 CONNACK: fixed header, remaining length 3, ack flags 0, reason code SUCCESS, no properties
    private static final byte[] CONNACK = {0x20, 0x03, 0x00, 0x00, 0x00};
    private static final long VALIDITY_MS = TimeUnit.HOURS.toMillis(1);

    private @NotNull MultiThreadIoEventLoopGroup group;
    private @NotNull Channel serverChannel;
    private @NotNull ServerMqttHandler serverHandler;
    private @NotNull X509Certificate caCertificate;

    @BeforeEach
    void setUp() throws Exception {
        assumeTrue(Quic.isAvailable(), "QUIC native library is not available");
        group = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
        serverHandler = new ServerMqttHandler();

        // CA + server certificate with SAN "localhost" so default HTTPS endpoint identification succeeds
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        final KeyPair caKeyPair = keyPairGenerator.generateKeyPair();
        final KeyPair serverKeyPair = keyPairGenerator.generateKeyPair();

        final Date notBefore = new Date(System.currentTimeMillis() - 60_000);
        final Date notAfter = new Date(System.currentTimeMillis() + VALIDITY_MS);

        final ContentSigner caSigner = new JcaContentSignerBuilder("SHA256withRSA").build(caKeyPair.getPrivate());
        final X509v3CertificateBuilder caBuilder = new JcaX509v3CertificateBuilder(new X500Name("CN=QUIC test CA"),
                BigInteger.ONE, notBefore, notAfter, new X500Name("CN=QUIC test CA"), caKeyPair.getPublic());
        caBuilder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        caBuilder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.keyCertSign));
        caCertificate = new JcaX509CertificateConverter().getCertificate(caBuilder.build(caSigner));

        final X509v3CertificateBuilder serverBuilder = new JcaX509v3CertificateBuilder(new X500Name("CN=QUIC test CA"),
                BigInteger.TWO, notBefore, notAfter, new X500Name("CN=localhost"), serverKeyPair.getPublic());
        serverBuilder.addExtension(Extension.subjectAlternativeName, false,
                new GeneralNames(new GeneralName(GeneralName.dNSName, "localhost")));
        final X509Certificate serverCertificate =
                new JcaX509CertificateConverter().getCertificate(serverBuilder.build(caSigner));

        final QuicSslContext sslContext = QuicSslContextBuilder.forServer(serverKeyPair.getPrivate(), null,
                        serverCertificate, caCertificate)
                .applicationProtocols("mqtt")
                .build();
        final ChannelHandler codec = new QuicServerCodecBuilder().sslContext(sslContext)
                .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                .initialMaxData(10_000_000)
                .initialMaxStreamDataBidirectionalLocal(1_000_000)
                .initialMaxStreamDataBidirectionalRemote(1_000_000)
                .initialMaxStreamsBidirectional(16)
                .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(final @NotNull QuicStreamChannel ch) {
                        ch.pipeline().addLast(serverHandler);
                    }
                })
                .build();

        serverChannel = new Bootstrap().group(group)
                .channel(NioDatagramChannel.class)
                .handler(codec)
                .bind(new InetSocketAddress("localhost", 0))
                .sync()
                .channel();
    }

    @AfterEach
    void tearDown() {
        serverChannel.close();
        group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS);
    }

    @Test
    @Timeout(30)
    void connectAndPublish() throws Exception {
        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("ca", caCertificate);
        final TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);

        final Mqtt5Client client = Mqtt5Client.builder()
                .identifier("quic-test-client")
                .serverHost("localhost")
                .serverPort(((InetSocketAddress) serverChannel.localAddress()).getPort())
                .sslConfig(MqttClientSslConfig.builder().trustManagerFactory(trustManagerFactory).build())
                .quicWithDefaultConfig()
                .build();

        final Mqtt5ConnAck connAck = client.toBlocking().connect();
        assertFalse(connAck.getReasonCode().isError());
        assertTrue(serverHandler.connectLatch.await(10, TimeUnit.SECONDS));

        client.toBlocking()
                .publishWith()
                .topic("test/topic")
                .payload("hello".getBytes(StandardCharsets.UTF_8))
                .send();
        assertTrue(serverHandler.publishLatch.await(10, TimeUnit.SECONDS));
        assertTrue(serverHandler.publishPayload.toString().contains("hello"));

        client.toBlocking().disconnect();
    }

    @Test
    @Timeout(30)
    void reconnectThenSecondClientOnSameEventLoop() throws Exception {
        final TrustManagerFactory trustManagerFactory = trustManagerFactory();
        final MultithreadEventLoopGroup clientGroup = newClientEventLoopGroup();
        final MqttClientExecutorConfig executorConfig =
                MqttClientExecutorConfig.builder().nettyExecutor(clientGroup).nettyThreads(1).build();
        final CountDownLatch connectedTwice = new CountDownLatch(2);

        final Mqtt5BlockingClient client1 = Mqtt5Client.builder()
                .identifier("quic-reconnect-client")
                .serverHost("localhost")
                .serverPort(((InetSocketAddress) serverChannel.localAddress()).getPort())
                .sslConfig(MqttClientSslConfig.builder().trustManagerFactory(trustManagerFactory).build())
                .quicWithDefaultConfig()
                .executorConfig(executorConfig)
                .automaticReconnect()
                .initialDelay(100, TimeUnit.MILLISECONDS)
                .maxDelay(200, TimeUnit.MILLISECONDS)
                .applyAutomaticReconnect()
                .addConnectedListener(context -> connectedTwice.countDown())
                .buildBlocking();

        try {
            final Mqtt5ConnAck connAck = client1.connect();
            assertFalse(connAck.getReasonCode().isError());
            assertTrue(serverHandler.connectLatch.await(10, TimeUnit.SECONDS));
            assertEquals(1, serverHandler.connectCount.get());

            final ChannelHandlerContext firstStream = serverHandler.lastStream.get();
            assertNotNull(firstStream);
            // close the QUIC connection (not only the stream) so the client sees a full transport drop
            firstStream.channel().parent().close().sync();

            assertTrue(serverHandler.secondConnectLatch.await(10, TimeUnit.SECONDS),
                    "auto-reconnect must resolve again on the shared event loop");
            assertTrue(connectedTwice.await(10, TimeUnit.SECONDS));
            assertEquals(MqttClientState.CONNECTED, client1.getState());

            final Mqtt5BlockingClient client2 = Mqtt5Client.builder()
                    .identifier("quic-second-client")
                    .serverHost("localhost")
                    .serverPort(((InetSocketAddress) serverChannel.localAddress()).getPort())
                    .sslConfig(MqttClientSslConfig.builder().trustManagerFactory(trustManagerFactory).build())
                    .quicWithDefaultConfig()
                    .executorConfig(executorConfig)
                    .buildBlocking();
            try {
                final Mqtt5ConnAck connAck2 = client2.connect();
                assertFalse(connAck2.getReasonCode().isError());
                assertEquals(MqttClientState.CONNECTED, client2.getState());
                assertTrue(serverHandler.thirdConnectLatch.await(10, TimeUnit.SECONDS),
                        "a second client on the same event loop must still resolve");
            } finally {
                client2.disconnect();
            }
        } finally {
            client1.disconnect();
            clientGroup.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).sync();
        }
    }

    private @NotNull TrustManagerFactory trustManagerFactory() throws Exception {
        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, null);
        keyStore.setCertificateEntry("ca", caCertificate);
        final TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);
        return trustManagerFactory;
    }

    private static @NotNull MultithreadEventLoopGroup newClientEventLoopGroup() {
        if (ClassUtil.isAvailable("io.netty.channel.epoll.Epoll") && Epoll.isAvailable()) {
            return new MultiThreadIoEventLoopGroup(1, EpollIoHandler.newFactory());
        }
        return new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    }

    @ChannelHandler.Sharable
    private static class ServerMqttHandler extends SimpleChannelInboundHandler<ByteBuf> {

        final @NotNull AtomicInteger connectCount = new AtomicInteger();
        final @NotNull AtomicReference<ChannelHandlerContext> lastStream = new AtomicReference<>();
        final @NotNull CountDownLatch connectLatch = new CountDownLatch(1);
        final @NotNull CountDownLatch secondConnectLatch = new CountDownLatch(2);
        final @NotNull CountDownLatch thirdConnectLatch = new CountDownLatch(3);
        final @NotNull CountDownLatch publishLatch = new CountDownLatch(1);
        final @NotNull StringBuilder publishPayload = new StringBuilder();

        @Override
        protected void channelRead0(final @NotNull ChannelHandlerContext ctx, final @NotNull ByteBuf msg) {
            final int packetType = msg.getUnsignedByte(0) >> 4;
            if (packetType == 1) { // CONNECT
                connectCount.incrementAndGet();
                lastStream.set(ctx);
                connectLatch.countDown();
                secondConnectLatch.countDown();
                thirdConnectLatch.countDown();
                ctx.writeAndFlush(Unpooled.copiedBuffer(CONNACK));
            } else if (packetType == 3) { // PUBLISH
                publishPayload.append(new String(ByteBufUtil.getBytes(msg), StandardCharsets.UTF_8));
                publishLatch.countDown();
            }
        }
    }
}
