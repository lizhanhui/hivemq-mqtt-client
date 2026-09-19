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

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.quic.InsecureQuicTokenHandler;
import io.netty.handler.codec.quic.Quic;
import io.netty.handler.codec.quic.QuicServerCodecBuilder;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.net.ssl.TrustManagerFactory;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * @author Silvio Giebl
 */
class MqttQuicTransportTest {

    private NioEventLoopGroup group;
    private Channel serverChannel;
    private SelfSignedCertificate certificate;

    @BeforeEach
    void setUp() throws Exception {
        assumeTrue(Quic.isAvailable(),
                "Netty QUIC native library is not available: " + Quic.unavailabilityCause());
        certificate = new SelfSignedCertificate("localhost");
        group = new NioEventLoopGroup(1);
        serverChannel = new Bootstrap().group(group)
                .channel(NioDatagramChannel.class)
                .handler(new QuicServerCodecBuilder().sslContext(
                                QuicSslContextBuilder.forServer(certificate.key(), null, certificate.cert())
                                        .applicationProtocols("mqtt")
                                        .build())
                        .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                        .initialMaxData(10_000_000)
                        .initialMaxStreamDataBidirectionalLocal(1_000_000)
                        .initialMaxStreamDataBidirectionalRemote(1_000_000)
                        .initialMaxStreamsBidirectional(1)
                        .handler(new ChannelInboundHandlerAdapter())
                        .streamHandler(new ChannelInitializer<QuicStreamChannel>() {
                            @Override
                            protected void initChannel(final QuicStreamChannel ch) {
                                ch.pipeline().addLast(new MqttQuicTestServerHandler());
                            }
                        })
                        .build())
                .bind(new InetSocketAddress("127.0.0.1", 0))
                .sync()
                .channel();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (serverChannel != null) {
            serverChannel.close().sync();
        }
        if (group != null) {
            group.shutdownGracefully(0, 2, TimeUnit.SECONDS).sync();
        }
        if (certificate != null) {
            certificate.delete();
        }
    }

    @Test
    @Timeout(30)
    void connectPublishSubscribeDisconnect() throws Exception {
        final int port = ((InetSocketAddress) serverChannel.localAddress()).getPort();
        final Mqtt3BlockingClient client = MqttClient.builder()
                .useMqttVersion3()
                .serverHost("127.0.0.1")
                .serverPort(port)
                .quicWithDefaultConfig()
                .sslConfig()
                .trustManagerFactory(trustManagerFactory(certificate.cert()))
                .hostnameVerifier((hostname, session) -> true)
                .applySslConfig()
                .buildBlocking();

        client.connect();
        try (final Mqtt3BlockingClient.Mqtt3Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            client.subscribeWith().topicFilter("test/topic").qos(MqttQos.AT_LEAST_ONCE).send();
            client.publishWith()
                    .topic("test/topic")
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .payload("payload".getBytes())
                    .send();
            assertArrayEquals("payload".getBytes(),
                    publishes.receive(2, TimeUnit.SECONDS).orElseThrow().getPayloadAsBytes());
        } finally {
            client.disconnect();
        }
    }

    private static TrustManagerFactory trustManagerFactory(final Certificate certificate) throws Exception {
        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null);
        keyStore.setCertificateEntry("ca", certificate);
        final TrustManagerFactory trustManagerFactory =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(keyStore);
        return trustManagerFactory;
    }

    private static final class MqttQuicTestServerHandler extends ChannelInboundHandlerAdapter {

        private ByteBuf accumulator;

        @Override
        public void handlerAdded(final ChannelHandlerContext ctx) {
            accumulator = ctx.alloc().buffer();
        }

        @Override
        public void handlerRemoved(final ChannelHandlerContext ctx) {
            if (accumulator != null) {
                accumulator.release();
                accumulator = null;
            }
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            final ByteBuf in = (ByteBuf) msg;
            accumulator.writeBytes(in);
            in.release();
            while (readPacket(ctx)) {
            }
        }

        private boolean readPacket(final ChannelHandlerContext ctx) {
            if (accumulator.readableBytes() < 2) {
                return false;
            }
            final int readerIndex = accumulator.readerIndex();
            final int firstByte = accumulator.readUnsignedByte();
            int remaining = 0;
            int multiplier = 1;
            int encodedBytes = 0;
            byte digit;
            do {
                if (!accumulator.isReadable()) {
                    accumulator.readerIndex(readerIndex);
                    return false;
                }
                digit = accumulator.readByte();
                remaining += (digit & 127) * multiplier;
                multiplier *= 128;
                encodedBytes++;
            } while (((digit & 128) != 0) && (encodedBytes < 4));
            if (accumulator.readableBytes() < remaining) {
                accumulator.readerIndex(readerIndex);
                return false;
            }
            final int type = (firstByte >> 4) & 0x0F;
            final int qos = (firstByte >> 1) & 0x03;
            respond(ctx, type, qos, readerIndex + 1 + encodedBytes, remaining);
            accumulator.skipBytes(remaining);
            accumulator.discardReadBytes();
            return true;
        }

        private void respond(
                final ChannelHandlerContext ctx,
                final int type,
                final int qos,
                final int variableHeaderIndex,
                final int remaining) {

            switch (type) {
                case 1: // CONNECT
                    ctx.writeAndFlush(Unpooled.wrappedBuffer(new byte[]{0x20, 0x02, 0x00, 0x00}));
                    break;
                case 3: // PUBLISH
                    if (qos > 0) {
                        final int topicLength = accumulator.getUnsignedShort(variableHeaderIndex);
                        final int packetId = accumulator.getUnsignedShort(variableHeaderIndex + 2 + topicLength);
                        echoPublish(ctx, variableHeaderIndex, remaining);
                        ctx.writeAndFlush(Unpooled.buffer(4).writeByte(0x40).writeByte(0x02).writeShort(packetId));
                    }
                    break;
                case 8: // SUBSCRIBE
                    final int subscribeId = accumulator.getUnsignedShort(variableHeaderIndex);
                    ctx.writeAndFlush(Unpooled.buffer(5).writeByte(0x90).writeByte(0x03).writeShort(subscribeId).writeByte(0x01));
                    break;
                case 12: // PINGREQ
                    ctx.writeAndFlush(Unpooled.wrappedBuffer(new byte[]{(byte) 0xD0, 0x00}));
                    break;
                case 14: // DISCONNECT
                    ctx.close();
                    break;
                default:
                    break;
            }
        }

        private void echoPublish(final ChannelHandlerContext ctx, final int variableHeaderIndex, final int remaining) {
            final int topicLength = accumulator.getUnsignedShort(variableHeaderIndex);
            final int topicAndLen = 2 + topicLength;
            final int payloadIndex = variableHeaderIndex + topicAndLen + 2;
            final int payloadLength = remaining - topicAndLen - 2;
            final int newRemaining = topicAndLen + payloadLength;
            final ByteBuf publish = Unpooled.buffer(2 + newRemaining);
            publish.writeByte(0x30);
            writeRemainingLength(publish, newRemaining);
            publish.writeBytes(accumulator, variableHeaderIndex, topicAndLen);
            publish.writeBytes(accumulator, payloadIndex, payloadLength);
            ctx.writeAndFlush(publish);
        }

        private static void writeRemainingLength(final ByteBuf out, int remaining) {
            do {
                int digit = remaining % 128;
                remaining /= 128;
                if (remaining > 0) {
                    digit |= 128;
                }
                out.writeByte(digit);
            } while (remaining > 0);
        }
    }
}
