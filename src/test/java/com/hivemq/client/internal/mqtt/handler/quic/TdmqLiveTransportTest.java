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
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Live TDMQ cluster checks. Skipped unless {@code TDMQ_HOST}, {@code TDMQ_USERNAME} and {@code TDMQ_PASSWORD} are set.
 */
class TdmqLiveTransportTest {

    private static String host;
    private static String username;
    private static byte[] password;

    @BeforeAll
    static void requireCredentials() {
        host = System.getenv("TDMQ_HOST");
        username = System.getenv("TDMQ_USERNAME");
        final String passwordText = System.getenv("TDMQ_PASSWORD");
        assumeTrue(host != null && !host.isEmpty(), "TDMQ_HOST not set");
        assumeTrue(username != null && !username.isEmpty(), "TDMQ_USERNAME not set");
        assumeTrue(passwordText != null && !passwordText.isEmpty(), "TDMQ_PASSWORD not set");
        password = passwordText.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    @Timeout(45)
    void tcp() throws InterruptedException {
        exercise("tcp", builder -> builder.serverPort(MqttClient.DEFAULT_SERVER_PORT));
    }

    @Test
    @Timeout(45)
    void ssl() throws InterruptedException {
        exercise("ssl", builder -> builder.serverPort(MqttClient.DEFAULT_SERVER_PORT_SSL).sslWithDefaultConfig());
    }

    @Test
    @Timeout(45)
    void websocket() throws InterruptedException {
        exercise("ws", builder -> builder.serverPort(80).webSocketWithDefaultConfig());
    }

    @Test
    @Timeout(45)
    void websocketSsl() throws InterruptedException {
        exercise("wss",
                builder -> builder.serverPort(MqttClient.DEFAULT_SERVER_PORT_WEBSOCKET_SSL)
                        .webSocketWithDefaultConfig()
                        .sslWithDefaultConfig());
    }

    @Test
    @Timeout(45)
    void quic() throws InterruptedException {
        exercise("quic", builder -> builder.serverPort(MqttClient.DEFAULT_SERVER_PORT_QUIC)
                .quicWithDefaultConfig()
                .sslWithDefaultConfig());
    }

    private static void exercise(final String transport, final Consumer<Mqtt5ClientBuilder> transportConfig)
            throws InterruptedException {
        final String clientId = "hivemq-tdmq-" + transport + "-" + UUID.randomUUID();
        final String topic = "hivemq/quic-it/" + transport + "/" + clientId;
        final byte[] payload = ("hello-" + transport).getBytes(StandardCharsets.UTF_8);
        final Mqtt5ClientBuilder builder = Mqtt5Client.builder()
                .identifier(clientId)
                .serverHost(host)
                .simpleAuth()
                .username(username)
                .password(password)
                .applySimpleAuth();
        transportConfig.accept(builder);
        final Mqtt5BlockingClient client = builder.buildBlocking();
        try {
            final Mqtt5ConnAck connAck = client.connect();
            assertFalse(connAck.getReasonCode().isError(), () -> transport + " CONNACK " + connAck);
            System.out.println(transport + " connected: " + connAck);
            try (Mqtt5BlockingClient.Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
                client.subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).send();
                client.publishWith().topic(topic).qos(MqttQos.AT_LEAST_ONCE).payload(payload).send();
                final Optional<Mqtt5Publish> received = publishes.receive(15, TimeUnit.SECONDS);
                assertTrue(received.isPresent(), transport + " did not receive published message");
                assertArrayEquals(payload, received.get().getPayloadAsBytes());
                System.out.println(transport + " pub/sub ok topic=" + received.get().getTopic());
            }
        } finally {
            if (client.getState().isConnectedOrReconnect()) {
                client.disconnect();
            }
        }
    }
}
