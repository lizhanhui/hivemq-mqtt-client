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

package com.hivemq.client.mqtt.examples;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient.Mqtt5Publishes;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;

import java.nio.charset.StandardCharsets;

/**
 * Publishes over MQTT over QUIC, or MQTT over TLS if QUIC is not available.
 * <p>
 * Usage: {@code QuicFallback [host[:port]] [username] [password]}
 * <p>
 * Tries QUIC first (default port {@value MqttClient#DEFAULT_SERVER_PORT_QUIC}). If the native QUIC library is missing
 * or the QUIC connect fails, the same host is used with {@link Mqtt5ClientBuilder#sslWithDefaultConfig()} (default
 * port {@value MqttClient#DEFAULT_SERVER_PORT_SSL}). A port in the first argument applies only to the QUIC attempt.
 *
 * @author HiveMQ
 */
public class QuicFallback {

    private static final String TOPIC = "demo/quic";

    public static void main(final String[] args) throws InterruptedException {
        final String server = (args.length > 0) ? args[0] : "localhost";
        final Mqtt5BlockingClient client = connect(server, args);
        try (final Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            client.subscribeWith().topicFilter(TOPIC).qos(MqttQos.AT_LEAST_ONCE).send();
            for (int i = 0; i < 5; i++) {
                final String payload = "hello fallback " + i;
                client.publishWith()
                        .topic(TOPIC)
                        .qos(MqttQos.AT_LEAST_ONCE)
                        .payload(payload.getBytes(StandardCharsets.UTF_8))
                        .send();
                System.out.println("published: " + payload);
                System.out.println("received:  " + publishes.receive());
            }
        } finally {
            client.disconnect();
        }
    }

    private static Mqtt5BlockingClient connect(final String server, final String[] args) {
        try {
            final Mqtt5BlockingClient client = open(server, args, true);
            System.out.println("connected over QUIC to " + server);
            return client;
        } catch (final RuntimeException e) {
            System.out.println("QUIC unavailable (" + rootMessage(e) + "), falling back to MQTT over TLS");
        }
        final Mqtt5BlockingClient client = open(hostOnly(server), args, false);
        System.out.println("connected over MQTT/TLS to " + hostOnly(server) + ':' +
                MqttClient.DEFAULT_SERVER_PORT_SSL);
        return client;
    }

    private static Mqtt5BlockingClient open(final String server, final String[] args, final boolean quic) {
        final Mqtt5ClientBuilder builder = Mqtt5Client.builder().identifier("quic-fallback-example");
        if (quic) {
            builder.quicWithDefaultConfig();
        } else {
            builder.sslWithDefaultConfig();
        }
        applyServer(builder, server);
        final Mqtt5BlockingClient client = builder.buildBlocking();
        try {
            if (args.length >= 3) {
                client.connectWith()
                        .simpleAuth()
                        .username(args[1])
                        .password(args[2].getBytes(StandardCharsets.UTF_8))
                        .applySimpleAuth()
                        .send();
            } else {
                client.connect();
            }
            return client;
        } catch (final RuntimeException e) {
            try {
                client.disconnect();
            } catch (final RuntimeException ignored) {
            }
            throw e;
        }
    }

    private static void applyServer(final Mqtt5ClientBuilder builder, final String server) {
        final int colon = server.lastIndexOf(':');
        if ((colon > 0) && (server.indexOf(']') < colon)) {
            builder.serverHost(server.substring(0, colon)).serverPort(Integer.parseInt(server.substring(colon + 1)));
        } else {
            builder.serverHost(server);
        }
    }

    private static String hostOnly(final String server) {
        final int colon = server.lastIndexOf(':');
        if ((colon > 0) && (server.indexOf(']') < colon)) {
            return server.substring(0, colon);
        }
        return server;
    }

    private static String rootMessage(final Throwable throwable) {
        Throwable current = throwable;
        while (current.getCause() != null) {
            current = current.getCause();
        }
        final String message = current.getMessage();
        return (message == null) || message.isEmpty() ? current.getClass().getSimpleName() : message;
    }
}
