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

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

import java.nio.charset.StandardCharsets;

/**
 * Publishes to {@code demo/quic} over MQTT over QUIC.
 * <p>
 * Run {@link QuicSubscribe} in another process, then start this example. Requires a broker that speaks MQTT over
 * QUIC (TLS 1.3, ALPN {@code mqtt}). If no host is given, {@code localhost:14567} is used. Add the
 * {@code hivemq-mqtt-client-quic} module to the classpath.
 *
 * @author HiveMQ
 */
public class QuicPublish {

    public static void main(final String[] args) {
        final String host = (args.length > 0) ? args[0] : "localhost";

        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .identifier("quic-publish-example")
                .serverHost(host)
                .quicWithDefaultConfig()
                .buildBlocking();

        client.connect();
        try {
            for (int i = 0; i < 5; i++) {
                final String payload = "hello quic " + i;
                client.publishWith()
                        .topic("demo/quic")
                        .qos(MqttQos.AT_LEAST_ONCE)
                        .payload(payload.getBytes(StandardCharsets.UTF_8))
                        .send();
                System.out.println("published over QUIC: " + payload);
            }
        } finally {
            client.disconnect();
        }
    }
}
