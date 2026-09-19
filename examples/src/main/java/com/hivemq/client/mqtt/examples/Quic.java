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

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;

import java.nio.charset.StandardCharsets;

/**
 * Subscribe and publish over QUIC. Requires the {@code hivemq-mqtt-client-quic} module (Netty QUIC native codec) at
 * runtime.
 * <p>
 * Point {@code serverHost} at a broker that accepts MQTT on a single bidirectional QUIC stream (for example EMQX or
 * TDMQ on UDP 14567). QUIC always uses TLS 1.3; {@code sslWithDefaultConfig()} uses the JVM trust store.
 */
public class Quic {

    public static void main(final String[] args) throws InterruptedException {
        final String host = "localhost";
        final String topic = "demo/quic";

        final Mqtt5BlockingClient subscriber = quicClient(host, "demo-quic-sub").buildBlocking();
        final Mqtt5BlockingClient publisher = quicClient(host, "demo-quic-pub").buildBlocking();

        subscriber.connect();
        publisher.connect();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes = subscriber.publishes(MqttGlobalPublishFilter.ALL)) {
            subscriber.subscribeWith().topicFilter(topic).qos(MqttQos.AT_LEAST_ONCE).send();
            System.out.println("subscribed " + topic);

            publisher.publishWith()
                    .topic(topic)
                    .qos(MqttQos.AT_LEAST_ONCE)
                    .payload("hello-quic".getBytes(StandardCharsets.UTF_8))
                    .send();
            System.out.println("published");

            final Mqtt5Publish received = publishes.receive();
            System.out.println("received " + received.getTopic() + " " +
                    new String(received.getPayloadAsBytes(), StandardCharsets.UTF_8));
        } finally {
            publisher.disconnect();
            subscriber.disconnect();
        }
    }

    private static Mqtt5ClientBuilder quicClient(final String host, final String clientId) {
        return Mqtt5Client.builder()
                .identifier(clientId)
                .serverHost(host)
                .quicWithDefaultConfig()
                .sslWithDefaultConfig();
    }
}
