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
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient.Mqtt5Publishes;

/**
 * Subscribes to {@code demo/quic} over MQTT over QUIC.
 * <p>
 * Usage: {@code QuicSubscribe [host[:port]] [username] [password]}
 * <p>
 * Requires a broker that speaks MQTT over QUIC (TLS 1.3, ALPN {@code mqtt}). If no host is given,
 * {@code localhost:14567} is used. Add the {@code hivemq-mqtt-client-quic} module to the classpath.
 *
 * @author HiveMQ
 */
public class QuicSubscribe {

    public static void main(final String[] args) throws InterruptedException {
        final Mqtt5BlockingClient client = QuicExample.connect("quic-subscribe-example", args);
        try (final Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL)) {
            client.subscribeWith().topicFilter("demo/quic").qos(MqttQos.AT_LEAST_ONCE).send();
            System.out.println("subscribed over QUIC, waiting for messages on demo/quic ...");
            while (!Thread.currentThread().isInterrupted()) {
                System.out.println(publishes.receive());
            }
        } finally {
            client.disconnect();
        }
    }
}
