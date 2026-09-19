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

import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;

/**
 * MQTT over QUIC. Requires the {@code hivemq-mqtt-client-quic} module (Netty QUIC native codec) at runtime.
 * <p>
 * Point {@code serverHost} / {@code serverPort} at a broker that accepts MQTT on a single bidirectional QUIC stream
 * (for example EMQX on UDP 14567).
 */
public class Quic {

    public static void main(final String[] args) {
        final Mqtt5BlockingClient client = Mqtt5Client.builder()
                .serverHost("localhost")
                .quicWithDefaultConfig()
                .sslWithDefaultConfig()
                .buildBlocking();

        client.connect();
        try {
            client.publishWith().topic("demo/quic").payload("hello-quic".getBytes()).send();
        } finally {
            client.disconnect();
        }
    }
}
