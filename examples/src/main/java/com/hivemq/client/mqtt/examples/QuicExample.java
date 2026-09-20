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
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;

import java.nio.charset.StandardCharsets;

/**
 * Shared setup for the QUIC publish/subscribe examples.
 */
final class QuicExample {

    static Mqtt5BlockingClient connect(final String identifier, final String[] args) {
        final String server = (args.length > 0) ? args[0] : "localhost";
        final Mqtt5ClientBuilder builder = Mqtt5Client.builder().identifier(identifier).quicWithDefaultConfig();
        applyServer(builder, server);

        final Mqtt5BlockingClient client = builder.buildBlocking();
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
        System.out.println("connected over QUIC to " + server);
        return client;
    }

    private static void applyServer(final Mqtt5ClientBuilder builder, final String server) {
        final int colon = server.lastIndexOf(':');
        if ((colon > 0) && (server.indexOf(']') < colon)) {
            builder.serverHost(server.substring(0, colon)).serverPort(Integer.parseInt(server.substring(colon + 1)));
        } else {
            builder.serverHost(server);
        }
    }

    private QuicExample() {}
}
