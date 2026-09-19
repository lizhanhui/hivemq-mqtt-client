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

package com.hivemq.client.internal.mqtt;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttClientTransportConfig;
import com.hivemq.client.mqtt.MqttProxyConfig;
import com.hivemq.client.mqtt.MqttQuicConfig;
import com.hivemq.client.mqtt.MqttWebSocketConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Silvio Giebl
 */
class MqttClientTransportConfigImplBuilderTest {

    @Test
    void defaultPort_quic() {
        final MqttClientTransportConfig config = MqttClientTransportConfig.builder().quicWithDefaultConfig().build();
        assertEquals(MqttClient.DEFAULT_SERVER_PORT_QUIC, config.getServerAddress().getPort());
        assertTrue(config.getQuicConfig().isPresent());
        assertEquals(MqttQuicConfig.DEFAULT_APPLICATION_PROTOCOL,
                config.getQuicConfig().get().getApplicationProtocols().get(0));
    }

    @Test
    void defaultPort_quicIgnoresSsl() {
        final MqttClientTransportConfig config =
                MqttClientTransportConfig.builder().quicWithDefaultConfig().sslWithDefaultConfig().build();
        assertEquals(MqttClient.DEFAULT_SERVER_PORT_QUIC, config.getServerAddress().getPort());
    }

    @Test
    void rejectQuicAndWebSocket() {
        assertThrows(IllegalStateException.class,
                () -> MqttClientTransportConfig.builder()
                        .quicWithDefaultConfig()
                        .webSocketConfig(MqttWebSocketConfig.builder().build())
                        .build());
    }

    @Test
    void rejectQuicAndProxy() {
        assertThrows(IllegalStateException.class,
                () -> MqttClientTransportConfig.builder()
                        .quicWithDefaultConfig()
                        .proxyConfig(MqttProxyConfig.builder().build())
                        .build());
    }

    @Test
    void quicCanBeRemoved() {
        final MqttClientTransportConfig config =
                MqttClientTransportConfig.builder().quicWithDefaultConfig().quicConfig(null).build();
        assertFalse(config.getQuicConfig().isPresent());
        assertEquals(MqttClient.DEFAULT_SERVER_PORT, config.getServerAddress().getPort());
    }
}
