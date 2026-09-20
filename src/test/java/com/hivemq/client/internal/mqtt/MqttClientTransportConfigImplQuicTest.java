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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author HiveMQ
 */
class MqttClientTransportConfigImplQuicTest {

    @Test
    void quic_default_config() {
        final MqttClientTransportConfigImpl transportConfig =
                new MqttClientTransportConfigImplBuilder.Default().quicWithDefaultConfig().build();

        assertTrue(transportConfig.getQuicConfig().isPresent());
        assertEquals(MqttQuicConfigImpl.DEFAULT, transportConfig.getRawQuicConfig());
    }

    @Test
    void quic_defaults_to_default_ssl_config() {
        final MqttClientTransportConfigImpl transportConfig =
                new MqttClientTransportConfigImplBuilder.Default().quicWithDefaultConfig().build();

        // a QUIC transport always uses TLS 1.3
        assertTrue(transportConfig.getSslConfig().isPresent());
        assertEquals(MqttClientSslConfigImpl.DEFAULT, transportConfig.getRawSslConfig());
    }

    @Test
    void quic_keeps_explicit_ssl_config() {
        final MqttClientSslConfigImpl sslConfig =
                new MqttClientSslConfigImplBuilder.Default().hostnameVerifier((hostname, session) -> true).build();

        final MqttClientTransportConfigImpl transportConfig = new MqttClientTransportConfigImplBuilder.Default()
                .sslConfig(sslConfig)
                .quicWithDefaultConfig()
                .build();

        assertEquals(sslConfig, transportConfig.getRawSslConfig());
    }

    @Test
    void quic_not_combinable_with_websocket() {
        final MqttClientTransportConfigImplBuilder.Default builder =
                new MqttClientTransportConfigImplBuilder.Default().quicWithDefaultConfig().webSocketWithDefaultConfig();

        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    void quic_not_combinable_with_proxy() {
        final MqttClientTransportConfigImplBuilder.Default builder = new MqttClientTransportConfigImplBuilder.Default()
                .quicWithDefaultConfig()
                .proxyConfig(new MqttProxyConfigImplBuilder.Default().build());

        assertThrows(IllegalStateException.class, builder::build);
    }

    @Test
    void quic_config_removed_with_null() {
        final MqttClientTransportConfigImpl transportConfig = new MqttClientTransportConfigImplBuilder.Default()
                .quicWithDefaultConfig()
                .quicConfig(null)
                .build();

        assertFalse(transportConfig.getQuicConfig().isPresent());
    }

    @Test
    void extend_keeps_quic_config() {
        final MqttClientTransportConfigImpl transportConfig =
                new MqttClientTransportConfigImplBuilder.Default().quicWithDefaultConfig().build();

        assertEquals(transportConfig, transportConfig.extend().build());
    }
}
