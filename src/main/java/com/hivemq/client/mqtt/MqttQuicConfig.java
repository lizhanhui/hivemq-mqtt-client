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

package com.hivemq.client.mqtt;

import com.hivemq.client.annotations.DoNotImplement;
import com.hivemq.client.annotations.Immutable;
import com.hivemq.client.internal.mqtt.MqttQuicConfigImplBuilder;
import org.jetbrains.annotations.NotNull;

import java.util.List;

/**
 * Configuration for a QUIC transport to use by {@link MqttClient MQTT clients}.
 * <p>
 * MQTT packets are carried unchanged on a single client-initiated bidirectional QUIC stream (a TCP replacement). QUIC
 * always uses TLS 1.3; configure certificates via {@link MqttClientSslConfig}.
 *
 * @since 1.5
 */
@DoNotImplement
public interface MqttQuicConfig {

    /**
     * The default QUIC application protocol (ALPN).
     */
    @NotNull String DEFAULT_APPLICATION_PROTOCOL = "mqtt";
    /**
     * The default QUIC handshake timeout in milliseconds.
     */
    int DEFAULT_HANDSHAKE_TIMEOUT_MS = 10_000;
    /**
     * The default QUIC idle timeout in milliseconds.
     * <p>
     * {@code 0} means the client derives an idle timeout from the MQTT keep-alive so QUIC does not close the connection
     * before MQTT ping.
     */
    int DEFAULT_MAX_IDLE_TIMEOUT_MS = 0;

    /**
     * Creates a builder for a QUIC configuration.
     *
     * @return the created builder for a QUIC configuration.
     */
    static @NotNull MqttQuicConfigBuilder builder() {
        return new MqttQuicConfigImplBuilder.Default();
    }

    /**
     * @return the QUIC application protocols (ALPN) offered to the server.
     */
    @NotNull @Immutable List<@NotNull String> getApplicationProtocols();

    /**
     * @return the QUIC handshake timeout in milliseconds.
     */
    int getHandshakeTimeoutMs();

    /**
     * @return the QUIC idle timeout in milliseconds, or {@code 0} to derive it from the MQTT keep-alive.
     */
    int getMaxIdleTimeoutMs();

    /**
     * Creates a builder for extending this QUIC configuration.
     *
     * @return the created builder.
     */
    @NotNull MqttQuicConfigBuilder extend();
}
