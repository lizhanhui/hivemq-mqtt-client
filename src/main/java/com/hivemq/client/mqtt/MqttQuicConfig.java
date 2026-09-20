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
import com.hivemq.client.internal.mqtt.MqttQuicConfigImplBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Configuration for a QUIC transport to use by {@link MqttClient MQTT clients}.
 * <p>
 * A QUIC transport always uses TLS 1.3, so a {@link MqttClientSslConfig} applies as well. If none is configured, the
 * default secure transport configuration is used.
 * <p>
 * A QUIC transport can not be combined with a WebSocket transport or a proxy.
 *
 * @author HiveMQ
 * @since 1.5
 */
@DoNotImplement
public interface MqttQuicConfig {

    /**
     * The default connection-level flow control limit in bytes (10 MiB).
     */
    long DEFAULT_INITIAL_MAX_DATA = 10L * 1024 * 1024;
    /**
     * The default stream-level flow control limit for locally initiated bidirectional streams in bytes (1 MiB).
     */
    long DEFAULT_INITIAL_MAX_STREAM_DATA_BIDIRECTIONAL_LOCAL = 1024L * 1024;
    /**
     * The default stream-level flow control limit for remotely initiated bidirectional streams in bytes (1 MiB).
     */
    long DEFAULT_INITIAL_MAX_STREAM_DATA_BIDIRECTIONAL_REMOTE = 1024L * 1024;
    /**
     * The default maximum number of bidirectional streams the remote peer is allowed to open.
     */
    long DEFAULT_INITIAL_MAX_STREAMS_BIDIRECTIONAL = 16;
    /**
     * The default maximum idle timeout in milliseconds (<code>0</code> = disabled).
     * <p>
     * If disabled, liveness is governed by the MQTT keep-alive mechanism.
     */
    long DEFAULT_MAX_IDLE_TIMEOUT_MS = 0;

    /**
     * Creates a builder for a QUIC configuration.
     *
     * @return the created builder for a QUIC configuration.
     */
    static @NotNull MqttQuicConfigBuilder builder() {
        return new MqttQuicConfigImplBuilder.Default();
    }

    /**
     * @return the connection-level flow control limit in bytes.
     */
    long getInitialMaxData();

    /**
     * @return the stream-level flow control limit for locally initiated bidirectional streams in bytes.
     */
    long getInitialMaxStreamDataBidirectionalLocal();

    /**
     * @return the stream-level flow control limit for remotely initiated bidirectional streams in bytes.
     */
    long getInitialMaxStreamDataBidirectionalRemote();

    /**
     * @return the maximum number of bidirectional streams the remote peer is allowed to open.
     */
    long getInitialMaxStreamsBidirectional();

    /**
     * @return the maximum idle timeout in milliseconds (<code>0</code> = disabled).
     */
    long getMaxIdleTimeoutMs();

    /**
     * Creates a builder for extending this QUIC configuration.
     *
     * @return the created builder.
     */
    @NotNull MqttQuicConfigBuilder extend();
}
