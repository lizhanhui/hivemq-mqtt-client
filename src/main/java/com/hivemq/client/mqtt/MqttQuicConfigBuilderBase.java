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

import com.hivemq.client.annotations.CheckReturnValue;
import com.hivemq.client.annotations.DoNotImplement;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Builder base for a {@link MqttQuicConfig}.
 *
 * @param <B> the type of the builder.
 * @since 1.5
 */
@DoNotImplement
public interface MqttQuicConfigBuilderBase<B extends MqttQuicConfigBuilderBase<B>> {

    /**
     * Sets the {@link MqttQuicConfig#getApplicationProtocols() application protocols} to a single protocol.
     *
     * @param applicationProtocol the application protocol (ALPN).
     * @return the builder.
     */
    @CheckReturnValue
    @NotNull B applicationProtocol(@NotNull String applicationProtocol);

    /**
     * Sets the {@link MqttQuicConfig#getApplicationProtocols() application protocols}.
     *
     * @param applicationProtocols the application protocols (ALPN).
     * @return the builder.
     */
    @CheckReturnValue
    @NotNull B applicationProtocols(@NotNull Collection<String> applicationProtocols);

    /**
     * Sets the {@link MqttQuicConfig#getHandshakeTimeoutMs() QUIC handshake timeout}.
     * <p>
     * The timeout in milliseconds must be in the range: [0, {@link Integer#MAX_VALUE}].
     *
     * @param timeout  the handshake timeout or <code>0</code> to disable the timeout.
     * @param timeUnit the time unit of the given timeout (this timeout only supports millisecond precision).
     * @return the builder.
     */
    @CheckReturnValue
    @NotNull B handshakeTimeout(long timeout, @NotNull TimeUnit timeUnit);

    /**
     * Sets the {@link MqttQuicConfig#getMaxIdleTimeoutMs() QUIC idle timeout}.
     * <p>
     * The timeout in milliseconds must be in the range: [0, {@link Integer#MAX_VALUE}]. {@code 0} derives the idle
     * timeout from the MQTT keep-alive.
     *
     * @param timeout  the idle timeout or <code>0</code> to derive it from the MQTT keep-alive.
     * @param timeUnit the time unit of the given timeout (this timeout only supports millisecond precision).
     * @return the builder.
     */
    @CheckReturnValue
    @NotNull B maxIdleTimeout(long timeout, @NotNull TimeUnit timeUnit);
}
