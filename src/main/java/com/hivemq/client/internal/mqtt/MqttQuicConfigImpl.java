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

import com.hivemq.client.internal.util.collections.ImmutableList;
import com.hivemq.client.mqtt.MqttQuicConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * @author Silvio Giebl
 */
public class MqttQuicConfigImpl implements MqttQuicConfig {

    static final @NotNull MqttQuicConfigImpl DEFAULT =
            new MqttQuicConfigImpl(ImmutableList.of(DEFAULT_APPLICATION_PROTOCOL), DEFAULT_HANDSHAKE_TIMEOUT_MS,
                    DEFAULT_MAX_IDLE_TIMEOUT_MS);

    private final @NotNull ImmutableList<String> applicationProtocols;
    private final int handshakeTimeoutMs;
    private final int maxIdleTimeoutMs;

    MqttQuicConfigImpl(
            final @NotNull ImmutableList<String> applicationProtocols,
            final int handshakeTimeoutMs,
            final int maxIdleTimeoutMs) {

        this.applicationProtocols = applicationProtocols;
        this.handshakeTimeoutMs = handshakeTimeoutMs;
        this.maxIdleTimeoutMs = maxIdleTimeoutMs;
    }

    @Override
    public @NotNull List<String> getApplicationProtocols() {
        return applicationProtocols;
    }

    public @NotNull ImmutableList<String> getRawApplicationProtocols() {
        return applicationProtocols;
    }

    @Override
    public int getHandshakeTimeoutMs() {
        return handshakeTimeoutMs;
    }

    @Override
    public int getMaxIdleTimeoutMs() {
        return maxIdleTimeoutMs;
    }

    @Override
    public MqttQuicConfigImplBuilder.@NotNull Default extend() {
        return new MqttQuicConfigImplBuilder.Default(this);
    }

    @Override
    public boolean equals(final @Nullable Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MqttQuicConfigImpl)) {
            return false;
        }
        final MqttQuicConfigImpl that = (MqttQuicConfigImpl) o;
        return applicationProtocols.equals(that.applicationProtocols) &&
                (handshakeTimeoutMs == that.handshakeTimeoutMs) && (maxIdleTimeoutMs == that.maxIdleTimeoutMs);
    }

    @Override
    public int hashCode() {
        int result = applicationProtocols.hashCode();
        result = 31 * result + Integer.hashCode(handshakeTimeoutMs);
        result = 31 * result + Integer.hashCode(maxIdleTimeoutMs);
        return result;
    }
}
