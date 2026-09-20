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

import com.hivemq.client.mqtt.MqttQuicConfig;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * @author HiveMQ
 */
public class MqttQuicConfigImpl implements MqttQuicConfig {

    public static final @NotNull MqttQuicConfigImpl DEFAULT = new MqttQuicConfigImpl(DEFAULT_INITIAL_MAX_DATA,
            DEFAULT_INITIAL_MAX_STREAM_DATA_BIDIRECTIONAL_LOCAL, DEFAULT_INITIAL_MAX_STREAM_DATA_BIDIRECTIONAL_REMOTE,
            DEFAULT_INITIAL_MAX_STREAMS_BIDIRECTIONAL, DEFAULT_MAX_IDLE_TIMEOUT_MS);

    private final long initialMaxData;
    private final long initialMaxStreamDataBidirectionalLocal;
    private final long initialMaxStreamDataBidirectionalRemote;
    private final long initialMaxStreamsBidirectional;
    private final long maxIdleTimeoutMs;

    MqttQuicConfigImpl(
            final long initialMaxData,
            final long initialMaxStreamDataBidirectionalLocal,
            final long initialMaxStreamDataBidirectionalRemote,
            final long initialMaxStreamsBidirectional,
            final long maxIdleTimeoutMs) {

        this.initialMaxData = initialMaxData;
        this.initialMaxStreamDataBidirectionalLocal = initialMaxStreamDataBidirectionalLocal;
        this.initialMaxStreamDataBidirectionalRemote = initialMaxStreamDataBidirectionalRemote;
        this.initialMaxStreamsBidirectional = initialMaxStreamsBidirectional;
        this.maxIdleTimeoutMs = maxIdleTimeoutMs;
    }

    @Override
    public long getInitialMaxData() {
        return initialMaxData;
    }

    @Override
    public long getInitialMaxStreamDataBidirectionalLocal() {
        return initialMaxStreamDataBidirectionalLocal;
    }

    @Override
    public long getInitialMaxStreamDataBidirectionalRemote() {
        return initialMaxStreamDataBidirectionalRemote;
    }

    @Override
    public long getInitialMaxStreamsBidirectional() {
        return initialMaxStreamsBidirectional;
    }

    @Override
    public long getMaxIdleTimeoutMs() {
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

        return (initialMaxData == that.initialMaxData) &&
                (initialMaxStreamDataBidirectionalLocal == that.initialMaxStreamDataBidirectionalLocal) &&
                (initialMaxStreamDataBidirectionalRemote == that.initialMaxStreamDataBidirectionalRemote) &&
                (initialMaxStreamsBidirectional == that.initialMaxStreamsBidirectional) &&
                (maxIdleTimeoutMs == that.maxIdleTimeoutMs);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(initialMaxData);
        result = 31 * result + Long.hashCode(initialMaxStreamDataBidirectionalLocal);
        result = 31 * result + Long.hashCode(initialMaxStreamDataBidirectionalRemote);
        result = 31 * result + Long.hashCode(initialMaxStreamsBidirectional);
        result = 31 * result + Long.hashCode(maxIdleTimeoutMs);
        return result;
    }
}
