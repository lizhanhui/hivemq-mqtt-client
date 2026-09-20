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

import com.hivemq.client.internal.util.Checks;
import com.hivemq.client.mqtt.MqttQuicConfigBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author HiveMQ
 */
public abstract class MqttQuicConfigImplBuilder<B extends MqttQuicConfigImplBuilder<B>> {

    private long initialMaxData = MqttQuicConfigImpl.DEFAULT_INITIAL_MAX_DATA;
    private long initialMaxStreamDataBidirectionalLocal =
            MqttQuicConfigImpl.DEFAULT_INITIAL_MAX_STREAM_DATA_BIDIRECTIONAL_LOCAL;
    private long initialMaxStreamDataBidirectionalRemote =
            MqttQuicConfigImpl.DEFAULT_INITIAL_MAX_STREAM_DATA_BIDIRECTIONAL_REMOTE;
    private long initialMaxStreamsBidirectional = MqttQuicConfigImpl.DEFAULT_INITIAL_MAX_STREAMS_BIDIRECTIONAL;
    private long maxIdleTimeoutMs = MqttQuicConfigImpl.DEFAULT_MAX_IDLE_TIMEOUT_MS;

    MqttQuicConfigImplBuilder() {}

    MqttQuicConfigImplBuilder(final @Nullable MqttQuicConfigImpl quicConfig) {
        if (quicConfig != null) {
            initialMaxData = quicConfig.getInitialMaxData();
            initialMaxStreamDataBidirectionalLocal = quicConfig.getInitialMaxStreamDataBidirectionalLocal();
            initialMaxStreamDataBidirectionalRemote = quicConfig.getInitialMaxStreamDataBidirectionalRemote();
            initialMaxStreamsBidirectional = quicConfig.getInitialMaxStreamsBidirectional();
            maxIdleTimeoutMs = quicConfig.getMaxIdleTimeoutMs();
        }
    }

    abstract @NotNull B self();

    public @NotNull B initialMaxData(final long initialMaxData) {
        this.initialMaxData = Checks.unsignedInt(initialMaxData, "Initial max data");
        return self();
    }

    public @NotNull B initialMaxStreamDataBidirectionalLocal(final long initialMaxStreamDataBidirectionalLocal) {
        this.initialMaxStreamDataBidirectionalLocal =
                Checks.unsignedInt(initialMaxStreamDataBidirectionalLocal, "Initial max stream data bidirectional local");
        return self();
    }

    public @NotNull B initialMaxStreamDataBidirectionalRemote(final long initialMaxStreamDataBidirectionalRemote) {
        this.initialMaxStreamDataBidirectionalRemote = Checks.unsignedInt(initialMaxStreamDataBidirectionalRemote,
                "Initial max stream data bidirectional remote");
        return self();
    }

    public @NotNull B initialMaxStreamsBidirectional(final long initialMaxStreamsBidirectional) {
        this.initialMaxStreamsBidirectional =
                Checks.unsignedInt(initialMaxStreamsBidirectional, "Initial max streams bidirectional");
        return self();
    }

    public @NotNull B maxIdleTimeout(final long timeout, final @Nullable TimeUnit timeUnit) {
        Checks.notNull(timeUnit, "Time unit");
        maxIdleTimeoutMs = Checks.range(timeUnit.toMillis(timeout), 0, Long.MAX_VALUE,
                "Max idle timeout in milliseconds");
        return self();
    }

    public @NotNull MqttQuicConfigImpl build() {
        return new MqttQuicConfigImpl(initialMaxData, initialMaxStreamDataBidirectionalLocal,
                initialMaxStreamDataBidirectionalRemote, initialMaxStreamsBidirectional, maxIdleTimeoutMs);
    }

    public static class Default extends MqttQuicConfigImplBuilder<Default> implements MqttQuicConfigBuilder {

        public Default() {}

        Default(final @Nullable MqttQuicConfigImpl quicConfig) {
            super(quicConfig);
        }

        @Override
        @NotNull Default self() {
            return this;
        }
    }

    public static class Nested<P> extends MqttQuicConfigImplBuilder<Nested<P>> implements MqttQuicConfigBuilder.Nested<P> {

        private final @NotNull Function<? super MqttQuicConfigImpl, P> parentConsumer;

        Nested(
                final @Nullable MqttQuicConfigImpl quicConfig,
                final @NotNull Function<? super MqttQuicConfigImpl, P> parentConsumer) {

            super(quicConfig);
            this.parentConsumer = parentConsumer;
        }

        @Override
        @NotNull Nested<P> self() {
            return this;
        }

        @Override
        public @NotNull P applyQuicConfig() {
            return parentConsumer.apply(build());
        }
    }
}
