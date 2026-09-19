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
import com.hivemq.client.internal.util.collections.ImmutableList;
import com.hivemq.client.mqtt.MqttQuicConfigBuilder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Silvio Giebl
 */
public abstract class MqttQuicConfigImplBuilder<B extends MqttQuicConfigImplBuilder<B>> {

    private @NotNull ImmutableList<String> applicationProtocols =
            ImmutableList.of(MqttQuicConfigImpl.DEFAULT_APPLICATION_PROTOCOL);
    private int handshakeTimeoutMs = MqttQuicConfigImpl.DEFAULT_HANDSHAKE_TIMEOUT_MS;
    private int maxIdleTimeoutMs = MqttQuicConfigImpl.DEFAULT_MAX_IDLE_TIMEOUT_MS;

    MqttQuicConfigImplBuilder() {}

    MqttQuicConfigImplBuilder(final @Nullable MqttQuicConfigImpl quicConfig) {
        if (quicConfig != null) {
            applicationProtocols = quicConfig.getRawApplicationProtocols();
            handshakeTimeoutMs = quicConfig.getHandshakeTimeoutMs();
            maxIdleTimeoutMs = quicConfig.getMaxIdleTimeoutMs();
        }
    }

    abstract @NotNull B self();

    public @NotNull B applicationProtocol(final @Nullable String applicationProtocol) {
        this.applicationProtocols = ImmutableList.of(Checks.notEmpty(applicationProtocol, "Application protocol"));
        return self();
    }

    public @NotNull B applicationProtocols(final @Nullable Collection<String> applicationProtocols) {
        final ImmutableList<String> protocols =
                ImmutableList.copyOf(applicationProtocols, "Application protocols");
        if (protocols.isEmpty()) {
            throw new IllegalArgumentException("Application protocols must contain at least one protocol.");
        }
        for (int i = 0; i < protocols.size(); i++) {
            Checks.notEmpty(protocols.get(i), "Application protocol");
        }
        this.applicationProtocols = protocols;
        return self();
    }

    public @NotNull B handshakeTimeout(final long timeout, final @Nullable TimeUnit timeUnit) {
        Checks.notNull(timeUnit, "Time unit");
        this.handshakeTimeoutMs = (int) Checks.range(timeUnit.toMillis(timeout), 0, Integer.MAX_VALUE,
                "Handshake timeout in milliseconds");
        return self();
    }

    public @NotNull B maxIdleTimeout(final long timeout, final @Nullable TimeUnit timeUnit) {
        Checks.notNull(timeUnit, "Time unit");
        this.maxIdleTimeoutMs = (int) Checks.range(timeUnit.toMillis(timeout), 0, Integer.MAX_VALUE,
                "Idle timeout in milliseconds");
        return self();
    }

    public @NotNull MqttQuicConfigImpl build() {
        return new MqttQuicConfigImpl(applicationProtocols, handshakeTimeoutMs, maxIdleTimeoutMs);
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

    public static class Nested<P> extends MqttQuicConfigImplBuilder<Nested<P>>
            implements MqttQuicConfigBuilder.Nested<P> {

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
