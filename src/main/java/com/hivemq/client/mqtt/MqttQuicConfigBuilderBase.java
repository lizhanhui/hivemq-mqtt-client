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

import java.util.concurrent.TimeUnit;

/**
 * Builder base for a {@link MqttQuicConfig}.
 *
 * @param <B> the type of the builder.
 * @author HiveMQ
 * @since 1.5
 */
@DoNotImplement
public interface MqttQuicConfigBuilderBase<B extends MqttQuicConfigBuilderBase<B>> {

    /**
     * Sets the {@link MqttQuicConfig#getInitialMaxData() connection-level flow control limit}.
     *
     * @param initialMaxData the connection-level flow control limit in bytes.
     * @return the builder.
     */
    @CheckReturnValue
    @NotNull B initialMaxData(long initialMaxData);

    /**
     * Sets the {@link MqttQuicConfig#getInitialMaxStreamDataBidirectionalLocal() stream-level flow control limit for
     * locally initiated bidirectional streams}.
     *
     * @param initialMaxStreamDataBidirectionalLocal the stream-level flow control limit in bytes.
     * @return the builder.
     */
    @CheckReturnValue
    @NotNull B initialMaxStreamDataBidirectionalLocal(long initialMaxStreamDataBidirectionalLocal);

    /**
     * Sets the {@link MqttQuicConfig#getInitialMaxStreamDataBidirectionalRemote() stream-level flow control limit for
     * remotely initiated bidirectional streams}.
     *
     * @param initialMaxStreamDataBidirectionalRemote the stream-level flow control limit in bytes.
     * @return the builder.
     */
    @CheckReturnValue
    @NotNull B initialMaxStreamDataBidirectionalRemote(long initialMaxStreamDataBidirectionalRemote);

    /**
     * Sets the {@link MqttQuicConfig#getInitialMaxStreamsBidirectional() maximum number of bidirectional streams the
     * remote peer is allowed to open}.
     *
     * @param initialMaxStreamsBidirectional the maximum number of bidirectional streams.
     * @return the builder.
     */
    @CheckReturnValue
    @NotNull B initialMaxStreamsBidirectional(long initialMaxStreamsBidirectional);

    /**
     * Sets the {@link MqttQuicConfig#getMaxIdleTimeoutMs() maximum idle timeout}.
     * <p>
     * The timeout in milliseconds must be in the range: [0, {@link Long#MAX_VALUE}].
     *
     * @param timeout  the maximum idle timeout or <code>0</code> to disable the timeout.
     * @param timeUnit the time unit of the given timeout (this timeout only supports millisecond precision).
     * @return the builder.
     */
    @CheckReturnValue
    @NotNull B maxIdleTimeout(long timeout, @NotNull TimeUnit timeUnit);
}
