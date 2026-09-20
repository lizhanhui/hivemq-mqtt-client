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

import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author HiveMQ
 */
class MqttQuicConfigImplTest {

    @Test
    void equals() {
        EqualsVerifier.forClass(MqttQuicConfigImpl.class).suppress(Warning.STRICT_INHERITANCE).verify();
    }

    @Test
    void defaults() {
        final MqttQuicConfigImpl quicConfig = new MqttQuicConfigImplBuilder.Default().build();

        assertEquals(MqttQuicConfigImpl.DEFAULT_INITIAL_MAX_DATA, quicConfig.getInitialMaxData());
        assertEquals(MqttQuicConfigImpl.DEFAULT_INITIAL_MAX_STREAM_DATA_BIDIRECTIONAL_LOCAL,
                quicConfig.getInitialMaxStreamDataBidirectionalLocal());
        assertEquals(MqttQuicConfigImpl.DEFAULT_INITIAL_MAX_STREAM_DATA_BIDIRECTIONAL_REMOTE,
                quicConfig.getInitialMaxStreamDataBidirectionalRemote());
        assertEquals(MqttQuicConfigImpl.DEFAULT_INITIAL_MAX_STREAMS_BIDIRECTIONAL,
                quicConfig.getInitialMaxStreamsBidirectional());
        assertEquals(MqttQuicConfigImpl.DEFAULT_MAX_IDLE_TIMEOUT_MS, quicConfig.getMaxIdleTimeoutMs());
    }

    @Test
    void builder() {
        final MqttQuicConfigImpl quicConfig = new MqttQuicConfigImplBuilder.Default().initialMaxData(1000)
                .initialMaxStreamDataBidirectionalLocal(100)
                .initialMaxStreamDataBidirectionalRemote(200)
                .initialMaxStreamsBidirectional(4)
                .maxIdleTimeout(5, TimeUnit.SECONDS)
                .build();

        assertEquals(1000, quicConfig.getInitialMaxData());
        assertEquals(100, quicConfig.getInitialMaxStreamDataBidirectionalLocal());
        assertEquals(200, quicConfig.getInitialMaxStreamDataBidirectionalRemote());
        assertEquals(4, quicConfig.getInitialMaxStreamsBidirectional());
        assertEquals(5000, quicConfig.getMaxIdleTimeoutMs());
    }

    @Test
    void builder_invalid_values() {
        final MqttQuicConfigImplBuilder.Default builder = new MqttQuicConfigImplBuilder.Default();

        assertThrows(IllegalArgumentException.class, () -> builder.initialMaxData(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.initialMaxStreamDataBidirectionalLocal(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.initialMaxStreamDataBidirectionalRemote(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.initialMaxStreamsBidirectional(-1));
        assertThrows(IllegalArgumentException.class, () -> builder.maxIdleTimeout(-1, TimeUnit.SECONDS));
    }

    @Test
    void extend() {
        final MqttQuicConfigImpl quicConfig = new MqttQuicConfigImplBuilder.Default().initialMaxData(1000).build();
        final MqttQuicConfigImpl extended = quicConfig.extend().build();

        assertEquals(quicConfig, extended);
    }
}
