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

package com.hivemq.client.internal.mqtt.handler.quic;

import com.hivemq.client.internal.mqtt.MqttClientSslConfigImplBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author HiveMQ
 */
class MqttQuicInitializerTest {

    @Test
    void usesJdkEndpointIdentification_when_no_custom_verifier_on_jvm() {
        assertTrue(MqttQuicInitializer.usesJdkEndpointIdentification(
                new MqttClientSslConfigImplBuilder.Default().build()));
    }

    @Test
    void usesJdkEndpointIdentification_false_with_custom_verifier() {
        assertFalse(MqttQuicInitializer.usesJdkEndpointIdentification(
                new MqttClientSslConfigImplBuilder.Default().hostnameVerifier((hostname, session) -> true).build()));
    }

    @Test
    void hostnameVerifierForHandshake_null_on_jvm_default() {
        assertNull(MqttQuicInitializer.hostnameVerifierForHandshake(
                new MqttClientSslConfigImplBuilder.Default().build()));
    }

    @Test
    void hostnameVerifierForHandshake_keeps_custom_verifier() {
        assertNotNull(MqttQuicInitializer.hostnameVerifierForHandshake(
                new MqttClientSslConfigImplBuilder.Default().hostnameVerifier((hostname, session) -> true).build()));
    }
}
