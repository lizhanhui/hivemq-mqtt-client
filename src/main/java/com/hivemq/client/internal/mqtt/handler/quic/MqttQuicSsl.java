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

import com.hivemq.client.internal.mqtt.MqttClientConfig;
import com.hivemq.client.internal.mqtt.MqttClientSslConfigImpl;
import com.hivemq.client.internal.mqtt.MqttQuicConfigImpl;
import com.hivemq.client.internal.util.collections.ImmutableList;
import io.netty.handler.codec.quic.QuicSslContext;
import io.netty.handler.codec.quic.QuicSslContextBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Builds a {@link QuicSslContext} from the client's SSL and QUIC configuration.
 *
 * @author Silvio Giebl
 */
final class MqttQuicSsl {

    private static final @NotNull String ENDPOINT_IDENTIFICATION_ALGORITHM = "HTTPS";

    static @NotNull QuicSslContext sslContext(
            final @NotNull MqttClientConfig clientConfig,
            final @NotNull MqttClientSslConfigImpl sslConfig,
            final @NotNull MqttQuicConfigImpl quicConfig) {

        final Object cached = clientConfig.getCurrentQuicSslContext();
        if (cached instanceof QuicSslContext) {
            return (QuicSslContext) cached;
        }
        final QuicSslContext sslContext = createSslContext(sslConfig, quicConfig);
        clientConfig.setCurrentQuicSslContext(sslContext);
        return sslContext;
    }

    static @NotNull QuicSslContext createSslContext(
            final @NotNull MqttClientSslConfigImpl sslConfig, final @NotNull MqttQuicConfigImpl quicConfig) {

        final ImmutableList<String> applicationProtocols = quicConfig.getRawApplicationProtocols();
        final QuicSslContextBuilder builder = QuicSslContextBuilder.forClient()
                .trustManager(sslConfig.getRawTrustManagerFactory())
                .keyManager(sslConfig.getRawKeyManagerFactory(), null)
                .applicationProtocols(applicationProtocols.toArray(new String[0]));
        if (sslConfig.getRawHostnameVerifier() == null) {
            builder.endpointIdentificationAlgorithm(ENDPOINT_IDENTIFICATION_ALGORITHM);
        } else {
            builder.endpointIdentificationAlgorithm(null);
        }
        return builder.build();
    }

    private MqttQuicSsl() {}
}
