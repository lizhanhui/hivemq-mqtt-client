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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.jetbrains.annotations.NotNull;

/**
 * Closes the QUIC connection and UDP transport when the MQTT stream becomes inactive.
 *
 * @author Silvio Giebl
 */
public class MqttQuicParentCloser extends ChannelInboundHandlerAdapter {

    static final @NotNull String NAME = "quic.parent";

    @Override
    public void channelInactive(final @NotNull ChannelHandlerContext ctx) {
        ctx.fireChannelInactive();
        closeParents(ctx.channel());
    }

    static void closeParents(final @NotNull Channel stream) {
        final Channel quicChannel = stream.parent();
        if (quicChannel != null) {
            final Channel datagramChannel = quicChannel.parent();
            quicChannel.close();
            if (datagramChannel != null) {
                datagramChannel.close();
            }
        }
    }
}
