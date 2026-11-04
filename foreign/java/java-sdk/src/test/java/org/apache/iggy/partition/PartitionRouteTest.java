/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.partition;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartitionRouteTest {
    @Test
    void shouldDecodeRouteWithoutInterpretingItsViewAsAContext() {
        ByteBuf reply = Unpooled.buffer()
                .writeLongLE(7)
                .writeLongLE(23)
                .writeIntLE(11)
                .writeIntLE(3)
                .writeByte(2);
        try {
            PartitionRoute route = PartitionRoute.decode(reply);
            assertThat(route).isEqualTo(new PartitionRoute(BigInteger.valueOf(7), BigInteger.valueOf(23), 11, 3, 2));
            assertThat(route.context())
                    .isEqualTo(new PartitionContext(BigInteger.valueOf(7), BigInteger.ZERO, BigInteger.valueOf(23)));
            assertThat(reply.isReadable()).isFalse();
            assertThat(PartitionContext.ENCODED_SIZE).isEqualTo(24);
        } finally {
            reply.release();
        }
    }

    @Test
    void shouldRejectLegacyContextAndTrailingRouteBytes() {
        for (int length : new int[] {24, 26}) {
            ByteBuf reply = Unpooled.buffer().writeZero(length);
            try {
                assertThatThrownBy(() -> PartitionRoute.decode(reply))
                        .isInstanceOf(IggyMalformedResponseException.class);
            } finally {
                reply.release();
            }
        }
    }
}
