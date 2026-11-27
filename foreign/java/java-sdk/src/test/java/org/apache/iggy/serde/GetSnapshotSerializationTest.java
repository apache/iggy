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

package org.apache.iggy.serde;

import io.netty.buffer.ByteBufUtil;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.system.Snapshot;
import org.apache.iggy.system.SnapshotCompression;
import org.apache.iggy.system.SystemSnapshotType;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GetSnapshotSerializationTest {

    @Test
    void shouldEncodeCompressionThenCountThenTypeCodes() {
        // when
        var payload = BytesSerializer.toGetSnapshot(
                SnapshotCompression.Deflated,
                List.of(SystemSnapshotType.FilesystemOverview, SystemSnapshotType.ServerLogs));

        // then the layout matches core/binary_protocol/src/requests/system/get_snapshot.rs
        try {
            assertThat(ByteBufUtil.getBytes(payload)).containsExactly(2, 2, 1, 5);
        } finally {
            payload.release();
        }
    }

    @Test
    void shouldEncodeAllAsCode100() {
        var payload = BytesSerializer.toGetSnapshot(SnapshotCompression.Zstd, List.of(SystemSnapshotType.All));

        try {
            assertThat(ByteBufUtil.getBytes(payload)).containsExactly(4, 1, 100);
        } finally {
            payload.release();
        }
    }

    @Test
    void shouldEncodeAnEmptyTypeListAsCountZero() {
        var payload = BytesSerializer.toGetSnapshot(SnapshotCompression.Stored, List.of());

        try {
            assertThat(ByteBufUtil.getBytes(payload)).containsExactly(1, 0);
        } finally {
            payload.release();
        }
    }

    @Test
    void shouldRefuseMoreTypesThanTheOneByteCountHolds() {
        // a count of 256 would wrap to 0 on the wire and the server would read a different request
        var tooMany = Collections.nCopies(256, SystemSnapshotType.Test);

        assertThatThrownBy(() -> BytesSerializer.toGetSnapshot(SnapshotCompression.Deflated, tooMany))
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void shouldRejectNullSnapshotTypes() {
        assertThatThrownBy(() ->
                        BytesSerializer.toGetSnapshot(SnapshotCompression.Deflated, Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldMatchTheServerCodesForEveryEnumValue() {
        // codes from core/common/src/types/snapshot/mod.rs, the server reads only these numbers
        assertThat(List.of(SnapshotCompression.values()).stream().map(SnapshotCompression::asCode))
                .containsExactly(1, 2, 3, 4, 5, 6);
        assertThat(List.of(SystemSnapshotType.values()).stream().map(SystemSnapshotType::asCode))
                .containsExactly(1, 2, 3, 4, 5, 6, 100);
    }

    @Test
    void snapshotsWithTheSameBytesShouldBeEqual() {
        var snapshot = new Snapshot(new byte[] {'P', 'K', 3, 4});

        assertThat(snapshot).isEqualTo(new Snapshot(new byte[] {'P', 'K', 3, 4}));
        assertThat(snapshot).hasSameHashCodeAs(new Snapshot(new byte[] {'P', 'K', 3, 4}));
        assertThat(snapshot).isNotEqualTo(new Snapshot(new byte[] {'P', 'K'}));
        assertThat(snapshot).hasToString("Snapshot[4 bytes]");
    }
}
