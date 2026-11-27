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

package org.apache.iggy.system;

import org.junit.jupiter.api.Test;

import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotTest {

    @Test
    void shouldKeepItsValueWhenCallerChangesArchiveBytes() {
        byte[] data = {1, 2, 3};
        var snapshot = new Snapshot(data);
        var snapshots = new HashSet<Snapshot>();
        snapshots.add(snapshot);

        data[0] = 4;
        byte[] returnedData = snapshot.data();
        returnedData[1] = 5;

        assertThat(snapshot.data()).containsExactly(1, 2, 3);
        assertThat(snapshots.contains(snapshot)).isTrue();
    }

    @Test
    void shouldNotEqualAnUnrelatedObject() {
        var snapshot = new Snapshot(new byte[] {1, 2, 3});

        assertThat(snapshot).isNotEqualTo(new Object());
    }
}
