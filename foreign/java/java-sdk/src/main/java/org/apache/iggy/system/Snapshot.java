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

import java.util.Arrays;

/**
 * A system snapshot containing the ZIP archive bytes the server returns.
 */
public record Snapshot(byte[] data) {

    public Snapshot {
        data = data.clone();
    }

    /** Returns a copy of the ZIP archive bytes. */
    @Override
    public byte[] data() {
        return data.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Snapshot snapshot && Arrays.equals(data, snapshot.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "Snapshot[" + data.length + " bytes]";
    }
}
