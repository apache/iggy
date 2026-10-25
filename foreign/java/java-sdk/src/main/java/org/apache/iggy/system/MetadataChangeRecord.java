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

import java.math.BigInteger;
import java.util.Optional;

/**
 * One change record: the committed operation that produced it, its position within that operation, and what is
 * true after it.
 *
 * @param op the metadata operation that produced the record
 * @param ordinal position of the record within {@code op}
 * @param checksum prepare checksum of {@code op}
 * @param timestamp prepare timestamp of {@code op}, in microseconds since the Unix epoch
 * @param actorUserId the acting user, empty for server actions
 * @param kind the kind, empty for a record this build does not know
 * @param body the typed body, or {@link MetadataChangeBody.Unknown} and {@link MetadataChangeBody.UnknownJson} for
 *     a record this build does not know. A mirror that cannot apply a record stops without advancing its cursor.
 */
public record MetadataChangeRecord(
        BigInteger op,
        long ordinal,
        BigInteger checksum,
        BigInteger timestamp,
        Optional<Long> actorUserId,
        Optional<MetadataChangeKind> kind,
        MetadataChangeBody body) {

    /** The group, empty for a record this build does not know. */
    public Optional<MetadataChangeGroup> group() {
        return kind.map(MetadataChangeKind::group);
    }
}
