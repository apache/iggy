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
import java.util.List;
import java.util.Optional;

/**
 * One page of the metadata change log, or of the current state.
 *
 * @param status outcome of the query, only {@code OK} carries records or items
 * @param phase whether the page holds change records or current state items
 * @param logId the answering node's log id, empty before it applied op 1
 * @param oldestOp lowest whole operation from which the requested groups are complete
 * @param retainedSince the requested groups are complete for every timestamp above this value, 0 when nothing was
 *     evicted
 * @param headOp highest metadata operation the answering node applied
 * @param next the cursor to send back: present on ok, echoed on behind, empty on expired and reset, and the anchor
 *     to follow from on the last page of a current state scan
 * @param hasMore more entries remain after {@code next}, or another state page exists
 * @param records change records in {@code (op, ordinal)} order, empty on a current state page
 * @param items current state items, empty on a change page
 * @param currentPageToken continuation of a current state scan, empty once it is complete
 */
public record MetadataChanges(
        MetadataChangeStatus status,
        MetadataChangePhase phase,
        Optional<BigInteger> logId,
        BigInteger oldestOp,
        BigInteger retainedSince,
        BigInteger headOp,
        Optional<MetadataChangeCursor> next,
        boolean hasMore,
        List<MetadataChangeRecord> records,
        List<MetadataCurrentItem> items,
        Optional<MetadataCurrentPageToken> currentPageToken) {

    public MetadataChanges {
        records = List.copyOf(records);
        items = List.copyOf(items);
    }
}
