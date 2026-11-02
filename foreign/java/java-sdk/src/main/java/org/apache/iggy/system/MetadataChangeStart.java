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

import java.util.Objects;
import java.util.Optional;

/** Where a metadata changes query starts. */
public sealed interface MetadataChangeStart {

    /** The oldest operation the requested groups still hold completely. */
    static MetadataChangeStart earliest() {
        return new Earliest();
    }

    /** No records, only a cursor at the newest retained record of the requested groups. */
    static MetadataChangeStart latest() {
        return new Latest();
    }

    /** Records after {@code cursor}. */
    static MetadataChangeStart after(MetadataChangeCursor cursor) {
        return new After(cursor);
    }

    /** The first page of a current state scan. */
    static MetadataChangeStart current() {
        return new Current(Optional.empty());
    }

    /** The page of a current state scan that {@code pageToken} continues. */
    static MetadataChangeStart current(MetadataCurrentPageToken pageToken) {
        return new Current(Optional.of(pageToken));
    }

    /** The start mode code on the wire. */
    int asCode();

    /** The oldest operation the requested groups still hold completely. */
    record Earliest() implements MetadataChangeStart {
        @Override
        public int asCode() {
            return 0;
        }
    }

    /** No records, only a cursor at the newest retained record. */
    record Latest() implements MetadataChangeStart {
        @Override
        public int asCode() {
            return 1;
        }
    }

    /** Records after a change cursor. */
    record After(MetadataChangeCursor cursor) implements MetadataChangeStart {
        public After {
            Objects.requireNonNull(cursor, "cursor");
        }

        @Override
        public int asCode() {
            return 2;
        }
    }

    /** Current state pages, the first one without a page token. */
    record Current(Optional<MetadataCurrentPageToken> pageToken) implements MetadataChangeStart {
        public Current {
            Objects.requireNonNull(pageToken, "pageToken");
        }

        @Override
        public int asCode() {
            return 3;
        }
    }
}
