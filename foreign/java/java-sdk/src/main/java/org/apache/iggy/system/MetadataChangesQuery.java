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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A metadata changes query. {@code groups} empty means every group. {@code since} and {@code until} keep records
 * whose prepare timestamp, in microseconds since the Unix epoch, is in the inclusive range, and apply only to
 * {@link MetadataChangeStart#earliest()} and {@link MetadataChangeStart#after(MetadataChangeCursor)}.
 *
 * @param start where the query starts
 * @param groups the groups to read, duplicates ignored, empty for every group
 * @param since lowest prepare timestamp to keep
 * @param until highest prepare timestamp to keep
 * @param limit records or items per page, 1 to 1000
 */
public record MetadataChangesQuery(
        MetadataChangeStart start,
        List<MetadataChangeGroup> groups,
        Optional<BigInteger> since,
        Optional<BigInteger> until,
        long limit) {

    /** Records or items per page when the caller does not say. */
    public static final long DEFAULT_LIMIT = 100;

    /** Most records or items one page returns. */
    public static final long MAX_LIMIT = 1000;

    public MetadataChangesQuery {
        Objects.requireNonNull(start, "start");
        groups = List.copyOf(groups);
        Objects.requireNonNull(since, "since");
        Objects.requireNonNull(until, "until");
    }

    public static Builder builder() {
        return new Builder();
    }

    /** The same query starting elsewhere, how a follow loop moves to the cursor an answer returned. */
    public MetadataChangesQuery withStart(MetadataChangeStart newStart) {
        return new MetadataChangesQuery(newStart, groups, since, until, limit);
    }

    /**
     * Applies the rules every transport and the server share, so a refused query is never sent.
     *
     * @throws org.apache.iggy.exception.IggyValidationException invalid command naming the broken rule
     */
    public void validate() {
        if (limit < 1 || limit > MAX_LIMIT) {
            throw MetadataChangeEncoding.refused("The limit must be 1 to " + MAX_LIMIT + ", got " + limit);
        }
        validateBounds();
        validateStart();
    }

    /** The query string of {@code GET /metadata/changes}, in the order the server documents. */
    public String httpQuery() {
        List<String> pairs = new ArrayList<>(6);
        if (start instanceof MetadataChangeStart.After after) {
            pairs.add("after=" + after.cursor());
        } else if (start instanceof MetadataChangeStart.Current current) {
            pairs.add("from=current");
            current.pageToken().ifPresent(token -> pairs.add("current_after=" + token));
        } else {
            pairs.add(start instanceof MetadataChangeStart.Latest ? "from=latest" : "from=earliest");
        }
        if (!groups.isEmpty()) {
            pairs.add("groups="
                    + MetadataChangeGroup.fromMask(MetadataChangeGroup.mask(groups)).stream()
                            .map(MetadataChangeGroup::value)
                            .collect(Collectors.joining(",")));
        }
        since.ifPresent(value -> pairs.add("since=" + value));
        until.ifPresent(value -> pairs.add("until=" + value));
        pairs.add("limit=" + limit);
        return String.join("&", pairs);
    }

    private void validateBounds() {
        if (since.filter(MetadataChangesQuery::outsideU64).isPresent()
                || until.filter(MetadataChangesQuery::outsideU64).isPresent()) {
            throw MetadataChangeEncoding.refused("Time bounds are unsigned 64-bit microseconds");
        }
        if (since.isPresent() && until.isPresent() && since.get().compareTo(until.get()) > 0) {
            throw MetadataChangeEncoding.refused("Since must not be above until");
        }
    }

    private void validateStart() {
        boolean hasTimeBound = since.isPresent() || until.isPresent();
        if (start instanceof MetadataChangeStart.After after && !after.cursor().isCanonical()) {
            throw MetadataChangeEncoding.refused("A cursor at op 0 carries no checksum and no ordinal");
        }
        boolean timeBoundAllowed =
                start instanceof MetadataChangeStart.Earliest || start instanceof MetadataChangeStart.After;
        if (hasTimeBound && !timeBoundAllowed) {
            throw MetadataChangeEncoding.refused(
                    "Time bounds apply only when starting at the earliest record or after a cursor");
        }
        if (start instanceof MetadataChangeStart.Current current) {
            validateCurrent(current);
        }
    }

    private static boolean outsideU64(BigInteger value) {
        return value.signum() < 0 || value.bitLength() > Long.SIZE;
    }

    private void validateCurrent(MetadataChangeStart.Current current) {
        int mask = MetadataChangeGroup.mask(groups);
        for (MetadataChangeGroup group : MetadataChangeGroup.fromMask(mask)) {
            for (MetadataChangeGroup parent : group.parents()) {
                if ((mask & parent.bit()) == 0) {
                    throw MetadataChangeEncoding.refused("A current state scan of " + group.value()
                            + " must include its parent group " + parent.value());
                }
            }
        }
        if (current.pageToken().filter(token -> token.groupsMask() != mask).isPresent()) {
            throw MetadataChangeEncoding.refused("The page token was issued for other groups");
        }
    }

    /** Builds a query from the defaults: earliest, every group, no time bounds and a limit of 100. */
    public static final class Builder {
        private MetadataChangeStart start = MetadataChangeStart.earliest();
        private List<MetadataChangeGroup> groups = List.of();
        private Optional<BigInteger> since = Optional.empty();
        private Optional<BigInteger> until = Optional.empty();
        private long limit = DEFAULT_LIMIT;

        private Builder() {}

        public Builder start(MetadataChangeStart value) {
            start = value;
            return this;
        }

        public Builder groups(List<MetadataChangeGroup> value) {
            groups = value;
            return this;
        }

        public Builder groups(MetadataChangeGroup... value) {
            groups = List.of(value);
            return this;
        }

        public Builder since(BigInteger value) {
            since = Optional.of(value);
            return this;
        }

        public Builder until(BigInteger value) {
            until = Optional.of(value);
            return this;
        }

        public Builder limit(long value) {
            limit = value;
            return this;
        }

        public MetadataChangesQuery build() {
            return new MetadataChangesQuery(start, groups, since, until, limit);
        }
    }
}
