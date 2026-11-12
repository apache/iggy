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

package org.apache.iggy.client.async.reader;

import org.apache.iggy.message.PollingStrategy;
import org.apache.iggy.partition.PartitionContext;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iggy.client.async.reader.ReaderSupport.sameIncarnationAndOwner;

/**
 * The owners that the reads of one partition of a reader named. After the reader lost its group membership, as after
 * a lost session, the server installs the owner of each partition that the member gets again under a higher
 * generation, and it refuses a read or write that names an earlier one. So until a read names the new owner, the
 * partition names owner generation 0, which the server binds to the installed owner. The client and session check of
 * the server still refuses a member that does not own the partition.
 */
final class PartitionOwners {
    private final List<PartitionContext> named = new ArrayList<>();
    private BigInteger seenOwner = BigInteger.ZERO;
    private BigInteger lostOwner = BigInteger.ZERO;

    /** The context of a read, as the partition keeps it. */
    PartitionContext take(PartitionContext context) {
        if (!named(context)) {
            named.add(context);
        }
        PartitionContext taken = lost(context) ? withOwner(context, BigInteger.ZERO) : context;
        seenOwner = seenOwner.max(taken.ownerGeneration());
        return taken;
    }

    /**
     * Whether a read of the partition named the owner of {@code context}. A batch of an earlier assignment of the
     * partition names an owner that no read of this assignment named, so it decides nothing.
     */
    boolean named(PartitionContext context) {
        for (PartitionContext owner : named) {
            if (sameIncarnationAndOwner(owner, context)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The context of an offset write from {@code source}, with the owner of {@code current} instead of a lost one. The
     * server binds owner generation 0 only to an owner that it installed at or below the metadata op of the write, and
     * a new session installs it after every op of the session before. So a write that replaces a lost owner with 0
     * names at least {@code watermark}, the op that the client saw committed. It keeps the incarnation, which the
     * server checks, and the client sends incarnation 0 with the incarnation of the partition route. Any other write
     * keeps its op.
     */
    PartitionContext write(PartitionContext source, PartitionContext current, BigInteger watermark) {
        if (!lost(source)) {
            return source;
        }
        BigInteger owner = current.ownerGeneration();
        return new PartitionContext(
                source.incarnation(),
                owner,
                owner.signum() == 0 ? source.metadataOp().max(watermark) : source.metadataOp());
    }

    /** {@code position} without an owner of a lost membership. */
    PollingStrategy fence(PollingStrategy position) {
        return position.context()
                .filter(this::lost)
                .map(context -> position.withContext(withOwner(context, BigInteger.ZERO)))
                .orElse(position);
    }

    /** Follows a lost membership of a partition that reads at {@code position}, and returns its {@code context}. */
    PartitionContext lose(PollingStrategy position, PartitionContext context) {
        lostOwner = position.context()
                .map(PartitionContext::ownerGeneration)
                .orElse(BigInteger.ZERO)
                .max(seenOwner);
        return take(context);
    }

    /** Whether {@code context} names an owner of a membership that the reader lost. */
    boolean lost(PartitionContext context) {
        return lostOwner.signum() > 0 && context.ownerGeneration().compareTo(lostOwner) <= 0;
    }

    private static PartitionContext withOwner(PartitionContext context, BigInteger owner) {
        return new PartitionContext(context.incarnation(), owner, context.metadataOp());
    }
}
