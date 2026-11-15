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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

/**
 * The reads of one group in one turn of a {@link PartitionReader}. Its quiet partitions share the one read that may
 * wait. Its backlogged partitions spread over the extra slots that the group gets, each slot a read of its own that
 * returns at once.
 */
final class GroupPlan {
    private final int index;
    private final List<Partition> quiet = new ArrayList<>();
    // The least recently served first.
    private final List<Partition> backlogged = new ArrayList<>();
    private int extra;

    GroupPlan(int index, List<Partition> ready) {
        this.index = index;
        ready.forEach(partition -> (partition.backlogged ? backlogged : quiet).add(partition));
    }

    /**
     * Deals {@code slots} to {@code plans}, one slot to each plan in turn, while a plan has room for it.
     *
     * @return the slots that no plan has room for
     */
    static int deal(List<GroupPlan> plans, int slots) {
        int left = slots;
        boolean dealt = true;
        while (left > 0 && dealt) {
            dealt = false;
            for (GroupPlan plan : plans) {
                if (left > 0 && plan.extra < plan.room()) {
                    plan.extra++;
                    left--;
                    dealt = true;
                }
            }
        }
        return left;
    }

    /**
     * Under {@code OnRead} a read probes each target that has no known start before it sends its own request. The
     * first probe takes the slot of the read and each other one a slot of {@code room}. The targets without a slot
     * wait for a later read.
     *
     * @return the slots of {@code room} that the probes take
     */
    static int fitProbes(List<Partition> targets, int room, Predicate<Partition> probes) {
        int probing = 0;
        for (Iterator<Partition> iterator = targets.iterator(); iterator.hasNext(); ) {
            if (probes.test(iterator.next())) {
                if (probing > room) {
                    iterator.remove();
                } else {
                    probing++;
                }
            }
        }
        return Math.max(0, probing - 1);
    }

    int index() {
        return index;
    }

    /** The partitions of the read that may wait: the quiet ones, and the backlogged ones if no read returns at once. */
    List<Partition> waiting() {
        List<Partition> waiting = new ArrayList<>(quiet);
        if (reads() == 0) {
            waiting.addAll(backlogged);
        }
        return waiting;
    }

    /** The partitions of each read that returns at once. */
    List<List<Partition>> spread() {
        int reads = reads();
        List<List<Partition>> spread = new ArrayList<>();
        for (int read = 0; read < reads; read++) {
            spread.add(new ArrayList<>());
        }
        for (int at = 0; reads > 0 && at < backlogged.size(); at++) {
            spread.get(at % reads).add(backlogged.get(at));
        }
        return spread;
    }

    /** The extra slots that the group can use, so that every read holds a backlogged partition. */
    private int room() {
        return backlogged.size() - (quiet.isEmpty() ? 1 : 0);
    }

    /** The reads that return at once: one per extra slot, and the slot of the group itself without quiet partitions. */
    private int reads() {
        return extra + (quiet.isEmpty() ? 1 : 0);
    }
}
