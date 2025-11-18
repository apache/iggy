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

package org.apache.iggy.connector.flink.source;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * State for the Iggy source split enumerator.
 * Used for checkpointing and recovery.
 */
public class IggySourceEnumeratorState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Set<IggySourceSplit> assignedSplits;
    private final Set<Integer> discoveredPartitions;

    /**
     * Creates a new enumerator state.
     *
     * @param assignedSplits the splits that have been assigned to readers
     * @param discoveredPartitions the partition IDs that have been discovered
     */
    public IggySourceEnumeratorState(Set<IggySourceSplit> assignedSplits, Set<Integer> discoveredPartitions) {
        this.assignedSplits = new HashSet<>(assignedSplits);
        this.discoveredPartitions = new HashSet<>(discoveredPartitions);
    }

    /**
     * Creates an empty initial state.
     *
     * @return empty enumerator state
     */
    public static IggySourceEnumeratorState empty() {
        return new IggySourceEnumeratorState(Collections.emptySet(), Collections.emptySet());
    }

    public Set<IggySourceSplit> getAssignedSplits() {
        return Collections.unmodifiableSet(assignedSplits);
    }

    public Set<Integer> getDiscoveredPartitions() {
        return Collections.unmodifiableSet(discoveredPartitions);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        IggySourceEnumeratorState that = (IggySourceEnumeratorState) o;
        return Objects.equals(assignedSplits, that.assignedSplits)
                && Objects.equals(discoveredPartitions, that.discoveredPartitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignedSplits, discoveredPartitions);
    }

    @Override
    public String toString() {
        return "IggySourceEnumeratorState{"
                + "assignedSplits=" + assignedSplits.size()
                + ", discoveredPartitions=" + discoveredPartitions.size()
                + '}';
    }
}
