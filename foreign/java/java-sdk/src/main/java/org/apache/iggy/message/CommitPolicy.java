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

package org.apache.iggy.message;

import java.util.Objects;

/** When the offsets of the messages that a read returns are stored. */
public sealed interface CommitPolicy permits CommitPolicy.Manual, CommitPolicy.OnRead, CommitPolicy.Auto {

    /** Nothing is stored. The application commits checkpoints. The default. */
    static CommitPolicy manual() {
        return Manual.INSTANCE;
    }

    /**
     * The server stores the offset of the last message of each read as part of the read, in one round trip. While the
     * primary stays up a message is delivered at most once. A failover before the store replicates can deliver the
     * last batch again.
     */
    static CommitPolicy onRead() {
        return OnRead.INSTANCE;
    }

    /**
     * The client stores the latest counted position, at least once with {@link CommitAfter#PROCESSING}. A one-shot
     * poll refuses it, because it has nothing to count. A partition reader and a consumer accept it.
     */
    static CommitPolicy auto(AutoCommit autoCommit) {
        return new Auto(autoCommit);
    }

    /** {@link #auto(AutoCommit)} with {@link AutoCommit#defaults()}. */
    static CommitPolicy auto() {
        return new Auto(AutoCommit.defaults());
    }

    record Manual() implements CommitPolicy {
        private static final Manual INSTANCE = new Manual();
    }

    record OnRead() implements CommitPolicy {
        private static final OnRead INSTANCE = new OnRead();
    }

    record Auto(AutoCommit autoCommit) implements CommitPolicy {
        public Auto {
            Objects.requireNonNull(autoCommit, "autoCommit");
        }
    }
}
