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

import org.apache.iggy.exception.IggyInvalidArgumentException;

/** The event on which {@link AutoCommit} also stores. */
public sealed interface CommitEvery permits CommitEvery.Message, CommitEvery.Messages, CommitEvery.Batch {

    /** Each counted message, or each counted batch of a partition reader. */
    static CommitEvery message() {
        return new Message();
    }

    /** This many counted messages of a partition since its last store. */
    static CommitEvery messages(long count) {
        return new Messages(count);
    }

    /** The last message of each batch. */
    static CommitEvery batch() {
        return new Batch();
    }

    record Message() implements CommitEvery {}

    record Messages(long count) implements CommitEvery {
        public Messages {
            if (count < 1 || count > 0xFFFF_FFFFL) {
                throw new IggyInvalidArgumentException("A commit every " + count + " messages needs 1 to 4294967295");
            }
        }
    }

    record Batch() implements CommitEvery {}
}
