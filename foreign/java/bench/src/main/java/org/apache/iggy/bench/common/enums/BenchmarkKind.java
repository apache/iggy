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

package org.apache.iggy.bench.common.enums;

public enum BenchmarkKind {
    PINNED_PRODUCER("pinned_producer"),
    PINNED_CONSUMER("pinned_consumer"),
    PINNED_PRODUCER_AND_CONSUMER("pinned_producer_and_consumer"),
    BALANCED_PRODUCER("balanced_producer"),
    BALANCED_CONSUMER_GROUP("balanced_consumer_group"),
    BALANCED_PRODUCER_AND_CONSUMER_GROUP("balanced_producer_and_consumer_group"),
    END_TO_END_PRODUCING_CONSUMER("end_to_end_producing_consumer"),
    END_TO_END_PRODUCING_CONSUMER_GROUP("end_to_end_producing_consumer_group");

    private final String value;

    BenchmarkKind(String value) {
        this.value = value;
    }

    public String value() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
}
