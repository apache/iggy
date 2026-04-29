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

package org.apache.iggy.bench.models.cli;

import org.apache.iggy.bench.exception.BenchmarkException;

public record PinnedProducerCliArgs(int streams, int producers, long maxTopicSize, long messageExpiry) {

    public void validate() {
        if (producers <= 0) {
            throw new BenchmarkException("--producers must be greater than 0.");
        }
        if (streams <= 0) {
            throw new BenchmarkException("--streams must be greater than 0.");
        }
        if (streams != producers) {
            throw new BenchmarkException("For pinned producer, --streams must match --producers.");
        }
        if (maxTopicSize < 0) {
            throw new BenchmarkException("--max-topic-size must be greater than or equal to 0.");
        }
        if (messageExpiry < 0L) {
            throw new BenchmarkException("--message-expiry must be greater than or equal to 0.");
        }
    }
}
