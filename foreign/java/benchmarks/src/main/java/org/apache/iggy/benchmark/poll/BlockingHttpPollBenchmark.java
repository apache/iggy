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

package org.apache.iggy.benchmark.poll;

import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.client.blocking.http.IggyHttpClient;
import org.apache.iggy.message.PolledMessages;
import org.apache.iggy.message.PollingStrategy;
import org.openjdk.jmh.annotations.Benchmark;

import java.math.BigInteger;
import java.util.Optional;

public class BlockingHttpPollBenchmark extends BasePollBenchmark {

    private IggyHttpClient client;

    @Override
    protected void setupClient() {
        client = new IggyHttpClient("http://localhost:" + iggyContainer.getHttpPort());
        client.users().login("iggy", "iggy");
    }

    @Override
    protected void teardownClient() {}

    @Override
    protected IggyBaseClient getManagementClient() {
        return client;
    }

    @Benchmark
    public PolledMessages pollNext() {
        return client.messages()
                .pollMessages(
                        streamId, topicId, Optional.of(0L), consumer, PollingStrategy.next(), messagesPerPoll, false);
    }

    @Benchmark
    public PolledMessages pollOffset() {
        return client.messages()
                .pollMessages(
                        streamId,
                        topicId,
                        Optional.of(0L),
                        consumer,
                        PollingStrategy.offset(BigInteger.ZERO),
                        messagesPerPoll,
                        false);
    }
}
