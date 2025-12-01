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

package org.apache.iggy.benchmark.send.batch;

import org.apache.iggy.benchmark.util.BlockingAsyncClientAdapter;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClient;
import org.apache.iggy.client.blocking.IggyBaseClient;
import org.apache.iggy.message.Partitioning;
import org.openjdk.jmh.annotations.Benchmark;

public class AsyncTcpSendBatchBenchmark extends BaseBatchMessageSendBenchmark {

    private AsyncIggyTcpClient asyncClient;

    @Override
    protected void setupClient() throws Exception {
        asyncClient = new AsyncIggyTcpClient("localhost", iggyContainer.getTcpPort());
        asyncClient
                .connect()
                .thenCompose(v -> asyncClient.users().loginAsync("iggy", "iggy"))
                .get();
    }

    @Override
    protected void teardownClient() throws Exception {
        asyncClient.close().get();
    }

    @Override
    protected IggyBaseClient getManagementClient() {
        return new BlockingAsyncClientAdapter(asyncClient);
    }

    @Benchmark
    public void send() throws Exception {
        asyncClient
                .messages()
                .sendMessagesAsync(streamId, topicId, Partitioning.partitionId(0L), messages)
                .get();
    }
}
