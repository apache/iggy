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

import org.apache.iggy.benchmark.send.BaseSendBenchmark;
import org.apache.iggy.benchmark.util.MessageFixtures;
import org.apache.iggy.message.Message;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;

import java.util.List;

public abstract class BaseBatchMessageSendBenchmark extends BaseSendBenchmark {

    @Param({"100", "1000"})
    public int payloadSizeBytes;

    @Param({"10", "100", "1000"})
    public int messagesPerBatch;

    protected List<Message> messages;

    @Setup(Level.Iteration)
    public void generateMessages() {
        messages = MessageFixtures.generateMessages(messagesPerBatch, payloadSizeBytes);
    }
}
