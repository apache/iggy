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

package org.apache.iggy.connector.pinot.consumer;

import org.apache.pinot.spi.stream.MessageBatch;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamPartitionMsgOffset;

import java.util.concurrent.TimeoutException;

/**
 * Wrapper for IggyPartitionGroupConsumer to implement PartitionLevelConsumer interface.
 * Delegates all operations to the underlying partition group consumer.
 */
public class IggyPartitionLevelConsumer implements PartitionLevelConsumer {

    private final IggyPartitionGroupConsumer delegate;

    public IggyPartitionLevelConsumer(IggyPartitionGroupConsumer delegate) {
        this.delegate = delegate;
    }

    @Override
    public MessageBatch fetchMessages(StreamPartitionMsgOffset startOffset, int timeoutMs) throws TimeoutException {
        return delegate.fetchMessages(startOffset, timeoutMs);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
