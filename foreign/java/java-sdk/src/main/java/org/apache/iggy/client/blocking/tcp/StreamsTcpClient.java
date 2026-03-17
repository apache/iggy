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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.blocking.StreamsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;

import java.util.List;
import java.util.Optional;

final class StreamsTcpClient implements StreamsClient {

    private final org.apache.iggy.client.async.StreamsClient delegate;

    StreamsTcpClient(org.apache.iggy.client.async.StreamsClient delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<StreamDetails> getStream(StreamId streamId) {
        return FutureUtil.resolve(delegate.getStream(streamId));
    }

    @Override
    public List<StreamBase> getStreams() {
        return FutureUtil.resolve(delegate.getStreams());
    }

    @Override
    public StreamDetails createStream(String name) {
        return FutureUtil.resolve(delegate.createStream(name));
    }

    @Override
    public void updateStream(StreamId streamId, String name) {
        FutureUtil.resolve(delegate.updateStream(streamId, name));
    }

    @Override
    public void deleteStream(StreamId streamId) {
        FutureUtil.resolve(delegate.deleteStream(streamId));
    }
}
