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

package org.apache.iggy.client.blocking.http;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.iggy.client.blocking.StreamsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.stream.StreamBase;
import org.apache.iggy.stream.StreamDetails;

import java.util.List;
import java.util.Optional;

class StreamsHttpClient implements StreamsClient {

    private static final String STREAMS = "/streams";
    private final InternalHttpClient httpClient;

    public StreamsHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Optional<StreamDetails> getStream(StreamId streamId) {
        var request = httpClient.prepareGetRequest(STREAMS + "/" + streamId);
        return httpClient.executeWithOptionalResponse(request, StreamDetails.class);
    }

    @Override
    public List<StreamBase> getStreams() {
        var request = httpClient.prepareGetRequest(STREAMS);
        return httpClient.execute(request, new TypeReference<>() {});
    }

    @Override
    public StreamDetails createStream(String name) {
        var request = httpClient.preparePostRequest(STREAMS, new CreateStream(name));
        return httpClient.execute(request, new TypeReference<>() {});
    }

    @Override
    public void updateStream(StreamId streamId, String name) {
        var request = httpClient.preparePutRequest(STREAMS + "/" + streamId, new UpdateStream(name));
        httpClient.execute(request);
    }

    @Override
    public void deleteStream(StreamId streamId) {
        var request = httpClient.prepareDeleteRequest(STREAMS + "/" + streamId);
        httpClient.execute(request);
    }

    record CreateStream(String name) {}

    record UpdateStream(String name) {}
}
