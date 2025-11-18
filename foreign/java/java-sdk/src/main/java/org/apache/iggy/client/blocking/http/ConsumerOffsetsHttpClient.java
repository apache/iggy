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

import org.apache.hc.core5.http.message.BasicNameValuePair;
import org.apache.iggy.client.blocking.ConsumerOffsetsClient;
import org.apache.iggy.consumergroup.Consumer;
import org.apache.iggy.consumeroffset.ConsumerOffsetInfo;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import java.math.BigInteger;
import java.util.Optional;

class ConsumerOffsetsHttpClient implements ConsumerOffsetsClient {

    private static final String DEFAULT_PARTITION_ID = "1";
    private final InternalHttpClient httpClient;

    public ConsumerOffsetsHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public void storeConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer, BigInteger offset) {
        var request = httpClient.preparePutRequest(
                path(streamId, topicId), new StoreConsumerOffset(consumer.id().toString(), partitionId, offset));
        httpClient.execute(request);
    }

    @Override
    public Optional<ConsumerOffsetInfo> getConsumerOffset(
            StreamId streamId, TopicId topicId, Optional<Long> partitionId, Consumer consumer) {
        var request = httpClient.prepareGetRequest(
                path(streamId, topicId),
                new BasicNameValuePair("consumer_id", consumer.id().toString()),
                new BasicNameValuePair(
                        "partition_id", partitionId.map(Object::toString).orElse(DEFAULT_PARTITION_ID)));
        return httpClient.executeWithOptionalResponse(request, ConsumerOffsetInfo.class);
    }

    private static String path(StreamId streamId, TopicId topicId) {
        return "/streams/" + streamId + "/topics/" + topicId + "/consumer-offsets";
    }

    private record StoreConsumerOffset(String consumerId, Optional<Long> partitionId, BigInteger offset) {}
}
