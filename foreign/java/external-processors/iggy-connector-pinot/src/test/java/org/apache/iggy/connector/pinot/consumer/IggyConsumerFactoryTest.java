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

import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.StreamConfig;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IggyConsumerFactoryTest {

    @Test
    void multiTopicConsumerReadsStreamPartition() {
        PartitionGroupConsumptionStatus status = new PartitionGroupConsumptionStatus(
                10001, 1, 0, new IggyStreamPartitionMsgOffset(0), null, "IN_PROGRESS");

        PartitionGroupConsumer consumer = createFactory().createPartitionGroupConsumer("test-client", status);

        assertThat(consumer).extracting("partitionId").isEqualTo(1);
    }

    @Test
    void singleTopicConsumerReadsPartitionGroup() {
        PartitionGroupConsumptionStatus status =
                new PartitionGroupConsumptionStatus(3, 0, new IggyStreamPartitionMsgOffset(0), null, "IN_PROGRESS");

        PartitionGroupConsumer consumer = createFactory().createPartitionGroupConsumer("test-client", status);

        assertThat(consumer).extracting("partitionId").isEqualTo(3);
    }

    private static IggyConsumerFactory createFactory() {
        Map<String, String> props = new HashMap<>();
        props.put("streamType", "iggy");
        props.put("stream.iggy.topic.name", "events");
        props.put("stream.iggy.consumer.type", "lowlevel");
        props.put(
                "stream.iggy.consumer.factory.class.name",
                "org.apache.iggy.connector.pinot.consumer.IggyConsumerFactory");
        props.put("stream.iggy.decoder.class.name", "org.apache.iggy.connector.pinot.decoder.IggyJsonMessageDecoder");
        props.put("stream.iggy.host", "localhost");
        props.put("stream.iggy.stream.id", "analytics");
        props.put("stream.iggy.topic.id", "events");
        props.put("stream.iggy.consumer.group", "test-consumer-group");

        IggyConsumerFactory factory = new IggyConsumerFactory();
        factory.init(new StreamConfig("test_table_REALTIME", props));
        return factory;
    }
}
