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

import org.apache.iggy.connector.pinot.config.IggyStreamConfig;
import org.apache.iggy.connector.pinot.metadata.IggyStreamMetadataProvider;
import org.apache.pinot.spi.stream.PartitionGroupConsumer;
import org.apache.pinot.spi.stream.PartitionGroupConsumptionStatus;
import org.apache.pinot.spi.stream.PartitionLevelConsumer;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.stream.StreamConsumerFactory;
import org.apache.pinot.spi.stream.StreamMetadataProvider;

/**
 * Factory for creating Iggy stream consumers and metadata providers.
 * This is the main entry point for Pinot's stream ingestion framework to interact with Iggy.
 *
 * <p>Configuration in Pinot table config:
 * <pre>{@code
 * "streamConfigs": {
 *   "streamType": "iggy",
 *   "stream.iggy.consumer.factory.class.name": "org.apache.iggy.connector.pinot.consumer.IggyConsumerFactory",
 *   "stream.iggy.host": "localhost",
 *   "stream.iggy.port": "8090",
 *   "stream.iggy.username": "iggy",
 *   "stream.iggy.password": "iggy",
 *   "stream.iggy.stream.id": "my-stream",
 *   "stream.iggy.topic.id": "my-topic",
 *   "stream.iggy.consumer.group": "pinot-consumer-group",
 *   "stream.iggy.poll.batch.size": "100"
 * }
 * }</pre>
 */
public class IggyConsumerFactory extends StreamConsumerFactory {

    private StreamConfig streamConfig;

    @Override
    public void init(StreamConfig streamConfig) {
        this.streamConfig = streamConfig;
    }

    /**
     * Creates a partition-level consumer for reading from a specific Iggy partition.
     * Pinot calls this method for each partition that needs to be consumed.
     *
     * @param clientId unique identifier for this consumer instance
     * @param partitionGroupConsumptionStatus consumption status containing partition group ID and offset info
     * @return a new partition consumer instance
     */
    @Override
    public PartitionGroupConsumer createPartitionGroupConsumer(
            String clientId, PartitionGroupConsumptionStatus partitionGroupConsumptionStatus) {
        IggyStreamConfig iggyConfig = new IggyStreamConfig(this.streamConfig);
        int partitionGroupId = partitionGroupConsumptionStatus.getPartitionGroupId();
        return new IggyPartitionGroupConsumer(clientId, iggyConfig, partitionGroupId);
    }

    /**
     * Creates a partition-level consumer (newer Pinot API).
     * Wraps the partition group consumer for compatibility.
     *
     * @param clientId unique identifier for this consumer instance
     * @param partition partition identifier
     * @return a new partition consumer instance
     */
    @Override
    public PartitionLevelConsumer createPartitionLevelConsumer(String clientId, int partition) {
        IggyStreamConfig iggyConfig = new IggyStreamConfig(this.streamConfig);
        IggyPartitionGroupConsumer groupConsumer = new IggyPartitionGroupConsumer(clientId, iggyConfig, partition);
        return new IggyPartitionLevelConsumer(groupConsumer);
    }

    /**
     * Creates a metadata provider for querying stream information.
     * Used by Pinot to discover partitions and check offset positions.
     *
     * @param clientId unique identifier for this metadata provider instance
     * @param groupId partition group identifier
     * @return a new metadata provider instance
     */
    @Override
    public StreamMetadataProvider createPartitionMetadataProvider(String clientId, int groupId) {
        IggyStreamConfig iggyConfig = new IggyStreamConfig(this.streamConfig);
        return new IggyStreamMetadataProvider(clientId, iggyConfig, groupId);
    }

    /**
     * Creates a metadata provider for the entire stream (all partitions).
     *
     * @param clientId unique identifier for this metadata provider instance
     * @return a new metadata provider instance
     */
    @Override
    public StreamMetadataProvider createStreamMetadataProvider(String clientId) {
        IggyStreamConfig iggyConfig = new IggyStreamConfig(this.streamConfig);
        return new IggyStreamMetadataProvider(clientId, iggyConfig);
    }
}
