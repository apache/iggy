// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package tcp

import (
	"context"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
)

func (c *IggyTcpClient) GetConsumerOffset(ctx context.Context, consumer iggcon.Consumer, streamId iggcon.Identifier, topicId iggcon.Identifier, partitionId *uint32) (*iggcon.ConsumerOffsetInfo, error) {
	buffer, err := c.do(ctx, &command.GetConsumerOffset{
		StreamId:    streamId,
		TopicId:     topicId,
		Consumer:    consumer,
		PartitionId: partitionId,
	})
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeOffset(buffer), nil
}

func (c *IggyTcpClient) StoreConsumerOffset(ctx context.Context, consumer iggcon.Consumer, streamId iggcon.Identifier, topicId iggcon.Identifier, offset uint64, partitionId *uint32) error {
	target := command.GetConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: partitionId}
	return c.writeOffset(ctx, &target, &command.StoreConsumerOffsetRequest{
		StreamId:    streamId,
		TopicId:     topicId,
		Offset:      offset,
		Consumer:    consumer,
		PartitionId: partitionId,
	})
}

func (c *IggyTcpClient) DeleteConsumerOffset(ctx context.Context, consumer iggcon.Consumer, streamId iggcon.Identifier, topicId iggcon.Identifier, partitionId *uint32) error {
	target := command.GetConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: partitionId}
	return c.writeOffset(ctx, &target, &command.DeleteConsumerOffset{
		Consumer:    consumer,
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: partitionId,
	})
}

// writeOffset sends a clustered offset write to the partition primary on the
// attached data connection. On the coordinator session, a partition refusal
// would walk the roster and register a new client identity, which is not a
// member of the group, so a group commit would be refused and the membership
// lost.
func (c *IggyTcpClient) writeOffset(ctx context.Context, target *command.GetConsumerOffset, write command.Command) error {
	if !c.topologyKnown.Load() {
		if _, err := c.GetClusterMetadata(ctx); err != nil {
			return err
		}
	}
	if !c.clustered.Load() {
		_, err := c.do(ctx, write)
		return err
	}
	routing, err := target.MarshalBinary()
	if err != nil {
		return err
	}
	payload, err := write.MarshalBinary()
	if err != nil {
		return err
	}
	key := routeKey{code: uint32(command.GetOffsetRoutingCode), target: string(routing)}
	_, err = c.sendRouted(ctx, uint32(write.Code()), key, routing, payload)
	return err
}
