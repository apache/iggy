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
	ierror "github.com/apache/iggy/foreign/go/errors"
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
	return c.writeOffset(ctx, &command.StoreConsumerOffsetRequest{
		StreamId:    streamId,
		TopicId:     topicId,
		Offset:      offset,
		Consumer:    consumer,
		PartitionId: partitionId,
	}, nil)
}

func (c *IggyTcpClient) StoreConsumerPosition(ctx context.Context, consumer iggcon.Consumer, streamId iggcon.Identifier, topicId iggcon.Identifier, position iggcon.ConsumerPosition) error {
	return c.writeOffset(ctx, &command.StoreConsumerOffsetRequest{
		StreamId:    streamId,
		TopicId:     topicId,
		Offset:      position.Offset,
		Consumer:    consumer,
		PartitionId: &position.PartitionId,
	}, &position.Context)
}

func (c *IggyTcpClient) DeleteConsumerOffset(ctx context.Context, consumer iggcon.Consumer, streamId iggcon.Identifier, topicId iggcon.Identifier, partitionId *uint32) error {
	return c.writeOffset(ctx, &command.DeleteConsumerOffset{
		Consumer:    consumer,
		StreamId:    streamId,
		TopicId:     topicId,
		PartitionId: partitionId,
	}, nil)
}

// writeOffset sends an offset write to the partition primary over a data
// connection, as the Rust SDK's PollRouter::write_offset does. A replay on the
// coordinator would walk the roster to the primary under a new client identity,
// which is no member of the group. A captured context is stamped on every
// attempt; without one, the write takes the context its route reports.
func (c *IggyTcpClient) writeOffset(ctx context.Context, cmd command.Command, captured *iggcon.PartitionContext) error {
	if ctx == nil {
		return ierror.ErrNilContext
	}
	if err := c.ensureTopology(ctx); err != nil {
		return err
	}
	if !c.clustered.Load() {
		if captured != nil {
			ctx = context.WithValue(ctx, capturedPartitionContext{}, *captured)
		}
		_, err := c.do(ctx, cmd)
		return err
	}
	payload, err := cmd.MarshalBinary()
	if err != nil {
		return err
	}
	routePayload, err := offsetRoutePayload(cmd.Code(), payload)
	if err != nil {
		return err
	}
	_, err = c.sendRouted(ctx, cmd.Code(), routeKey(command.GetOffsetRoutingCode, routePayload),
		command.GetOffsetRoutingCode, routePayload, payload, captured)
	return err
}
