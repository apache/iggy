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
	"log/slog"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/hash"
)

func (c *IggyTcpClient) SendMessages(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	partitioning iggcon.Partitioning,
	messages []iggcon.IggyMessage,
) (*iggcon.SendMessagesResponse, error) {
	if len(partitioning.Value) > 255 ||
		(partitioning.Kind != iggcon.Balanced && len(partitioning.Value) == 0) {
		return nil, ierror.ErrInvalidKeyValueLength
	}
	if len(messages) == 0 {
		return nil, ierror.ErrInvalidMessagesCount
	}

	// Routing needs an explicit partition: the broker never picks one under
	// consensus. Balanced and key-based strategies resolve here against the
	// topic's partition count.
	resolved, err := c.resolvePartitioning(ctx, streamId, topicId, partitioning)
	if err != nil {
		return nil, err
	}

	response, err := c.do(ctx, &command.SendMessages{
		Compression:  c.MessageCompression,
		StreamId:     streamId,
		TopicId:      topicId,
		Partitioning: resolved,
		Messages:     messages,
	})
	if err != nil {
		return nil, err
	}

	confirmations, err := binaryserialization.DeserializeSendMessagesConfirmations(response)
	if err != nil {
		// The batch already committed. Failing here would make a retrying
		// caller write it twice, so the commit is reported without placement.
		c.logger.Warn("Failed to decode the send confirmations", slog.Any("error", err))
		return &iggcon.SendMessagesResponse{}, nil
	}
	return confirmations, nil
}

// resolvePartitioning turns any partitioning strategy into an explicit
// partition id. An explicit strategy passes through untouched.
func (c *IggyTcpClient) resolvePartitioning(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	partitioning iggcon.Partitioning,
) (iggcon.Partitioning, error) {
	if partitioning.Kind == iggcon.PartitionIdKind {
		return partitioning, nil
	}

	key, err := newTopicKey(streamId, topicId)
	if err != nil {
		return iggcon.Partitioning{}, err
	}
	partitionsCount, err := c.topicPartitionsCount(ctx, key, streamId, topicId)
	if err != nil {
		return iggcon.Partitioning{}, err
	}

	switch partitioning.Kind {
	case iggcon.Balanced:
		return iggcon.PartitionId(c.topics.nextBalanced(key, partitionsCount)), nil
	case iggcon.MessageKey:
		return iggcon.PartitionId(hash.XXHash32(partitioning.Value) % partitionsCount), nil
	default:
		return iggcon.Partitioning{}, ierror.ErrInvalidCommand
	}
}

// topicPartitionsCount reads the partition count of a topic, caching it so a
// send does not pay a metadata round trip per batch.
func (c *IggyTcpClient) topicPartitionsCount(
	ctx context.Context,
	key topicKey,
	streamId, topicId iggcon.Identifier,
) (uint32, error) {
	if cached, ok := c.topics.partitionsCount(key); ok {
		return cached, nil
	}

	topic, err := c.GetTopic(ctx, streamId, topicId)
	if err != nil {
		return 0, err
	}
	if topic == nil || topic.PartitionsCount == 0 {
		return 0, ierror.ErrTopicIdNotFound
	}

	c.topics.setPartitionsCount(key, topic.PartitionsCount)
	return topic.PartitionsCount, nil
}

func (c *IggyTcpClient) PollMessages(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	consumer iggcon.Consumer,
	strategy iggcon.PollingStrategy,
	count uint32,
	autoCommit bool,
	partitionId *uint32,
) (*iggcon.PolledMessage, error) {
	// A group poll that names no partition is orchestrated client-side: the
	// member fetches its assignment and polls the partitions it owns in turn.
	if consumer.Kind == iggcon.ConsumerKindGroup && partitionId == nil {
		return c.pollGroup(ctx, streamId, topicId, consumer, strategy, count, autoCommit)
	}
	return c.pollPartition(ctx, streamId, topicId, consumer, strategy, count, autoCommit, partitionId)
}

// pollPartition issues one poll against the partition the caller named.
func (c *IggyTcpClient) pollPartition(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	consumer iggcon.Consumer,
	strategy iggcon.PollingStrategy,
	count uint32,
	autoCommit bool,
	partitionId *uint32,
) (*iggcon.PolledMessage, error) {
	buffer, err := c.do(ctx, &command.PollMessages{
		StreamId:    streamId,
		TopicId:     topicId,
		Consumer:    consumer,
		AutoCommit:  autoCommit,
		Strategy:    strategy,
		Count:       count,
		PartitionId: partitionId,
	})
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeFetchMessagesResponse(buffer, c.MessageCompression)
}
