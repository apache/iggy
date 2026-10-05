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
	"encoding/binary"
	"errors"
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

	// Resolve balanced and key-based routing here so the SDK can cache the
	// topic's partition count and retain its own round-robin position.
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
		if errors.Is(err, ierror.ErrPartitionNotFound) {
			// The cached count pointed this send at a partition the server
			// does not have, so the topic was likely recreated smaller.
			// Nothing else invalidates a count another client changed.
			c.topics.invalidate(newTopicKey(streamId, topicId))
		}
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

	key := newTopicKey(streamId, topicId)
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
	if !c.topologyKnown.Load() {
		if _, err := c.GetClusterMetadata(ctx); err != nil {
			return nil, err
		}
	}
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
	request := &command.PollMessages{
		StreamId:    streamId,
		TopicId:     topicId,
		Consumer:    consumer,
		AutoCommit:  autoCommit,
		Strategy:    strategy,
		Count:       count,
		PartitionId: partitionId,
	}
	// Only the partition primary serves a poll, with or without auto-commit.
	var buffer []byte
	var err error
	if c.clustered.Load() {
		buffer, err = c.pollPrimary(ctx, request)
	} else {
		buffer, err = c.do(ctx, request)
	}
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeFetchMessagesResponse(buffer, c.MessageCompression)
}

// capturePartitionContext attaches the partition context the command must
// carry. It also returns the key of the cached route that context came from,
// or an empty key when it came from no route.
func (c *IggyTcpClient) capturePartitionContext(ctx context.Context, cmd command.Command) (context.Context, string, error) {
	if _, captured := ctx.Value(capturedPartitionContext{}).(iggcon.PartitionContext); captured {
		return ctx, "", nil
	}
	var routeCommand command.Command
	routeCode := command.GetPollRoutingCode
	switch request := cmd.(type) {
	case *command.SendMessages:
		if request.Partitioning.Kind != iggcon.PartitionIdKind || len(request.Partitioning.Value) != 4 {
			return ctx, "", ierror.ErrInvalidCommand
		}
		stream, err := request.StreamId.MarshalBinary()
		if err != nil {
			return ctx, "", err
		}
		topic, err := request.TopicId.MarshalBinary()
		if err != nil {
			return ctx, "", err
		}
		key := topicKey{stream: string(stream), topic: string(topic)}
		partition := binary.LittleEndian.Uint32(request.Partitioning.Value)
		captured, cached := c.topics.partitionContext(key, partition)
		if !cached {
			payload := append(append(stream, topic...), request.Partitioning.Value...)
			response, err := c.SendBinaryRequest(ctx, uint32(command.GetSendContextCode), payload)
			if err != nil {
				return ctx, "", err
			}
			if err := captured.UnmarshalBinary(response); err != nil {
				return ctx, "", err
			}
			c.topics.setPartitionContext(key, partition, captured)
		}
		return context.WithValue(ctx, capturedPartitionContext{}, captured), "", nil
	case *command.PollMessages:
		routeCommand = request
	case *command.StoreConsumerOffsetRequest:
		routeCommand = &command.GetConsumerOffset{StreamId: request.StreamId, TopicId: request.TopicId,
			Consumer: request.Consumer, PartitionId: request.PartitionId}
		routeCode = command.GetOffsetRoutingCode
	case *command.DeleteConsumerOffset:
		routeCommand = &command.GetConsumerOffset{StreamId: request.StreamId, TopicId: request.TopicId,
			Consumer: request.Consumer, PartitionId: request.PartitionId}
		routeCode = command.GetOffsetRoutingCode
	default:
		return ctx, "", nil
	}
	payload, err := routeCommand.MarshalBinary()
	if err != nil {
		return ctx, "", err
	}
	keyBytes := payload
	if routeCode == command.GetPollRoutingCode {
		keyBytes = payload[:len(payload)-pollParametersSize]
	}
	key := string([]byte{byte(routeCode)}) + string(keyBytes)
	route, err := c.consumerRoute(ctx, key, payload, routeCode)
	if err != nil {
		return ctx, "", err
	}
	return context.WithValue(ctx, capturedPartitionContext{}, route.context), key, nil
}
