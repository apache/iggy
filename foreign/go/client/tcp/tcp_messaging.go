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
	"fmt"
	"log/slog"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/hash"
)

// SendMessages payload layout a raw request is parsed by.
const (
	sendMetadataLengthSize = 4
	fieldPrefixSize        = 2
	partitionIdSize        = 4
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
			// Nothing else invalidates a count another client changed. The
			// contexts stay: only a refusal of a context drops it.
			c.topics.forgetCount(newTopicKey(streamId, topicId))
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
	if err := c.ensureTopology(ctx); err != nil {
		return nil, err
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
	// Dispatch on the code, not the type, so a raw request captures exactly
	// what its typed counterpart does.
	switch cmd.Code() {
	case command.SendMessagesCode:
		key, partition, err := sendTarget(cmd)
		if err != nil {
			return ctx, "", err
		}
		captured, cached := c.topics.partitionContext(key, partition)
		if !cached {
			payload := binary.LittleEndian.AppendUint32([]byte(key.stream+key.topic), partition)
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
	case command.PollMessagesCode:
		if poll, ok := cmd.(*command.PollMessages); ok && poll.Strategy.Context != nil {
			return context.WithValue(ctx, capturedPartitionContext{}, *poll.Strategy.Context), "", nil
		}
		payload, err := cmd.MarshalBinary()
		if err != nil {
			return ctx, "", err
		}
		if len(payload) <= pollParametersSize {
			return ctx, "", ierror.ErrInvalidCommand
		}
		key := routeKey(command.GetPollRoutingCode, payload[:len(payload)-pollParametersSize])
		return c.captureRouteContext(ctx, key, payload, command.GetPollRoutingCode)
	case command.StoreOffsetCode, command.DeleteConsumerOffsetCode:
		payload, err := cmd.MarshalBinary()
		if err != nil {
			return ctx, "", err
		}
		routePayload, err := offsetRoutePayload(cmd.Code(), payload)
		if err != nil {
			return ctx, "", err
		}
		key := routeKey(command.GetOffsetRoutingCode, routePayload)
		return c.captureRouteContext(ctx, key, routePayload, command.GetOffsetRoutingCode)
	default:
		return ctx, "", nil
	}
}

func (c *IggyTcpClient) captureRouteContext(ctx context.Context, key string, payload []byte, code command.Code) (context.Context, string, error) {
	route, err := c.consumerRoute(ctx, key, payload, code)
	if err != nil {
		return ctx, "", err
	}
	return context.WithValue(ctx, capturedPartitionContext{}, route.context), key, nil
}

// sendTarget reads the topic and the explicit partition of a SendMessages. A
// raw payload is [metadata length u32][stream id][topic id][partitioning],
// each of the last three [kind u8][length u8][value].
func sendTarget(cmd command.Command) (topicKey, uint32, error) {
	switch request := cmd.(type) {
	case *command.SendMessages:
		if request.Partitioning.Kind != iggcon.PartitionIdKind || len(request.Partitioning.Value) != partitionIdSize {
			return topicKey{}, 0, ierror.ErrInvalidCommand
		}
		stream, err := request.StreamId.MarshalBinary()
		if err != nil {
			return topicKey{}, 0, err
		}
		topic, err := request.TopicId.MarshalBinary()
		if err != nil {
			return topicKey{}, 0, err
		}
		return topicKey{stream: string(stream), topic: string(topic)},
			binary.LittleEndian.Uint32(request.Partitioning.Value), nil
	case rawRequest:
		var fields [3][]byte
		cursor := sendMetadataLengthSize
		for index := range fields {
			if len(request.payload) < cursor+fieldPrefixSize {
				return topicKey{}, 0, ierror.ErrInvalidCommand
			}
			end := cursor + fieldPrefixSize + int(request.payload[cursor+1])
			if len(request.payload) < end {
				return topicKey{}, 0, ierror.ErrInvalidCommand
			}
			fields[index] = request.payload[cursor:end]
			cursor = end
		}
		partitioning := fields[2]
		switch iggcon.PartitioningKind(partitioning[0]) {
		case iggcon.PartitionIdKind:
			if len(partitioning) != fieldPrefixSize+partitionIdSize {
				return topicKey{}, 0, ierror.ErrInvalidCommand
			}
			return topicKey{stream: string(fields[0]), topic: string(fields[1])},
				binary.LittleEndian.Uint32(partitioning[fieldPrefixSize:]), nil
		case iggcon.Balanced, iggcon.MessageKey:
			// A raw send cannot pick the partition, and a zero context is
			// always refused, so it fails before anything is written.
			return topicKey{}, 0, fmt.Errorf("%w: a raw SendMessages needs an explicit partition id; "+
				"use SendMessages for balanced or key partitioning", ierror.ErrFeatureUnavailable)
		default:
			return topicKey{}, 0, ierror.ErrInvalidCommand
		}
	default:
		return topicKey{}, 0, ierror.ErrInvalidCommand
	}
}

// offsetRoutePayload strips the offset and the ack level an offset write ends
// with, which leaves the GetConsumerOffset encoding its route is asked by.
func offsetRoutePayload(code command.Code, payload []byte) ([]byte, error) {
	suffix := deleteOffsetSuffixSize
	if code == command.StoreOffsetCode {
		suffix = storeOffsetSuffixSize
	}
	if len(payload) <= suffix {
		return nil, ierror.ErrInvalidCommand
	}
	return payload[:len(payload)-suffix], nil
}

func routeKey(code command.Code, prefix []byte) string {
	return string([]byte{byte(code)}) + string(prefix)
}
