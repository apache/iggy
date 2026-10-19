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
	"time"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/hash"
)

// SendMessages payload layout a raw request is parsed by.
const (
	sendMetadataLengthSize = 4
	sendMessagesCountSize  = 4
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
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := operationContext(ctx)
	defer cancel()
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

	request := &command.SendMessages{
		Compression: c.MessageCompression, StreamId: streamId, TopicId: topicId,
		Partitioning: resolved, Messages: messages,
	}
	payload, err := request.MarshalBinary()
	if err != nil {
		return nil, err
	}
	partitionOffset := sendMetadataLengthSize + int(binary.LittleEndian.Uint32(payload)) - sendMessagesCountSize - partitionIdSize
	var response []byte
	for attempt := 0; ; attempt++ {
		response, err = c.do(ctx, rawRequest{code: command.SendMessagesCode, payload: payload})
		if err == nil {
			break
		}
		if !errors.Is(err, ierror.ErrPartitionNotFound) {
			return nil, err
		}
		key := newTopicKey(streamId, topicId)
		c.topics.forgetCount(key)
		if attempt != 0 || partitioning.Kind == iggcon.PartitionIdKind {
			return nil, err
		}
		count, refreshErr := c.topicPartitionsCount(ctx, key)
		if refreshErr != nil {
			return nil, refreshErr
		}
		partition := binary.LittleEndian.Uint32(resolved.Value) % count
		if partitioning.Kind == iggcon.MessageKey {
			partition = hash.XXHash32(partitioning.Value) % count
		}
		binary.LittleEndian.PutUint32(payload[partitionOffset:], partition)
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
	partitionsCount, err := c.topicPartitionsCount(ctx, key)
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
) (uint32, error) {
	if cached, ok := c.topics.partitionsCount(key); ok {
		return cached, nil
	}

	query := binary.LittleEndian.AppendUint32([]byte(key.stream+key.topic), 0)
	ctx, cancel := operationContext(ctx)
	defer cancel()
	c.polls.dropRoute(string(query))
	for {
		_, err := c.consumerRoute(ctx, query)
		if err == nil {
			break
		}
		if !errors.Is(err, ierror.ErrTransientNotAccepted) {
			return 0, err
		}
		deadline, _ := ctx.Deadline()
		if time.Until(deadline) <= replayInterval {
			return 0, err
		}
		if err := c.waitBeforeReplay(ctx, deadline); err != nil {
			return 0, err
		}
	}
	count, _ := c.topics.partitionsCount(key)
	if count == 0 {
		return 0, ierror.ErrTopicIdNotFound
	}
	return count, nil
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
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := operationContext(ctx)
	defer cancel()
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
	buffer, err := c.do(ctx, request)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeFetchMessagesResponse(buffer, c.MessageCompression)
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

func isPartitionCommand(code command.Code) bool {
	switch code {
	case command.SendMessagesCode, command.PollMessagesCode, command.GetOffsetCode, command.StoreOffsetCode, command.DeleteConsumerOffsetCode:
		return true
	default:
		return false
	}
}

// partitionQuery validates the routing target before any network request. The
// returned encoding is also the cache key, shared by sends, polls and offsets.
func partitionQuery(code command.Code, payload []byte) ([]byte, error) {
	if code == command.SendMessagesCode {
		key, partition, err := sendTarget(rawRequest{code: code, payload: payload})
		if err != nil {
			return nil, err
		}
		return binary.LittleEndian.AppendUint32([]byte(key.stream+key.topic), partition), nil
	}
	if len(payload) < 1 {
		return nil, ierror.ErrInvalidCommand
	}
	var fields [3][]byte
	position := 1
	for index := range fields {
		if len(payload)-position < fieldPrefixSize {
			return nil, ierror.ErrInvalidCommand
		}
		end := position + fieldPrefixSize + int(payload[position+1])
		if end > len(payload) {
			return nil, ierror.ErrInvalidCommand
		}
		fields[index] = payload[position:end]
		position = end
	}
	suffix := 0
	switch code {
	case command.PollMessagesCode:
		suffix = pollParametersSize
	case command.StoreOffsetCode:
		suffix = storeOffsetSuffixSize
	case command.DeleteConsumerOffsetCode:
		suffix = deleteOffsetSuffixSize
	}
	if len(payload)-position != 1+partitionIdSize+suffix || payload[position] > 1 {
		return nil, ierror.ErrInvalidCommand
	}
	partition := binary.LittleEndian.Uint32(payload[position+1:])
	if payload[position] == 0 {
		if code == command.StoreOffsetCode || code == command.DeleteConsumerOffsetCode ||
			(code == command.PollMessagesCode && payload[0] == byte(iggcon.ConsumerKindGroup)) {
			return nil, ierror.ErrInvalidCommand
		}
		partition = 0
	}
	query := append(append(make([]byte, 0, len(fields[1])+len(fields[2])+partitionIdSize), fields[1]...), fields[2]...)
	return binary.LittleEndian.AppendUint32(query, partition), nil
}
