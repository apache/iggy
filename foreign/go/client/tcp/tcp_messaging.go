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

func (c *IggyTcpClient) SendMessages(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	partitioning iggcon.Partitioning,
	messages []iggcon.IggyMessage,
) error {
	if len(partitioning.Value) > 255 ||
		(partitioning.Kind != iggcon.Balanced && len(partitioning.Value) == 0) {
		return ierror.ErrInvalidKeyValueLength
	}
	if len(messages) == 0 {
		return ierror.ErrInvalidMessagesCount
	}
	_, err := c.do(ctx, &command.SendMessages{
		Compression:  c.MessageCompression,
		StreamId:     streamId,
		TopicId:      topicId,
		Partitioning: partitioning,
		Messages:     messages,
	})
	return err
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
	polled, _, err := c.PollMessagesInto(ctx, streamId, topicId, consumer, strategy, count, autoCommit, partitionId, nil)
	return polled, err
}

// PollMessagesInto polls messages like PollMessages but reads the response
// body into the caller-supplied buf, growing it as needed. The returned buf
// (possibly reallocated) should be passed back on the next call to avoid
// per-RPC allocations once the buffer is large enough.
//
// The Payload and UserHeaders fields of every returned message alias buf;
// copy out any bytes you need before the next PollMessagesInto call.
func (c *IggyTcpClient) PollMessagesInto(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	consumer iggcon.Consumer,
	strategy iggcon.PollingStrategy,
	count uint32,
	autoCommit bool,
	partitionId *uint32,
	buf []byte,
) (*iggcon.PolledMessage, []byte, error) {
	bp := acquireRequestBuf()
	wire, err := encodeWireRequest(*bp, &command.PollMessages{
		StreamId:    streamId,
		TopicId:     topicId,
		Consumer:    consumer,
		AutoCommit:  autoCommit,
		Strategy:    strategy,
		Count:       count,
		PartitionId: partitionId,
	})
	if err != nil {
		releaseRequestBuf(bp)
		return nil, buf, err
	}
	*bp = wire

	buf, err = c.sendWireAndFetchResponseInto(ctx, wire, buf)
	releaseRequestBuf(bp)
	if err != nil {
		return nil, buf, err
	}

	polled, err := binaryserialization.DeserializeFetchMessagesResponse(buf, c.MessageCompression)
	return polled, buf, err
}
