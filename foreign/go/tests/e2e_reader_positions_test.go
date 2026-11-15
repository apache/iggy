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

package tests_test

import (
	"context"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client/reader"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// readerCount is the count of each read of a reader, so a Last start
	// delivers the last readerCount messages first.
	readerCount = 3
	// batchGap puts the second batch at a later timestamp than the first, so
	// the first message at the timestamp of offset 5 is offset 5.
	batchGap = 20 * time.Millisecond
)

// The server fetches only from an offset or Next, so a reader must find a
// timestamp or Last position with a poll of its partition first. A fake server
// that accepts such fetch targets cannot show that the reader never sends them.
func TestE2E_ReaderResolvesTimestampAndLastPositions(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	streamId, topicId := scratchTopic(t, connected, 1)
	_, err := connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(0), testMessages(t, 5))
	require.NoError(t, err)
	time.Sleep(batchGap)
	_, err = connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(0), testMessages(t, 5))
	require.NoError(t, err)
	polled, err := connected.PollMessages(ctx, streamId, topicId, 0, iggcon.DefaultConsumer(),
		iggcon.OffsetPollingStrategy(0), countOptions(10, false))
	require.NoError(t, err)
	require.Len(t, polled.Messages, 10)
	secondBatch := iggcon.TimestampPollingStrategy(polled.Messages[5].Header.Timestamp)
	require.Less(t, polled.Messages[4].Header.Timestamp, secondBatch.Value, "the second batch is later than the first")

	policies := []struct {
		name   string
		commit iggcon.CommitPolicy
	}{{"OnRead", iggcon.OnReadCommit()}, {"Manual", iggcon.ManualCommit()}}
	for _, policy := range policies {
		t.Run(policy.name, func(t *testing.T) {
			t.Run("TimestampStart", func(t *testing.T) {
				partitionReader := startReader(t, connected, streamId, topicId, caseConsumer(t), secondBatch, policy.commit)
				assert.Equal(t, []uint64{5}, nextOffsets(t, partitionReader, 1))
			})
			t.Run("LastStart", func(t *testing.T) {
				partitionReader := startReader(t, connected, streamId, topicId, caseConsumer(t),
					iggcon.LastPollingStrategy(), policy.commit)
				assert.Equal(t, []uint64{7, 8, 9}, nextOffsets(t, partitionReader, readerCount))
			})
			t.Run("TimestampSeek", func(t *testing.T) {
				partitionReader := startReader(t, connected, streamId, topicId, caseConsumer(t),
					iggcon.NextPollingStrategy(), policy.commit)
				require.Equal(t, []uint64{0}, nextOffsets(t, partitionReader, 1))
				seekCtx, cancel := context.WithTimeout(ctx, longWait)
				err := partitionReader.Seek(seekCtx, 0, secondBatch)
				cancel()
				require.NoError(t, err)
				assert.Equal(t, []uint64{5}, nextOffsets(t, partitionReader, 1))
			})
		})
	}

	// It runs last, because its offset 10 moves the Last start of the cases above.
	t.Run("StoredOffsetWinsOverTheStart", func(t *testing.T) {
		consumer := caseConsumer(t)
		first := startReader(t, connected, streamId, topicId, consumer, secondBatch, iggcon.OnReadCommit())
		require.Equal(t, []uint64{5, 6, 7, 8, 9}, nextOffsets(t, first, 5))
		require.NoError(t, first.Close(ctx))
		partition := uint32(0)
		stored, err := connected.GetConsumerOffset(ctx, consumer, streamId, topicId, &partition)
		require.NoError(t, err)
		require.NotNil(t, stored)
		assert.Equal(t, uint64(9), stored.StoredOffset, "the close keeps the offset of the last delivered message")
		_, err = connected.SendMessages(ctx, streamId, topicId, iggcon.PartitionId(0), testMessages(t, 1))
		require.NoError(t, err)

		second := startReader(t, connected, streamId, topicId, consumer, secondBatch, iggcon.OnReadCommit())
		assert.Equal(t, []uint64{10}, nextOffsets(t, second, 1))
	})
}

// caseConsumer is a standalone consumer named after the running case, so each
// case starts without a stored offset.
func caseConsumer(t *testing.T) iggcon.Consumer {
	t.Helper()
	id, err := iggcon.NewIdentifier(t.Name())
	require.NoError(t, err)
	return iggcon.NewSingleConsumer(id)
}

// startReader starts a reader of partition 0 that reads readerCount messages at a time.
func startReader(
	t *testing.T,
	connected iggcon.Client,
	streamId, topicId iggcon.Identifier,
	consumer iggcon.Consumer,
	start iggcon.PollingStrategy,
	commit iggcon.CommitPolicy,
) *reader.PartitionReader {
	t.Helper()
	options := longWaitOptions(readerCount)
	options.Commit = commit
	partitionReader, err := reader.NewPartitionReader(context.Background(), connected, streamId, topicId, consumer,
		reader.WithPartitions(0), reader.WithStart(start), reader.WithPollOptions(options))
	require.NoError(t, err)
	t.Cleanup(func() { _ = partitionReader.Close(context.Background()) })
	return partitionReader
}

// nextOffsets returns the offsets of the next count messages that the reader delivers.
func nextOffsets(t *testing.T, partitionReader *reader.PartitionReader, count int) []uint64 {
	t.Helper()
	offsets := make([]uint64, 0, count)
	for len(offsets) < count {
		readCtx, cancel := context.WithTimeout(context.Background(), longWait)
		polled, err := partitionReader.Next(readCtx)
		cancel()
		require.NoError(t, err)
		for _, message := range polled.Messages {
			offsets = append(offsets, message.Header.Offset)
		}
	}
	return offsets[:count]
}
