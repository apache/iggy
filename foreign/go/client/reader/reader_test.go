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

package reader

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/batch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTimeout       = 5 * time.Second
	testRetryInterval = 100 * time.Millisecond
)

var (
	testContext    = iggcon.PartitionContext{Incarnation: 7, OwnerGeneration: 1, MetadataOp: 1}
	errStoreFailed = errors.New("the store failed")
)

type fakeRead struct {
	targets []iggcon.FetchMessagesTarget
	count   uint32
	commit  bool
	reply   chan fakeReply
}

type fakeReply struct {
	sections []iggcon.FetchMessagesSection
	err      error
}

type store struct {
	partition uint32
	offset    uint64
}

// fakeClient answers the reads of a reader one by one and records its stores.
type fakeClient struct {
	iggcon.Client
	mu          sync.Mutex
	reads       chan *fakeRead
	stores      []store
	abandoned   []uint64
	membership  []string
	assignments chan []uint32
	grouper     func([]uint32) [][]uint32
	// failedGroupings is the number of the next groupings that fail.
	failedGroupings int
	groupings       []time.Time
	starts          map[uint32]uint64
	deleted         []uint32
	storeStarted    chan context.Context
	storeRelease    chan struct{}
	// failedStores is the number of the next stores that fail.
	failedStores int
	// holdsCommits makes a committing read wait for its reply after its
	// caller stops waiting, as the client does.
	holdsCommits bool
	leaveErr     error
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		reads:       make(chan *fakeRead, 16),
		starts:      make(map[uint32]uint64),
		assignments: make(chan []uint32, 4),
		grouper:     func(partitions []uint32) [][]uint32 { return [][]uint32{partitions} },
	}
}

func (f *fakeClient) FetchMessages(ctx context.Context, _, _ iggcon.Identifier, _ iggcon.Consumer,
	targets []iggcon.FetchMessagesTarget, count uint32, autoCommit bool, options iggcon.DeferredPollOptions,
) ([]iggcon.FetchMessagesSection, error) {
	// The client refuses before the send what the server refuses.
	if err := options.ValidateFetch(count); err != nil {
		return nil, err
	}
	if !autoCommit && count == 1 && len(targets) == 1 && targets[0].Strategy.Kind == iggcon.POLLING_NEXT {
		return []iggcon.FetchMessagesSection{section(targets[0].PartitionId, f.starts[targets[0].PartitionId])}, nil
	}
	read := &fakeRead{targets: targets, count: count, commit: autoCommit, reply: make(chan fakeReply, 1)}
	f.reads <- read
	if autoCommit && f.holdsCommits {
		reply := <-read.reply
		return reply.sections, reply.err
	}
	select {
	case reply := <-read.reply:
		return reply.sections, reply.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeClient) ReadinessGroups(_ context.Context, _, _ iggcon.Identifier, partitions []uint32) ([][]uint32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.groupings = append(f.groupings, time.Now())
	if f.failedGroupings > 0 {
		f.failedGroupings--
		return nil, errors.New("no route to the partition yet")
	}
	return f.grouper(partitions), nil
}

func (f *fakeClient) StoreConsumerPosition(ctx context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier, position iggcon.ConsumerPosition) error {
	if f.storeStarted != nil {
		f.storeStarted <- ctx
		select {
		case <-f.storeRelease:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failedStores > 0 {
		f.failedStores--
		return errStoreFailed
	}
	f.stores = append(f.stores, store{partition: position.PartitionId, offset: position.Offset})
	return nil
}

func (f *fakeClient) DeleteConsumerPosition(_ context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier, position iggcon.ConsumerPosition) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, position.PartitionId)
	return nil
}

func (f *fakeClient) AbandonOperation(_ context.Context, operationID uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.abandoned = append(f.abandoned, operationID)
	return nil
}

func (f *fakeClient) JoinConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.membership = append(f.membership, "join")
	return nil
}

func (f *fakeClient) LeaveConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.membership = append(f.membership, "leave")
	return f.leaveErr
}

func (f *fakeClient) SyncConsumerGroup(ctx context.Context, _, _ iggcon.Identifier, _ iggcon.Identifier) (*iggcon.ConsumerGroupAssignment, error) {
	select {
	case partitions := <-f.assignments:
		return &iggcon.ConsumerGroupAssignment{Generation: 1, Partitions: partitions}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeClient) nextRead(t *testing.T) *fakeRead {
	t.Helper()
	select {
	case read := <-f.reads:
		return read
	case <-time.After(testTimeout):
		t.Fatal("the reader sent no read")
		return nil
	}
}

func (f *fakeClient) readsNothingFor(wait time.Duration) bool {
	select {
	case read := <-f.reads:
		f.reads <- read
		return false
	case <-time.After(wait):
		return true
	}
}

func (f *fakeClient) recordedStores() []store {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store(nil), f.stores...)
}

func (f *fakeClient) recordedAbandons() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.abandoned...)
}

func (f *fakeClient) recordedGroupings() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.groupings...)
}

func (r *fakeRead) answer(sections ...iggcon.FetchMessagesSection) {
	r.reply <- fakeReply{sections: sections}
}

// section is an OK section of partition with one message at each offset, the last one of the partition.
func section(partition uint32, offsets ...uint64) iggcon.FetchMessagesSection {
	var records []byte
	for _, offset := range offsets {
		records = append(records, record(offset)...)
	}
	next := offsets[len(offsets)-1] + 1
	return iggcon.FetchMessagesSection{PartitionId: partition, CurrentOffset: next - 1, MessageCount: uint32(len(offsets)),
		Context: testContext, NextOffset: next, Records: records}
}

// behind is an empty section of a partition whose read starts at offset 0
// while the partition holds messages up to head, as a fetch that reached its
// count before the partition returns.
func behind(partition uint32, head uint64) iggcon.FetchMessagesSection {
	return iggcon.FetchMessagesSection{PartitionId: partition, CurrentOffset: head, Context: testContext}
}

// ahead is an OK section with one message at each offset of a partition whose
// head is at head, past the last of them, so the partition is backlogged.
func ahead(partition uint32, head uint64, offsets ...uint64) iggcon.FetchMessagesSection {
	backlogged := section(partition, offsets...)
	backlogged.CurrentOffset = head
	return backlogged
}

func partitionsOf(read *fakeRead) []uint32 {
	partitions := make([]uint32, 0, len(read.targets))
	for _, target := range read.targets {
		partitions = append(partitions, target.PartitionId)
	}
	slices.Sort(partitions)
	return partitions
}

// record is one batch record with one message at offset.
func record(offset uint64) []byte {
	return batchRecord(offset, frame(0, []byte(fmt.Sprintf("message %d", offset)), 0))
}

// undecodable is one batch record at offset whose message claims more payload than the record holds.
func undecodable(offset uint64) []byte {
	message := frame(0, []byte("message"), 0)
	binary.LittleEndian.PutUint32(message[36:], 1024)
	return batchRecord(offset, message)
}

// sectionOf is an OK section of partition that holds records of count messages up to next.
func sectionOf(partition uint32, next uint64, count uint32, records ...[]byte) iggcon.FetchMessagesSection {
	return iggcon.FetchMessagesSection{PartitionId: partition, CurrentOffset: next - 1, MessageCount: count,
		Context: testContext, NextOffset: next, Records: slices.Concat(records...)}
}

// batchRecord is one batch record based at baseOffset holding frames.
func batchRecord(baseOffset uint64, frames ...[]byte) []byte {
	blob := slices.Concat(frames...)
	header := make([]byte, batch.HeaderSize)
	binary.LittleEndian.PutUint64(header[8:], baseOffset)
	binary.LittleEndian.PutUint64(header[32:], uint64(batch.HeaderSize+len(blob)))
	binary.LittleEndian.PutUint32(header[48:], uint32(len(frames)))
	return append(header, blob...)
}

// frame is one message frame at offsetDelta of its batch, with reserved in the reserved field.
func frame(offsetDelta uint32, payload []byte, reserved uint64) []byte {
	message := make([]byte, batch.MessageHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(message[24:], offsetDelta)
	binary.LittleEndian.PutUint32(message[36:], uint32(len(payload)))
	binary.LittleEndian.PutUint64(message[40:], reserved)
	copy(message[batch.MessageHeaderSize:], payload)
	return message
}

func identifier(t *testing.T, value uint32) iggcon.Identifier {
	t.Helper()
	id, err := iggcon.NewIdentifier(value)
	require.NoError(t, err)
	return id
}

func standalone(t *testing.T, client *fakeClient, options iggcon.PollOptions, partitions ...uint32) *PartitionReader {
	t.Helper()
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(partitions...), WithPollOptions(options),
		WithRetryInterval(testRetryInterval))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	return reader
}

// member is a group reader whose first refresh reads assigned.
func member(t *testing.T, client *fakeClient, options iggcon.PollOptions, assigned ...uint32) *PartitionReader {
	t.Helper()
	group, err := iggcon.NewIdentifier(uint32(9))
	require.NoError(t, err)
	client.assignments <- assigned
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewGroupConsumer(group), WithPollOptions(options), WithRetryInterval(testRetryInterval))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	return reader
}

// assign hands assigned to the next refresh of reader and waits until the reader follows it.
func assign(t *testing.T, client *fakeClient, reader *PartitionReader, assigned ...uint32) {
	t.Helper()
	client.assignments <- assigned
	require.Eventually(t, func() bool { return slices.Equal(reader.Partitions(), assigned) }, testTimeout, 5*time.Millisecond)
}

// nextInBackground starts a Next whose outcome arrives on the returned channel.
func nextInBackground(reader *PartitionReader) <-chan outcome {
	polled := make(chan outcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		batch, err := reader.Next(ctx)
		polled <- outcome{batch, err}
	}()
	return polled
}

type outcome struct {
	polled *iggcon.PolledMessage
	err    error
}

func queued(reader *PartitionReader) int {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return len(reader.queue)
}

func waiting(reader *PartitionReader) int {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.waiting
}

func next(t *testing.T, reader *PartitionReader) *iggcon.PolledMessage {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	polled, err := reader.Next(ctx)
	require.NoError(t, err)
	return polled
}

func offsets(polled *iggcon.PolledMessage) []uint64 {
	values := make([]uint64, 0, len(polled.Messages))
	for _, message := range polled.Messages {
		values = append(values, message.Header.Offset)
	}
	return values
}

var waitOneSecond = iggcon.WaitPollOptions(time.Second)

func withCommit(options iggcon.PollOptions, commit iggcon.CommitPolicy) iggcon.PollOptions {
	options.Commit = commit
	return options
}

func TestPartitionReader_ReadsEachGroupWithOneRequestAndContinuesAfterItsLastMessage(t *testing.T) {
	client := newFakeClient()
	client.grouper = func([]uint32) [][]uint32 { return [][]uint32{{1, 3}, {2}} }
	reader := standalone(t, client, waitOneSecond, 1, 2, 3)
	first, second := client.nextRead(t), client.nextRead(t)
	groupOfTwo, groupOfOneAndThree := first, second
	if first.targets[0].PartitionId != 2 {
		groupOfTwo, groupOfOneAndThree = second, first
	}
	require.Len(t, groupOfOneAndThree.targets, 2)

	groupOfTwo.answer(section(2, 5, 6))
	assert.Equal(t, []uint64{5, 6}, offsets(next(t, reader)))
	groupOfOneAndThree.answer(section(3, 0), section(1, 9))
	assert.Equal(t, []uint64{0}, offsets(next(t, reader)))
	assert.Equal(t, []uint64{9}, offsets(next(t, reader)))

	again := client.nextRead(t)
	if again.targets[0].PartitionId != 2 {
		again = client.nextRead(t)
	}
	partition, bound := again.targets[0].Strategy.Partition()
	assert.True(t, bound)
	assert.Equal(t, uint32(2), partition)
	assert.Equal(t, uint64(7), again.targets[0].Strategy.Value, "a partition continues after its last message")
}

// Under OnRead a read stores its offset, so the reader reads only for a waiting
// Next. After a lost reply the next read starts after what the lost read
// stored, and the reader reads those messages again without a store.
func TestPartitionReader_ReadsOnDemandUnderOnReadAndReadsAgainWhatALostReplyStored(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	assert.True(t, client.readsNothingFor(300*time.Millisecond))

	type outcome struct {
		polled *iggcon.PolledMessage
		err    error
	}
	polled := make(chan outcome, 1)
	go func() {
		batch, err := reader.Next(context.Background())
		polled <- outcome{batch, err}
	}()
	read := client.nextRead(t)
	assert.True(t, read.commit)
	read.answer(section(1, 0, 1))
	assert.Equal(t, []uint64{0, 1}, offsets((<-polled).polled))

	go func() {
		batch, err := reader.Next(context.Background())
		polled <- outcome{batch, err}
	}()
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	afterLoss := client.nextRead(t)
	assert.Equal(t, iggcon.POLLING_NEXT, afterLoss.targets[0].Strategy.Kind)
	afterLoss.answer(section(1, 5, 6))
	gap := client.nextRead(t)
	assert.False(t, gap.commit)
	assert.Equal(t, iggcon.POLLING_OFFSET, gap.targets[0].Strategy.Kind)
	assert.Equal(t, uint64(2), gap.targets[0].Strategy.Value)
	assert.Equal(t, uint32(3), gap.count)
	gap.answer(section(1, 2, 3, 4, 5))

	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{2, 3, 4}, offsets(result.polled))
	assert.Equal(t, []uint64{5, 6}, offsets(next(t, reader)))
	client.mu.Lock()
	assert.Equal(t, []uint64{42}, client.abandoned)
	client.mu.Unlock()
}

func TestPartitionReader_CommitsADeliveredCheckpointUnderManualAndRefusesOneUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 3, 4))
	batch := next(t, reader)
	checkpoint, _ := batch.CheckpointAt(0)

	require.NoError(t, reader.Commit(context.Background(), checkpoint))
	require.NoError(t, reader.Commit(context.Background(), checkpoint))
	assert.Equal(t, []store{{1, 3}}, client.recordedStores())

	onReadClient := newFakeClient()
	onReadClient.starts[1] = 3
	onRead := standalone(t, onReadClient, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	go func() { onReadClient.nextRead(t).answer(section(1, 3)) }()
	delivered, _ := next(t, onRead).Checkpoint()
	require.ErrorIs(t, onRead.Commit(context.Background(), delivered), ierror.ErrInvalidCommand)
	require.ErrorIs(t, onRead.Mark(delivered), ierror.ErrInvalidCommand)
}

// Under OnRead the server stored what the reader holds undelivered, so Close moves the offset back.
func TestPartitionReader_MovesTheOffsetOfUndeliveredBatchesBackOnCloseUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.starts[2] = 4
	client.grouper = func([]uint32) [][]uint32 { return [][]uint32{{1}, {2}} }
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	polled := make(chan *iggcon.PolledMessage, 1)
	go func() {
		batch, _ := reader.Next(context.Background())
		polled <- batch
	}()
	first, second := client.nextRead(t), client.nextRead(t)
	partitionOne, partitionTwo := first, second
	if first.targets[0].PartitionId != 1 {
		partitionOne, partitionTwo = second, first
	}
	partitionOne.answer(section(1, 0, 1))
	assert.Equal(t, []uint64{0, 1}, offsets(<-polled))
	partitionTwo.answer(section(2, 4, 5))
	require.Eventually(t, func() bool {
		reader.mu.Lock()
		defer reader.mu.Unlock()
		return len(reader.queue) == 1
	}, testTimeout, 5*time.Millisecond)

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{2, 3}}, client.recordedStores(), "partition 2 started at its next offset and delivered nothing")
}

func TestPartitionReader_StopsAPartitionWhoseHistoryIsGoneUntilASeek(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 0))
	next(t, reader)
	client.nextRead(t).answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.ErrHistoryUnavailable})

	_, err := reader.Next(context.Background())
	var readError *ReadError
	require.ErrorAs(t, err, &readError)
	require.ErrorIs(t, err, ierror.ErrHistoryUnavailable)
	assert.Equal(t, uint32(1), *readError.PartitionID)
	assert.True(t, client.readsNothingFor(300*time.Millisecond))

	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.FirstPollingStrategy()))
	assert.Equal(t, uint64(0), client.nextRead(t).targets[0].Strategy.Value)
}

func TestPartitionReader_RefusesAContinuationOfAnotherPartition(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, waitOneSecond, 1, 2)
	client.nextRead(t).answer(section(1, 0))
	continuation, _ := next(t, reader).Continuation()

	require.ErrorIs(t, reader.Seek(context.Background(), 2, continuation), ierror.ErrInvalidCommand)
	require.ErrorIs(t, reader.Seek(context.Background(), 3, iggcon.FirstPollingStrategy()), ierror.ErrInvalidCommand)
}

// A rebalance takes a partition away only after its counted position is stored.
func TestPartitionReader_FollowsTheAssignmentAndStoresTheCountedPositionOfAPartitionThatLeaves(t *testing.T) {
	client := newFakeClient()
	client.grouper = func(partitions []uint32) [][]uint32 {
		groups := make([][]uint32, 0, len(partitions))
		for _, partition := range partitions {
			groups = append(groups, []uint32{partition})
		}
		return groups
	}
	auto := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessages, Messages: 1000}, After: iggcon.CommitAfterDelivery}
	group, err := iggcon.NewIdentifier(uint32(9))
	require.NoError(t, err)
	client.assignments <- []uint32{1, 2}
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewGroupConsumer(group), WithPollOptions(withCommit(waitOneSecond, iggcon.AutoCommitPolicy(auto))),
		WithRetryInterval(100*time.Millisecond))
	require.NoError(t, err)
	first, second := client.nextRead(t), client.nextRead(t)
	partitionOne := first
	if first.targets[0].PartitionId != 1 {
		partitionOne = second
	}
	partitionOne.answer(section(1, 7))
	next(t, reader)
	assert.Equal(t, []uint32{1, 2}, reader.Partitions())

	client.assignments <- []uint32{2}

	require.Eventually(t, func() bool { return len(client.recordedStores()) == 1 }, testTimeout, 5*time.Millisecond)
	assert.Equal(t, []store{{1, 7}}, client.recordedStores())
	assert.Equal(t, []uint32{2}, reader.Partitions())
	client.assignments <- []uint32{2}
	require.NoError(t, reader.Close(context.Background()))
	assert.Equal(t, []string{"join", "leave"}, client.membership)
}

// Under OnRead the server stored the batches that the reader holds, so a
// partition that leaves the assignment still hands them over.
func TestPartitionReader_HandsOverAQueuedBatchOfAPartitionThatLeavesUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.starts[2] = 4
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1), section(2, 4, 5))
	assert.Equal(t, []uint64{0, 1}, offsets((<-polled).polled))

	assign(t, client, reader, 1)

	require.Equal(t, 1, queued(reader), "the queued batch of the partition that left stays")
	batch := next(t, reader)
	assert.Equal(t, uint32(2), batch.PartitionId)
	assert.Equal(t, []uint64{4, 5}, offsets(batch))
}

// Under OnRead the server stored a read in flight, so its reply reaches the
// application after its partition left the assignment.
func TestPartitionReader_HandsOverAReadThatEndsAfterItsPartitionLeftUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.starts[2] = 4
	client.grouper = func(partitions []uint32) [][]uint32 {
		groups := make([][]uint32, 0, len(partitions))
		for _, partition := range partitions {
			groups = append(groups, []uint32{partition})
		}
		return groups
	}
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	polled := nextInBackground(reader)
	first, second := client.nextRead(t), client.nextRead(t)
	partitionOne, partitionTwo := first, second
	if first.targets[0].PartitionId != 1 {
		partitionOne, partitionTwo = second, first
	}
	partitionOne.answer(section(1, 0, 1))
	assert.Equal(t, []uint64{0, 1}, offsets((<-polled).polled))

	assign(t, client, reader, 1)
	partitionTwo.answer(section(2, 4, 5))

	require.Eventually(t, func() bool { return queued(reader) == 1 }, testTimeout, 5*time.Millisecond,
		"the reply of the partition that left is queued")
	batch := next(t, reader)
	assert.Equal(t, uint32(2), batch.PartitionId)
	assert.Equal(t, []uint64{4, 5}, offsets(batch))
}

// Under OnRead a partition that leaves while it reads again what a lost read
// stored reports those messages as lost and still hands over the batch that
// the server stored after them.
func TestPartitionReader_HandsOverTheBatchAfterALostReadWhenItsPartitionLeavesUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	polled = nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	client.nextRead(t).answer(section(1, 5, 6))
	assert.False(t, client.nextRead(t).commit)

	assign(t, client, reader, 2)

	select {
	case result := <-polled:
		var lost ierror.FetchProgressUncertain
		require.ErrorAs(t, result.err, &lost)
	case <-time.After(testTimeout):
		t.Fatal("the reader reported no loss for the partition that left")
	}
	assert.Equal(t, []uint64{5, 6}, offsets(next(t, reader)))
}

// A failed grouping waits the retry interval, however often the application
// asks for a batch.
func TestPartitionReader_WaitsTheRetryIntervalBeforeItGroupsAgainAfterAFailedGrouping(t *testing.T) {
	client := newFakeClient()
	client.failedGroupings = 2
	reader := standalone(t, client, waitOneSecond, 1)
	for range 3 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		_, _ = reader.Next(ctx)
		cancel()
	}

	read := client.nextRead(t)

	groupings := client.recordedGroupings()
	require.Len(t, groupings, 3)
	assert.GreaterOrEqual(t, groupings[1].Sub(groupings[0]), testRetryInterval)
	assert.GreaterOrEqual(t, groupings[2].Sub(groupings[1]), testRetryInterval)
	read.answer(section(1, 0))
	assert.Equal(t, []uint64{0}, offsets(next(t, reader)))
}

// A fetch that reaches its count serves its first targets, so the partitions
// served last go last in the next fetch and each partition gets its turn.
func TestPartitionReader_PutsThePartitionsServedLastAtTheEndOfTheNextFetch(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, waitOneSecond, 1, 3, 5)
	var delivered []uint32
	for range 3 {
		read := client.nextRead(t)
		first := read.targets[0]
		read.answer(section(first.PartitionId, first.Strategy.Value))
		delivered = append(delivered, next(t, reader).PartitionId)
	}

	assert.Equal(t, []uint32{1, 3, 5}, delivered)
}

func TestPartitionReader_HandsOverAQueuedBatchOfAPartitionAssignedAgainUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.starts[2] = 4
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1), section(2, 4, 5))
	assert.Equal(t, []uint64{0, 1}, offsets((<-polled).polled))

	assign(t, client, reader, 1)
	assign(t, client, reader, 1, 2)

	require.Equal(t, 1, queued(reader), "the queued batch of the partition that left stays")
	batch := next(t, reader)
	assert.Equal(t, uint32(2), batch.PartitionId)
	assert.Equal(t, []uint64{4, 5}, offsets(batch))
}

func TestPartitionReader_HandsOverAReadThatEndsAfterItsPartitionIsAssignedAgainUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.starts[2] = 4
	client.grouper = func(partitions []uint32) [][]uint32 {
		groups := make([][]uint32, 0, len(partitions))
		for _, partition := range partitions {
			groups = append(groups, []uint32{partition})
		}
		return groups
	}
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	polled := nextInBackground(reader)
	first, second := client.nextRead(t), client.nextRead(t)
	partitionOne, partitionTwo := first, second
	if first.targets[0].PartitionId != 1 {
		partitionOne, partitionTwo = second, first
	}
	partitionOne.answer(section(1, 0, 1))
	assert.Equal(t, []uint64{0, 1}, offsets((<-polled).polled))

	assign(t, client, reader, 1)
	assign(t, client, reader, 1, 2)
	partitionTwo.answer(section(2, 4, 5))

	require.Eventually(t, func() bool { return queued(reader) == 1 }, testTimeout, 5*time.Millisecond,
		"the reply of the partition that left is queued")
	batch := next(t, reader)
	assert.Equal(t, uint32(2), batch.PartitionId)
	assert.Equal(t, []uint64{4, 5}, offsets(batch))
}

func TestPartitionReader_WaitsTheRetryIntervalAfterASectionlessReplyWithZeroWait(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, iggcon.DefaultPollOptions(), 1)
	read := client.nextRead(t)
	started := time.Now()
	read.answer()
	client.nextRead(t)
	assert.GreaterOrEqual(t, time.Since(started), testRetryInterval)
	assert.Zero(t, queued(reader))
}

func TestPartitionReader_DropsAnOldStoredReplyAfterASeekAndReassignment(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	ctx, cancel := context.WithCancel(context.Background())
	polled := make(chan error, 1)
	go func() {
		_, err := reader.Next(ctx)
		polled <- err
	}()
	old := client.nextRead(t)
	cancel()
	require.ErrorIs(t, <-polled, context.Canceled)
	assign(t, client, reader, 2)
	assign(t, client, reader, 1)
	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(10)))
	client.starts[1] = 10
	assign(t, client, reader, 2)
	assign(t, client, reader, 1)
	old.answer(section(1, 0, 1))
	resumed := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 10))
	result := <-resumed
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{10}, offsets(result.polled))
}

func TestPartitionReader_RecoversTheFirstLostOnReadReply(t *testing.T) {
	for _, start := range []uint64{0, 5} {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			client := newFakeClient()
			client.starts[1] = start
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			polled := nextInBackground(reader)
			first := client.nextRead(t)
			require.True(t, first.commit)
			first.reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
			client.nextRead(t).answer(section(1, start+2))
			gap := client.nextRead(t)
			require.False(t, gap.commit)
			require.Equal(t, start, gap.targets[0].Strategy.Value)
			gap.answer(section(1, start, start+1))
			result := <-polled
			require.NoError(t, result.err)
			assert.Equal(t, []uint64{start, start + 1}, offsets(result.polled))
			assert.Equal(t, []uint64{start + 2}, offsets(next(t, reader)))
		})
	}
}

func TestPartitionReader_RewindsTheFirstUndeliveredOnReadToItsBaseline(t *testing.T) {
	for _, start := range []uint64{0, 5} {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			client := newFakeClient()
			client.starts[1] = start
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			polled := nextInBackground(reader)
			client.nextRead(t)
			require.NoError(t, reader.Close(context.Background()))
			require.Error(t, (<-polled).err)
			if start == 0 {
				assert.Equal(t, []uint32{1}, client.deleted)
			} else {
				assert.Equal(t, []store{{1, start - 1}}, client.recordedStores())
			}
		})
	}
}

func TestPartitionReader_RecoversTheFirstLostOnReadReplyWhenItConsumedTheWholeBacklog(t *testing.T) {
	for _, start := range []uint64{0, 5} {
		t.Run(fmt.Sprint(start), func(t *testing.T) {
			client := newFakeClient()
			client.starts[1] = start
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			polled := nextInBackground(reader)
			client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
			client.nextRead(t).answer(iggcon.FetchMessagesSection{PartitionId: 1, Context: testContext,
				CurrentOffset: start + 2, NextOffset: start + 2})
			gap := client.nextRead(t)
			require.False(t, gap.commit)
			require.Equal(t, start, gap.targets[0].Strategy.Value)
			gap.answer(section(1, start, start+1))
			result := <-polled
			require.NoError(t, result.err)
			assert.Equal(t, []uint64{start, start + 1}, offsets(result.polled))
		})
	}
}

// A read again of what a lost reply stored that the server refuses is sent
// again once its retry is due, so the partition does not stall.
func TestPartitionReader_SendsAgainARefusedReadOfWhatALostReplyStored(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	polled = nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	client.nextRead(t).answer(section(1, 5, 6))
	refused := client.nextRead(t)
	require.False(t, refused.commit)
	refused.answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.ErrDeferredPollCapacityExceeded})

	again := client.nextRead(t)

	assert.False(t, again.commit)
	assert.Equal(t, uint64(2), again.targets[0].Strategy.Value)
	again.answer(section(1, 2, 3, 4))
	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{2, 3, 4}, offsets(result.polled))
	assert.Equal(t, []uint64{5, 6}, offsets(next(t, reader)))
}

// A read again of what a lost reply stored that returns nothing is sent again
// after the retry interval, so the partition does not stall.
func TestPartitionReader_SendsAgainAnEmptyReadOfWhatALostReplyStored(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	polled = nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	client.nextRead(t).answer(section(1, 5, 6))
	empty := client.nextRead(t)
	require.False(t, empty.commit)
	empty.answer(iggcon.FetchMessagesSection{PartitionId: 1, Context: testContext, CurrentOffset: 7, NextOffset: 2})

	again := client.nextRead(t)

	assert.False(t, again.commit)
	assert.Equal(t, uint64(2), again.targets[0].Strategy.Value)
	again.answer(section(1, 2, 3, 4))
	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{2, 3, 4}, offsets(result.polled))
}

// A member refused as not the owner reads the partition again from its stored
// offset, since a rejoin gives the partition a new owner generation.
func TestPartitionReader_ReadsAPartitionRefusedAsNotOwnedAgainWithoutItsOwnerGeneration(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 0))
	next(t, reader)
	continued := client.nextRead(t)
	require.Equal(t, testContext, *continued.targets[0].Strategy.Context)
	continued.answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.ErrConsumerGroupPartitionNotOwned})

	retry := client.nextRead(t)

	assert.Equal(t, iggcon.POLLING_NEXT, retry.targets[0].Strategy.Kind)
	assert.Nil(t, retry.targets[0].Strategy.Context)
}

func TestPartitionReader_ReportsAPartitionThatStaysRefusedAsNotOwned(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, waitOneSecond, 1)
	for range lastingRefusalsBeforeStop {
		client.nextRead(t).answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.ErrConsumerGroupPartitionNotOwned})
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	_, err := reader.Next(ctx)

	var readError *ReadError
	require.ErrorAs(t, err, &readError)
	require.ErrorIs(t, err, ierror.ErrConsumerGroupPartitionNotOwned)
	assert.Equal(t, uint32(1), *readError.PartitionID)
	client.nextRead(t).answer(section(1, 0))
	assert.Equal(t, []uint64{0}, offsets(next(t, reader)), "the partition is read again after the report")
}

// A section that does not decode is read again one message at a time, so
// only the message that does not decode is skipped.
func TestPartitionReader_ReadsAnUndecodableSectionOneMessageAtATimeAndSkipsOnlyTheBadMessage(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(sectionOf(1, 3, 3, record(0), undecodable(1), record(2)))

	first := client.nextRead(t)
	assert.Equal(t, uint32(1), first.count)
	assert.Equal(t, iggcon.OffsetPollingStrategy(0).WithContext(testContext), first.targets[0].Strategy)
	first.answer(section(1, 0))
	assert.Equal(t, []uint64{0}, offsets(next(t, reader)))
	bad := client.nextRead(t)
	assert.Equal(t, uint32(1), bad.count)
	assert.Equal(t, uint64(1), bad.targets[0].Strategy.Value)
	bad.answer(sectionOf(1, 2, 1, undecodable(1)))
	_, err := reader.Next(context.Background())
	var readError *ReadError
	require.ErrorAs(t, err, &readError)
	require.NotNil(t, readError.Skip)
	assert.Equal(t, uint64(2), readError.Skip.Value, "only the message that does not decode is skipped")

	require.NoError(t, reader.Seek(context.Background(), 1, *readError.Skip))
	after := client.nextRead(t)
	assert.Equal(t, uint64(2), after.targets[0].Strategy.Value)
	after.answer(section(1, 2))
	assert.Equal(t, []uint64{2}, offsets(next(t, reader)))
}

func TestPartitionReader_StoresAgainACommitWhoseStoreFailed(t *testing.T) {
	client := newFakeClient()
	client.failedStores = 1
	reader := standalone(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 3, 4))
	checkpoint, _ := next(t, reader).Checkpoint()

	require.ErrorIs(t, reader.Commit(context.Background(), checkpoint), errStoreFailed)
	require.NoError(t, reader.Commit(context.Background(), checkpoint))

	assert.Equal(t, []store{{1, 4}}, client.recordedStores())
}

// The server raises the metadata op of a partition context per read, so a
// checkpoint delivered before that still counts.
func TestPartitionReader_CommitsACheckpointDeliveredBeforeTheMetadataOpMoved(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 3))
	earlier, _ := next(t, reader).Checkpoint()
	later := section(1, 4)
	later.Context.MetadataOp++
	client.nextRead(t).answer(later)
	next(t, reader)

	require.NoError(t, reader.Commit(context.Background(), earlier))

	assert.Equal(t, []store{{1, 3}}, client.recordedStores())
}

func TestPartitionReader_MarksACheckpointDeliveredBeforeTheMetadataOpMoved(t *testing.T) {
	client := newFakeClient()
	auto := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessage}, After: iggcon.CommitAfterMarked}
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.AutoCommitPolicy(auto)), 1)
	client.nextRead(t).answer(section(1, 3))
	earlier, _ := next(t, reader).Checkpoint()
	later := section(1, 4)
	later.Context.MetadataOp++
	client.nextRead(t).answer(later)
	next(t, reader)

	require.NoError(t, reader.Mark(earlier))

	require.Eventually(t, func() bool { return len(client.recordedStores()) == 1 }, testTimeout, 5*time.Millisecond)
	assert.Equal(t, []store{{1, 3}}, client.recordedStores())
}

func TestPartitionReader_CloseCalledAgainWaitsForTheFirstAndReturnsItsResult(t *testing.T) {
	client := newFakeClient()
	client.starts[1] = 5
	client.storeStarted = make(chan context.Context, 4)
	client.storeRelease = make(chan struct{})
	client.failedStores = 1
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	nextInBackground(reader)
	client.nextRead(t)
	first := make(chan error, 1)
	go func() { first <- reader.Close(context.Background()) }()
	select {
	case <-client.storeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the close stored nothing")
	}

	second := make(chan error, 1)
	go func() { second <- reader.Close(context.Background()) }()

	select {
	case <-second:
		t.Fatal("the second close returned before the first one finished")
	case <-time.After(300 * time.Millisecond):
	}
	close(client.storeRelease)
	require.ErrorIs(t, <-first, errStoreFailed)
	require.ErrorIs(t, <-second, errStoreFailed)
}

// After a lost reply the reader waits the retry interval before it reads the
// partition again, instead of reading it at the round trip rate.
func TestPartitionReader_WaitsTheRetryIntervalAfterALostReplyBeforeItReadsThePartitionAgain(t *testing.T) {
	for _, whole := range []bool{true, false} {
		t.Run(fmt.Sprintf("whole_%t", whole), func(t *testing.T) {
			client := newFakeClient()
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			polled := nextInBackground(reader)
			client.nextRead(t).answer(section(1, 0, 1))
			require.NoError(t, (<-polled).err)
			nextInBackground(reader)
			lost := client.nextRead(t)
			started := time.Now()
			if whole {
				lost.reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
			} else {
				lost.answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.FetchProgressUncertain{OperationID: 42}})
			}

			client.nextRead(t)

			assert.GreaterOrEqual(t, time.Since(started), testRetryInterval)
		})
	}
}

// A forward jump of a committing read without a lost read of the partition is
// a store of the application or of another session, so the reader goes on from
// there and reads nothing again.
func TestPartitionReader_GoesOnFromAForwardJumpWithoutALostRead(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty_%t", empty), func(t *testing.T) {
			client := newFakeClient()
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			polled := nextInBackground(reader)
			client.nextRead(t).answer(section(1, 0, 1))
			require.NoError(t, (<-polled).err)
			polled = nextInBackground(reader)
			jumped := client.nextRead(t)
			require.True(t, jumped.commit)

			if empty {
				jumped.answer(iggcon.FetchMessagesSection{PartitionId: 1, Context: testContext, CurrentOffset: 5, NextOffset: 5})
			} else {
				jumped.answer(section(1, 5, 6))
				result := <-polled
				require.NoError(t, result.err)
				assert.Equal(t, []uint64{5, 6}, offsets(result.polled))
				nextInBackground(reader)
			}

			after := client.nextRead(t)
			assert.True(t, after.commit, "the skipped offsets are not read again")
			assert.Equal(t, iggcon.POLLING_NEXT, after.targets[0].Strategy.Kind)
		})
	}
}

// The lost range can hold fewer messages than the minimum count of the reader,
// and its read still goes out.
func TestPartitionReader_ReadsALostRangeShorterThanTheMinimumCount(t *testing.T) {
	client := newFakeClient()
	options := withCommit(waitOneSecond, iggcon.OnReadCommit())
	options.MinCount = 5
	reader := standalone(t, client, options, 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1, 2, 3, 4))
	require.NoError(t, (<-polled).err)
	polled = nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	client.nextRead(t).answer(section(1, 8, 9, 10, 11, 12))

	gap := client.nextRead(t)

	assert.False(t, gap.commit)
	assert.Equal(t, uint32(3), gap.count)
	gap.answer(section(1, 5, 6, 7))
	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{5, 6, 7}, offsets(result.polled))
}

// Under OnRead the server can have stored what a lost read returned, so Close
// moves the offset back to the last delivered message.
func TestPartitionReader_MovesTheOffsetBackOverALostReadOnClose(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	require.Eventually(t, func() bool { return len(client.recordedAbandons()) == 1 }, testTimeout, 5*time.Millisecond)

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
}

// A committing read that ends during Close stored its messages, which nobody
// receives, so Close moves the offset back over them.
func TestPartitionReader_MovesTheOffsetBackOverAReadThatEndsDuringClose(t *testing.T) {
	client := newFakeClient()
	client.holdsCommits = true
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	nextInBackground(reader)
	parked := client.nextRead(t)
	closed := make(chan error, 1)
	go func() { closed <- reader.Close(context.Background()) }()
	require.Eventually(t, func() bool { return reader.run.Err() != nil }, testTimeout, time.Millisecond)

	parked.answer(section(1, 2, 3))

	require.NoError(t, <-closed)
	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
}

// A lost read of a partition that a seek or a new assignment replaced still
// holds the progress of the partition, so the reader abandons it.
func TestPartitionReader_AbandonsALostReadOfAPartitionThatChangedMeanwhile(t *testing.T) {
	for _, change := range []string{"seek", "revoke"} {
		t.Run(change, func(t *testing.T) {
			client := newFakeClient()
			options := withCommit(waitOneSecond, iggcon.OnReadCommit())
			var reader *PartitionReader
			if change == "seek" {
				reader = standalone(t, client, options, 1)
			} else {
				reader = member(t, client, options, 1)
			}
			polled := nextInBackground(reader)
			client.nextRead(t).answer(section(1, 0, 1))
			require.NoError(t, (<-polled).err)
			nextInBackground(reader)
			inFlight := client.nextRead(t)
			if change == "seek" {
				require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(10)))
			} else {
				assign(t, client, reader, 2)
			}

			inFlight.answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.FetchProgressUncertain{OperationID: 42}})

			require.Eventually(t, func() bool { return slices.Equal(client.recordedAbandons(), []uint64{42}) },
				testTimeout, 5*time.Millisecond)
		})
	}
}

// The end of a read of an older generation leaves the read of the new
// generation in flight, so no second read of the same position goes out.
func TestPartitionReader_KeepsTheReadOfANewGenerationInFlightWhenAnOlderReadEnds(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, waitOneSecond, 1)
	old := client.nextRead(t)
	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(5)))
	sought := client.nextRead(t)
	require.Equal(t, uint64(5), sought.targets[0].Strategy.Value)

	old.answer(section(1, 0))

	assert.True(t, client.readsNothingFor(300*time.Millisecond), "the read of the new generation is still in flight")
	sought.answer(section(1, 5))
	assert.Equal(t, []uint64{5}, offsets(next(t, reader)))
}

// A member that the server no longer knows has left its group already.
func TestPartitionReader_ClosesAMemberThatTheServerNoLongerKnows(t *testing.T) {
	client := newFakeClient()
	client.leaveErr = ierror.ErrConsumerGroupMemberNotFound
	reader := member(t, client, waitOneSecond, 1)

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []string{"join", "leave"}, client.membership)
}

// A reader that commits nothing reads each backlogged partition of a group in
// a read of its own while slots are free, each with the whole count, and the
// quiet partitions of the group share one read that may wait.
func TestPartitionReader_ReadsEachBackloggedPartitionOfAGroupInItsOwnRead(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, waitOneSecond, 1, 2, 3, 4, 5)
	first := client.nextRead(t)
	require.Len(t, first.targets, 5)
	ahead := section(1, 0)
	ahead.CurrentOffset = 100

	first.answer(ahead, behind(2, 50), behind(3, 50), behind(4, 50), iggcon.FetchMessagesSection{PartitionId: 5, Context: testContext})

	reads := make([][]uint32, 0, 5)
	for range 5 {
		read := client.nextRead(t)
		assert.Equal(t, waitOneSecond.Count, read.count)
		reads = append(reads, partitionsOf(read))
	}
	assert.ElementsMatch(t, [][]uint32{{1}, {2}, {3}, {4}, {5}}, reads)
	assert.Equal(t, []uint64{0}, offsets(next(t, reader)))
}

// More backlogged partitions than free slots are dealt over the slots, least
// recently served first.
func TestPartitionReader_DealsMoreBackloggedPartitionsThanFreeSlotsOverTheSlots(t *testing.T) {
	client := newFakeClient()
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(1, 2, 3, 4), WithPollOptions(waitOneSecond),
		WithPrefetchMessages(3*waitOneSecond.Count), WithRetryInterval(testRetryInterval))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	ahead := section(1, 0)
	ahead.CurrentOffset = 100

	client.nextRead(t).answer(ahead, behind(2, 50), behind(3, 50), behind(4, 50))

	reads := [][]uint32{partitionsOf(client.nextRead(t)), partitionsOf(client.nextRead(t))}
	assert.ElementsMatch(t, [][]uint32{{2, 4}, {1, 3}}, reads, "the queued batch holds one of three slots")
	assert.True(t, client.readsNothingFor(300*time.Millisecond))
}

// Under OnRead a seek waits for the read in flight before it stores the offset
// before its start, and still stores it when Close starts before that read ends.
func TestPartitionReader_StoresAPendingSeekWhenCloseStartsBeforeTheReadInFlightEnds(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_%t", lost), func(t *testing.T) {
			client := newFakeClient()
			client.holdsCommits = true
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			polled := nextInBackground(reader)
			client.nextRead(t).answer(section(1, 0, 1))
			require.NoError(t, (<-polled).err)
			nextInBackground(reader)
			inFlight := client.nextRead(t)
			require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(10)))
			closed := make(chan error, 1)
			go func() { closed <- reader.Close(context.Background()) }()
			require.Eventually(t, func() bool { return reader.run.Err() != nil }, testTimeout, time.Millisecond)

			if lost {
				inFlight.reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
			} else {
				inFlight.answer(section(1, 2, 3))
			}

			require.NoError(t, <-closed)
			assert.Equal(t, []store{{1, 9}}, client.recordedStores(), "the seek decides where the next session starts")
			if lost {
				assert.Equal(t, []uint64{42}, client.recordedAbandons())
			}
		})
	}
}

// The offset before a seek is the last position of the new generation, so
// Close moves a partition with a read in flight back to it.
func TestPartitionReader_MovesTheOffsetBackBeforeTheSeekOffsetOnCloseWhileAReadIsInFlight(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(10)))
	nextInBackground(reader)
	assert.Equal(t, iggcon.POLLING_NEXT, client.nextRead(t).targets[0].Strategy.Kind)

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{1, 9}, {1, 9}}, client.recordedStores())
}

// There is no offset before 0, and the other strategies name no offset, so
// such a seek stores nothing under OnRead.
func TestPartitionReader_StoresNothingForASeekThatNamesNoOffsetAboveZeroUnderOnRead(t *testing.T) {
	for _, start := range []iggcon.PollingStrategy{
		iggcon.FirstPollingStrategy(), iggcon.OffsetPollingStrategy(0), iggcon.LastPollingStrategy(),
		iggcon.NextPollingStrategy(), iggcon.TimestampPollingStrategy(10),
	} {
		t.Run(fmt.Sprintf("%v", start.Kind), func(t *testing.T) {
			client := newFakeClient()
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			require.NoError(t, reader.Seek(context.Background(), 1, start))
			nextInBackground(reader)

			read := client.nextRead(t)
			kind := start.Kind
			if kind == iggcon.POLLING_FIRST {
				kind = iggcon.POLLING_OFFSET
			}
			assert.Equal(t, kind, read.targets[0].Strategy.Kind)
			assert.True(t, read.commit)
			assert.Empty(t, client.recordedStores())
		})
	}
}

// Under OnRead a waiting Next fills the free slots too: the backlogged
// partitions of a group take committing reads of their own, each with the
// whole count, beside the one read of its quiet partitions that may wait, and
// a partition is in one committing read at a time.
func TestPartitionReader_SpreadsTheBacklogOfAGroupOverCommittingReadsOfTheirOwnUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2, 3, 4)
	polled := nextInBackground(reader)
	first := client.nextRead(t)
	require.Len(t, first.targets, 4)
	first.answer(ahead(1, 100, 0), ahead(2, 100, 0), ahead(3, 100, 0), section(4, 0))
	require.NoError(t, (<-polled).err)
	for range 3 {
		next(t, reader)
	}
	assert.True(t, client.readsNothingFor(300*time.Millisecond), "the reader reads for a waiting Next only")

	polled = nextInBackground(reader)
	spread := make([]*fakeRead, 0, 4)
	reads := make([][]uint32, 0, 4)
	for range 4 {
		read := client.nextRead(t)
		assert.True(t, read.commit)
		assert.Equal(t, waitOneSecond.Count, read.count)
		spread = append(spread, read)
		reads = append(reads, partitionsOf(read))
	}
	assert.ElementsMatch(t, [][]uint32{{1}, {2}, {3}, {4}}, reads)

	for _, read := range spread {
		if partition := read.targets[0].PartitionId; partition != 4 {
			read.answer(ahead(partition, 100, 1))
		}
	}
	require.NoError(t, (<-polled).err)
	next(t, reader)
	next(t, reader)
	nextInBackground(reader)

	again := [][]uint32{partitionsOf(client.nextRead(t)), partitionsOf(client.nextRead(t)), partitionsOf(client.nextRead(t))}
	assert.ElementsMatch(t, [][]uint32{{1}, {2}, {3}}, again, "the backlog reads again beside the read that waits, which keeps its partition")
	assert.True(t, client.readsNothingFor(300*time.Millisecond))
}
