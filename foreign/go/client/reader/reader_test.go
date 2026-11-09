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
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/batch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTimeout       = 5 * time.Second
	testRetryInterval = 100 * time.Millisecond
	// servedMessages is the length of the partition that serve reads from,
	// and servedPerRead the most messages of one of its replies.
	servedMessages = 4
	servedPerRead  = 2
	// uncertainOperation is the operation of a store that ends with an unknown outcome.
	uncertainOperation uint64 = 77
)

var (
	testContext     = iggcon.PartitionContext{Incarnation: 7, OwnerGeneration: 1, MetadataOp: 1}
	errStoreFailed  = errors.New("the store failed")
	errDeleteFailed = errors.New("the delete failed")
	errLookupFailed = errors.New("the lookup failed")
	errProbeFailed  = errors.New("the probe failed")
	errReadFailed   = errors.New("the read failed")
)

type fakeRead struct {
	targets []iggcon.FetchMessagesTarget
	count   uint32
	commit  bool
	// poll marks a poll of one partition, which the reply answers with its one section.
	poll  bool
	reply chan fakeReply
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
// It keeps the stored offsets as the server does: a committing read moves an
// offset only forward, a store sets it, and a delete removes it and fails for
// a missing one. A probe reads after the stored offset. A store past the last
// message of a partition fails, if the fake knows that last message. As the
// client does, it blocks the offset writes of a partition after a committing
// read or a write with an unknown outcome, until the abandon of its operation.
type fakeClient struct {
	iggcon.Client
	mu        sync.Mutex
	reads     chan *fakeRead
	stores    []store
	offsets   map[uint32]uint64
	lookups   []uint32
	abandoned []uint64
	// blocked holds the operation with an unknown outcome of each blocked partition.
	blocked map[uint32]uint64
	// abandonRelease, when set, holds each abandon until it closes.
	abandonRelease chan struct{}
	// uncertainStores is the number of the next stores that apply and end with an unknown outcome.
	uncertainStores int
	// uncertainDeletes is the number of the next deletes that apply and end with an unknown outcome.
	uncertainDeletes int
	membership       []string
	assignments      chan []uint32
	syncs            []time.Time
	grouper          func([]uint32) [][]uint32
	// failedGroupings is the number of the next groupings that fail.
	failedGroupings int
	groupings       []time.Time
	starts          map[uint32]uint64
	lastOffsets     map[uint32]uint64
	deleted         []uint32
	storeStarted    chan context.Context
	storeRelease    chan struct{}
	// failedStores is the number of the next stores that fail.
	failedStores int
	// failedDeletes is the number of the next deletes that fail.
	failedDeletes int
	// failedLookups is the number of the next lookups that fail.
	failedLookups int
	// holdsCommits makes a committing read wait for its reply after its
	// caller stops waiting, as the client does.
	holdsCommits bool
	leaveErr     error
	// uncertainJoins and uncertainLeaves are the numbers of the next joins and
	// leaves that apply and end with an unknown outcome. Such a write blocks the
	// metadata lane, as the client does, until its resolve or abandon.
	uncertainJoins  int
	uncertainLeaves int
	metadataBlocked *uint64
	resolved        []uint64
	// unresolved makes each resolve end with an unknown outcome again.
	unresolved bool
	// memberMissing makes the next sync miss the member, as after a lost session.
	memberMissing bool
	// refusedProbes holds the refusals of the next probes of each partition, one
	// per probe, and failedProbes the errors of the whole request, which come first.
	refusedProbes map[uint32][]error
	failedProbes  map[uint32][]error
	// undecodableProbes names the partitions whose probes read a message that does not decode.
	undecodableProbes map[uint32]bool
	probes            []uint32
	// probeStarted, when set, receives each probe, which then waits until probeRelease closes.
	probeStarted chan struct{}
	probeRelease chan struct{}
	// generation moves on each sync, as every hand-over in the group moves it on a server.
	generation uint64
	// owners holds the installed owner generation of a partition, and installOp
	// the metadata op that installed the owners. As the server does, the fake
	// refuses an offset write that names another owner, and one that names none
	// with a metadata op below installOp.
	owners    map[uint32]uint64
	installOp uint64
	// watermark is the metadata op that the client saw committed. A join raises
	// it to installOp, as the reply of the join does.
	watermark uint64
	// storeContexts holds the context of each store that the fake got, also of
	// a refused one.
	storeContexts []iggcon.PartitionContext
}

func (f *fakeClient) ownedLocked(position iggcon.ConsumerPosition) error {
	owner, installed := f.owners[position.PartitionId]
	named := position.Context.OwnerGeneration
	if installed && (named != 0 && named != owner || named == 0 && position.Context.MetadataOp < f.installOp) {
		return ierror.ErrConsumerGroupPartitionNotOwned
	}
	return nil
}

func (f *fakeClient) MetadataWatermark() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watermark
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		reads:       make(chan *fakeRead, 16),
		offsets:     make(map[uint32]uint64),
		blocked:     make(map[uint32]uint64),
		starts:      make(map[uint32]uint64),
		lastOffsets: make(map[uint32]uint64),
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
	for _, target := range targets {
		if kind := target.Strategy.Kind; kind != iggcon.POLLING_OFFSET && kind != iggcon.POLLING_NEXT {
			return nil, ierror.ErrInvalidCommand
		}
	}
	if !autoCommit && count == 1 && len(targets) == 1 && targets[0].Strategy.Kind == iggcon.POLLING_NEXT {
		// As the client does, the fake sends no probe whose context ended.
		if ctx.Err() != nil {
			return nil, ierror.ErrTransientNotAccepted
		}
		partition := targets[0].PartitionId
		f.mu.Lock()
		started, release := f.probeStarted, f.probeRelease
		f.mu.Unlock()
		if started != nil {
			started <- struct{}{}
			<-release
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		f.probes = append(f.probes, partition)
		if failures := f.failedProbes[partition]; len(failures) > 0 {
			f.failedProbes[partition] = failures[1:]
			return nil, failures[0]
		}
		if refusals := f.refusedProbes[partition]; len(refusals) > 0 {
			f.refusedProbes[partition] = refusals[1:]
			return []iggcon.FetchMessagesSection{{PartitionId: partition, Err: refusals[0]}}, nil
		}
		first := f.starts[partition]
		if stored, ok := f.offsets[partition]; ok {
			first = max(first, stored+1)
		}
		served := section(partition, first)
		if f.undecodableProbes[partition] {
			served = sectionOf(partition, first+1, 1, undecodable(first))
		}
		if owner, installed := f.owners[partition]; installed {
			// As the server does, the fake serves a read with owner 0 for the installed owner.
			served.Context.OwnerGeneration, served.Context.MetadataOp = owner, max(served.Context.MetadataOp, f.installOp)
		}
		return []iggcon.FetchMessagesSection{served}, nil
	}
	read := &fakeRead{targets: targets, count: count, commit: autoCommit, reply: make(chan fakeReply, 1)}
	f.reads <- read
	var reply fakeReply
	if autoCommit && f.holdsCommits {
		reply = <-read.reply
	} else {
		select {
		case reply = <-read.reply:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if autoCommit {
		f.commitRead(read.targets, reply)
	}
	return reply.sections, reply.err
}

// commitRead stores what a committing read returned, and blocks each partition whose reply it lost.
func (f *fakeClient) commitRead(targets []iggcon.FetchMessagesTarget, reply fakeReply) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var unknown ierror.FetchProgressUncertain
	if errors.As(reply.err, &unknown) {
		for _, target := range targets {
			f.blocked[target.PartitionId] = unknown.OperationID
		}
	}
	for _, section := range reply.sections {
		if errors.As(section.Err, &unknown) {
			f.blocked[section.PartitionId] = unknown.OperationID
		}
		if reply.err != nil || section.Err != nil || section.MessageCount == 0 || section.NextOffset == 0 {
			continue
		}
		last := section.NextOffset - 1
		if stored, ok := f.offsets[section.PartitionId]; !ok || last > stored {
			f.offsets[section.PartitionId] = last
		}
	}
}

// PollMessages answers a poll of one partition as a read of one target, whose
// reply holds at most one section.
func (f *fakeClient) PollMessages(ctx context.Context, _, _ iggcon.Identifier, partitionId uint32, _ iggcon.Consumer,
	strategy iggcon.PollingStrategy, options iggcon.PollOptions,
) (*iggcon.PolledMessage, error) {
	read := &fakeRead{targets: []iggcon.FetchMessagesTarget{{PartitionId: partitionId, Strategy: strategy}}, count: options.Count,
		commit: options.Commit.Kind == iggcon.CommitOnRead, poll: true, reply: make(chan fakeReply, 1)}
	f.reads <- read
	var reply fakeReply
	select {
	case reply = <-read.reply:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if reply.err != nil {
		return nil, reply.err
	}
	if len(reply.sections) == 0 {
		return &iggcon.PolledMessage{PartitionId: partitionId}, nil
	}
	return binaryserialization.DeserializeSectionMessages(reply.sections[0])
}

// blockedLocked refuses an offset write of a partition that an operation with an unknown outcome blocks.
func (f *fakeClient) blockedLocked(partition uint32) error {
	if operation, ok := f.blocked[partition]; ok {
		return ierror.PartitionLaneBlocked{PartitionID: partition, OperationID: operation}
	}
	return nil
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
	// As the client does, the fake sends no write whose context ended.
	if ctx.Err() != nil {
		return ierror.ErrTransientNotAccepted
	}
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
	f.storeContexts = append(f.storeContexts, position.Context)
	if err := f.blockedLocked(position.PartitionId); err != nil {
		return err
	}
	if err := f.ownedLocked(position); err != nil {
		return err
	}
	if f.failedStores > 0 {
		f.failedStores--
		return errStoreFailed
	}
	if last, ok := f.lastOffsets[position.PartitionId]; ok && position.Offset > last {
		return ierror.ErrInvalidOffset
	}
	f.stores = append(f.stores, store{partition: position.PartitionId, offset: position.Offset})
	f.offsets[position.PartitionId] = position.Offset
	if f.uncertainStores > 0 {
		f.uncertainStores--
		f.blocked[position.PartitionId] = uncertainOperation
		return ierror.OperationUncertain{OperationID: uncertainOperation}
	}
	return nil
}

func (f *fakeClient) GetConsumerOffset(ctx context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier, partitionId *uint32) (*iggcon.ConsumerOffsetInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups = append(f.lookups, *partitionId)
	if f.failedLookups > 0 {
		f.failedLookups--
		return nil, errLookupFailed
	}
	stored, ok := f.offsets[*partitionId]
	if !ok {
		return nil, nil
	}
	return &iggcon.ConsumerOffsetInfo{PartitionId: *partitionId, StoredOffset: stored}, nil
}

func (f *fakeClient) DeleteConsumerPosition(ctx context.Context, _ iggcon.Consumer, _, _ iggcon.Identifier, position iggcon.ConsumerPosition) error {
	if ctx.Err() != nil {
		return ierror.ErrTransientNotAccepted
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.blockedLocked(position.PartitionId); err != nil {
		return err
	}
	if err := f.ownedLocked(position); err != nil {
		return err
	}
	f.deleted = append(f.deleted, position.PartitionId)
	if f.failedDeletes > 0 {
		f.failedDeletes--
		return errDeleteFailed
	}
	if _, ok := f.offsets[position.PartitionId]; !ok {
		return ierror.ErrConsumerOffsetNotFound
	}
	delete(f.offsets, position.PartitionId)
	if f.uncertainDeletes > 0 {
		f.uncertainDeletes--
		f.blocked[position.PartitionId] = uncertainOperation
		return ierror.OperationUncertain{OperationID: uncertainOperation}
	}
	return nil
}

func (f *fakeClient) AbandonOperation(_ context.Context, operationID uint64) error {
	if f.abandonRelease != nil {
		<-f.abandonRelease
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.abandoned = append(f.abandoned, operationID)
	if f.metadataBlocked != nil && *f.metadataBlocked == operationID {
		f.metadataBlocked = nil
	}
	for partition, operation := range f.blocked {
		if operation == operationID {
			delete(f.blocked, partition)
		}
	}
	return nil
}

func (f *fakeClient) JoinConsumerGroup(context.Context, iggcon.Identifier, iggcon.Identifier, iggcon.Identifier) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.writeMembershipLocked("join", &f.uncertainJoins, nil)
	if err == nil {
		f.watermark = max(f.watermark, f.installOp)
	}
	return err
}

func (f *fakeClient) LeaveConsumerGroup(ctx context.Context, _, _, _ iggcon.Identifier) error {
	if ctx.Err() != nil {
		return ierror.ErrTransientNotAccepted
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.writeMembershipLocked("leave", &f.uncertainLeaves, f.leaveErr)
}

func (f *fakeClient) writeMembershipLocked(write string, uncertain *int, refusal error) error {
	if f.metadataBlocked != nil {
		return ierror.LaneBlocked{OperationID: *f.metadataBlocked}
	}
	f.membership = append(f.membership, write)
	if *uncertain > 0 {
		*uncertain--
		operation := uncertainOperation
		f.metadataBlocked = &operation
		return ierror.OperationUncertain{OperationID: uncertainOperation}
	}
	return refusal
}

func (f *fakeClient) ResolveOperation(_ context.Context, operationID uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolved = append(f.resolved, operationID)
	if f.unresolved {
		return ierror.OperationUncertain{OperationID: operationID}
	}
	if f.metadataBlocked != nil && *f.metadataBlocked == operationID {
		f.metadataBlocked = nil
	}
	return nil
}

func (f *fakeClient) SyncConsumerGroup(ctx context.Context, _, _ iggcon.Identifier, _ iggcon.Identifier) (*iggcon.ConsumerGroupAssignment, error) {
	f.mu.Lock()
	missing := f.memberMissing
	f.memberMissing = false
	f.mu.Unlock()
	if missing {
		return nil, ierror.ErrConsumerGroupMemberNotFound
	}
	select {
	case partitions := <-f.assignments:
		f.mu.Lock()
		defer f.mu.Unlock()
		f.syncs = append(f.syncs, time.Now())
		f.generation++
		return &iggcon.ConsumerGroupAssignment{Generation: f.generation, Partitions: partitions}, nil
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

func (f *fakeClient) recordedStoreContexts() []iggcon.PartitionContext {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]iggcon.PartitionContext(nil), f.storeContexts...)
}

func (f *fakeClient) recordedAbandons() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint64(nil), f.abandoned...)
}

func (f *fakeClient) recordedProbes() []uint32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]uint32(nil), f.probes...)
}

func (f *fakeClient) recordedGroupings() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.groupings...)
}

func (f *fakeClient) recordedSyncs() []time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]time.Time(nil), f.syncs...)
}

func (r *fakeRead) answer(sections ...iggcon.FetchMessagesSection) {
	r.reply <- fakeReply{sections: sections}
}

// findsTheStart answers the poll of a timestamp or Last start, if one comes,
// with a message at first, and returns the committing read that follows it.
func findsTheStart(t *testing.T, client *fakeClient, first uint64) *fakeRead {
	t.Helper()
	read := client.nextRead(t)
	if read.poll {
		require.False(t, read.commit, "a poll that finds an offset stores nothing")
		read.answer(section(read.targets[0].PartitionId, first))
		read = client.nextRead(t)
	}
	require.True(t, read.commit)
	require.Equal(t, iggcon.POLLING_OFFSET, read.targets[0].Strategy.Kind)
	require.Equal(t, first, read.targets[0].Strategy.Value, "the first committing read starts at the offset that the start names or the poll found")
	return read
}

// serve answers a read of one partition that holds servedMessages messages
// from offset 0, as the server does: an offset read starts at its offset, and
// a next read after the stored offset. It returns the served offsets, and
// leaves a read with nothing to serve waiting, as a read at the end does.
func (f *fakeClient) serve(read *fakeRead) []uint64 {
	target := read.targets[0]
	first := target.Strategy.Value
	if target.Strategy.Kind == iggcon.POLLING_NEXT {
		first = 0
		f.mu.Lock()
		if stored, ok := f.offsets[target.PartitionId]; ok {
			first = stored + 1
		}
		f.mu.Unlock()
	}
	var served []uint64
	for offset := first; offset < servedMessages && len(served) < servedPerRead; offset++ {
		served = append(served, offset)
	}
	if len(served) > 0 {
		read.answer(section(target.PartitionId, served...))
	}
	return served
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
	return memberWith(t, client, assigned, WithPollOptions(options))
}

// memberWith is a group reader with options whose first refresh reads assigned.
func memberWith(t *testing.T, client *fakeClient, assigned []uint32, options ...Option) *PartitionReader {
	t.Helper()
	client.assignments <- assigned
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewGroupConsumer(identifier(t, 9)), append([]Option{WithRetryInterval(testRetryInterval)}, options...)...)
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

// onReadFrom is a standalone reader of partition 1 under OnRead from start.
func onReadFrom(t *testing.T, client *fakeClient, start iggcon.PollingStrategy) *PartitionReader {
	t.Helper()
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(1), WithStart(start),
		WithPollOptions(withCommit(waitOneSecond, iggcon.OnReadCommit())), WithRetryInterval(testRetryInterval))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	return reader
}

// memberOfOwnPrimaries is a group reader of the partitions 1 to primaries, each
// on a primary of its own, with a prefetch of whole batches.
func memberOfOwnPrimaries(t *testing.T, client *fakeClient, primaries int, prefetchBatches uint32, commit bool) *PartitionReader {
	t.Helper()
	client.grouper = func(partitions []uint32) [][]uint32 {
		groups := make([][]uint32, 0, len(partitions))
		for _, partition := range partitions {
			groups = append(groups, []uint32{partition})
		}
		return groups
	}
	partitions := make([]uint32, primaries)
	for index := range partitions {
		partitions[index] = uint32(index + 1)
	}
	client.assignments <- partitions
	options := waitOneSecond
	if commit {
		options.Commit = iggcon.OnReadCommit()
	}
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewGroupConsumer(identifier(t, 9)), WithPollOptions(options),
		WithPrefetchMessages(prefetchBatches*options.Count), WithRetryInterval(testRetryInterval))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	return reader
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

// logLines receives each line that a text handler writes, and drops a line
// that does not fit, so a logger never blocks.
type logLines chan string

func (l logLines) Write(line []byte) (int, error) {
	select {
	case l <- string(line):
	default:
	}
	return len(line), nil
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

func TestPartitionReader_DrainsABusyPrimaryWhileOtherPrimariesWaitWithinTheEffectivePrefetch(t *testing.T) {
	const minimumSlots = 32
	for _, budget := range []struct {
		name            string
		primaries       int
		prefetchBatches uint32
	}{
		{"one_batch_three_primaries", 3, 1},
		{"minimum_prefetch", minimumSlots, 1},
	} {
		for _, commit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/on_read_%t", budget.name, commit), func(t *testing.T) {
				client := newFakeClient()
				reader := memberOfOwnPrimaries(t, client, budget.primaries, budget.prefetchBatches, commit)
				polled := nextInBackground(reader)
				reads := make(map[uint32]*fakeRead, budget.primaries)
				for range budget.primaries {
					read := client.nextRead(t)
					require.Len(t, read.targets, 1)
					assert.Equal(t, commit, read.commit)
					reads[read.targets[0].PartitionId] = read
				}
				require.Len(t, reads, budget.primaries, "every idle primary waits concurrently before any reply")

				const busyPartition = uint32(2)
				const lastOffset = uint64(2)
				busy := reads[busyPartition]
				for offset := uint64(0); offset <= lastOffset; offset++ {
					busy.answer(ahead(busyPartition, lastOffset, offset))
					result := <-polled
					require.NoError(t, result.err)
					assert.Equal(t, []uint64{offset}, offsets(result.polled))
					if offset < lastOffset {
						polled = nextInBackground(reader)
						busy = client.nextRead(t)
						assert.Equal(t, []uint32{busyPartition}, partitionsOf(busy), "quiet primaries keep their pending reads")
						assert.Equal(t, commit, busy.commit)
					}
				}
			})
		}
	}
}

// A larger prefetch does not raise the 32 reads that one reader runs at once,
// half of the deferred requests of a client. The group left without a read
// takes the first read that ends, before a busy group reads again.
func TestPartitionReader_RunsAtMost32ReadsAtOnceWithALargerPrefetch(t *testing.T) {
	const maxReads = 32
	for _, commit := range []bool{false, true} {
		t.Run(fmt.Sprintf("on_read_%t", commit), func(t *testing.T) {
			client := newFakeClient()
			reader := memberOfOwnPrimaries(t, client, maxReads+1, 2*maxReads, commit)
			polled := nextInBackground(reader)
			reads := make(map[uint32]*fakeRead, maxReads)
			for range maxReads {
				read := client.nextRead(t)
				reads[read.targets[0].PartitionId] = read
			}
			require.Len(t, reads, maxReads)
			assert.True(t, client.readsNothingFor(300*time.Millisecond), "a 64-batch budget runs at most 32 reads at once")

			const busyPartition = uint32(2)
			reads[busyPartition].answer(ahead(busyPartition, 1, 0))
			result := <-polled
			require.NoError(t, result.err)
			// Under OnRead the reader reads only for a waiting Next.
			nextInBackground(reader)
			assert.Equal(t, []uint32{maxReads + 1}, partitionsOf(client.nextRead(t)),
				"the group without a read takes the read that ended")
		})
	}
}

// The probes of a committing read count as reads of the reader.
func TestPartitionReader_RunsAtMost32ProbesAtOnceUnderOnRead(t *testing.T) {
	const maxReads = 32
	client := newFakeClient()
	partitions := make([]uint32, maxReads+1)
	for index := range partitions {
		partitions[index] = uint32(index + 1)
	}
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), partitions...)
	nextInBackground(reader)

	read := client.nextRead(t)
	assert.Len(t, client.recordedProbes(), maxReads, "the probes of one read take at most 32 requests")
	assert.Equal(t, partitions[:maxReads], partitionsOf(read), "a partition without a probe waits for a later read")
}

// Reads of one partition alone can wait on the server, so they leave a request
// for the read of each group, also when more of them wait than that allows.
func TestPartitionReader_LeavesARequestForTheReadOfEachGroupToReadsOfOnePartitionAlone(t *testing.T) {
	const maxReads = 32
	const groups = 2
	alone := make([]uint32, maxReads-groups+1)
	undecoded := make([]iggcon.FetchMessagesSection, 0, len(alone))
	for index := range alone {
		alone[index] = uint32(index + 1)
		undecoded = append(undecoded, sectionOf(alone[index], 2, 2, record(0), undecodable(1)))
	}
	quiet := uint32(len(alone) + 1)
	other := quiet + 1
	client := newFakeClient()
	client.grouper = func([]uint32) [][]uint32 { return [][]uint32{append(slices.Clone(alone), quiet), {other}} }
	standalone(t, client, waitOneSecond, append(slices.Clone(alone), quiet, other)...)
	first, second := client.nextRead(t), client.nextRead(t)
	if len(first.targets) == 1 {
		first = second
	}
	first.answer(undecoded...)

	singles := 0
	var groupReads [][]uint32
	for range maxReads - 1 {
		if read := client.nextRead(t); read.count == 1 {
			singles++
		} else {
			groupReads = append(groupReads, partitionsOf(read))
		}
	}
	assert.Equal(t, maxReads-groups, singles, "the reads of one partition alone leave a request per group")
	assert.Equal(t, [][]uint32{{quiet}}, groupReads, "the group whose read ended reads again")
	assert.True(t, client.readsNothingFor(300*time.Millisecond), "the reads stay within 32 requests")
}

// Reads of one partition alone take turns in partition order, so a gap that
// goes on waits for the next partition in turn.
func TestPartitionReader_GivesTheNextReadOfOnePartitionAloneToTheNextPartitionInTurn(t *testing.T) {
	const maxReads = 32
	const groups = 2
	const heldOffset = 5
	gapped := make([]uint32, maxReads-groups+1)
	heads := make([]iggcon.FetchMessagesSection, 0, len(gapped))
	for index := range gapped {
		gapped[index] = uint32(index + 1)
		heads = append(heads, section(gapped[index], heldOffset))
	}
	other := uint32(len(gapped) + 1)
	client := newFakeClient()
	client.grouper = func([]uint32) [][]uint32 { return [][]uint32{gapped, {other}} }
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), append(slices.Clone(gapped), other)...)
	nextInBackground(reader)
	first, second := client.nextRead(t), client.nextRead(t)
	if len(first.targets) == 1 {
		first = second
	}
	first.reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	client.nextRead(t).answer(heads...)

	gaps := make(map[uint32]*fakeRead, maxReads-groups)
	for range maxReads - groups {
		gap := client.nextRead(t)
		gaps[gap.targets[0].PartitionId] = gap
	}
	inTurn := gapped[len(gapped)-1]
	require.NotContains(t, gaps, inTurn)
	gaps[gapped[0]].answer(section(gapped[0], 0))
	assert.Equal(t, []uint32{inTurn}, partitionsOf(client.nextRead(t)), "the partition whose read just ended waits for its turn")
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

// A fully zero context names no history, so a partition at a start or a seek
// with one reads again after a new history, as without a context.
func TestPartitionReader_ReadsAgainAfterANewHistoryAtAStartOrSeekWithAZeroContext(t *testing.T) {
	position := iggcon.OffsetPollingStrategy(5).WithContext(iggcon.PartitionContext{})
	for _, test := range []struct {
		name  string
		start func(t *testing.T, client *fakeClient) *PartitionReader
	}{
		{"start", func(t *testing.T, client *fakeClient) *PartitionReader {
			reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
				iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(1), WithStart(position),
				WithPollOptions(waitOneSecond), WithRetryInterval(testRetryInterval))
			require.NoError(t, err)
			t.Cleanup(func() { _ = reader.Close(context.Background()) })
			return reader
		}},
		{"seek", func(t *testing.T, client *fakeClient) *PartitionReader {
			reader := standalone(t, client, waitOneSecond, 1)
			// The read from before the seek stays unanswered.
			client.nextRead(t)
			require.NoError(t, reader.Seek(context.Background(), 1, position))
			return reader
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			reader := test.start(t, client)
			client.nextRead(t).answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.ErrHistoryUnavailable})

			read := client.nextRead(t)
			assert.Equal(t, iggcon.OffsetPollingStrategy(5), read.targets[0].Strategy, "the partition reads again from its position")
			read.answer(section(1, 5))
			assert.Equal(t, []uint64{5}, offsets(next(t, reader)))
		})
	}
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

// A join hands the partitions of a member over one at a time, so the member
// syncs again soon after the sync that followed its join, not a refresh
// interval later.
func TestPartitionReader_SyncsAgainSoonAfterAJoinWhoseFirstSyncListsNoPartition(t *testing.T) {
	const soon = DefaultRetryInterval / 2
	client := newFakeClient()
	client.assignments <- []uint32{}
	client.assignments <- []uint32{1}
	started := time.Now()
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewGroupConsumer(identifier(t, 9)), WithPollOptions(waitOneSecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })

	client.nextRead(t).answer(section(1, 0))
	assert.Equal(t, []uint64{0}, offsets(next(t, reader)))

	assert.Less(t, time.Since(started), soon, "the partition waits for no refresh interval of %v", DefaultRetryInterval)
}

// Only the partitions of the member count, so a generation that moves on each
// sync does not keep the member syncing soon once its pause reached the
// refresh interval.
func TestPartitionReader_SyncsOncePerRefreshIntervalWhileItsPartitionsStayTheSame(t *testing.T) {
	// The pauses of 10 ms, 20 ms, ... 640 ms reach the default refresh interval within this time.
	const settled = 2 * time.Second
	client := newFakeClient()
	stopAssigning := make(chan struct{})
	defer close(stopAssigning)
	go func() {
		for {
			select {
			case client.assignments <- []uint32{1}:
			case <-stopAssigning:
				return
			}
		}
	}()
	started := time.Now()
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewGroupConsumer(identifier(t, 9)), WithPollOptions(waitOneSecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })

	require.Eventually(t, func() bool {
		syncs := client.recordedSyncs()
		return len(syncs) > 1 && syncs[len(syncs)-2].Sub(started) >= settled
	}, testTimeout, 10*time.Millisecond, "the member syncs twice after the first %v", settled)

	syncs := client.recordedSyncs()
	for index := 1; index < len(syncs); index++ {
		if syncs[index].Sub(started) >= settled {
			assert.GreaterOrEqual(t, syncs[index].Sub(syncs[index-1]), DefaultRetryInterval, "the pause before sync %d", index)
		}
	}
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
		var readError *ReadError
		require.ErrorAs(t, result.err, &readError)
		require.NotNil(t, readError.PartitionID)
		assert.Equal(t, uint32(1), *readError.PartitionID)
		assert.Contains(t, readError.Error(), "offsets 2 to 4 ", "the report names the range that the lost read stored")
		assert.Equal(t, &OffsetRange{First: 2, Last: 4}, readError.Lost)
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

// A failed lookup keeps the last good groups, so the partitions read on with
// them, and the lookup runs again after the retry interval. A partition that
// its primary refused waits for the end of the next lookup, so while the
// lookups fail the moved primary gets one read per lookup.
func TestPartitionReader_ReadsOnWithTheLastGoodGroupsWhileTheLookupsFail(t *testing.T) {
	for _, whole := range []bool{false, true} {
		t.Run(fmt.Sprintf("whole_%t", whole), func(t *testing.T) {
			client := newFakeClient()
			client.grouper = func([]uint32) [][]uint32 { return [][]uint32{{1}, {2}} }
			reader := standalone(t, client, waitOneSecond, 1, 2)
			moved, staying := client.nextRead(t), client.nextRead(t)
			if moved.targets[0].PartitionId != 1 {
				moved, staying = staying, moved
			}
			client.mu.Lock()
			// Every later lookup fails.
			client.failedGroupings = math.MaxInt
			client.mu.Unlock()

			for range 2 {
				// A stall can add a retry of the lookup, so only a lower bound holds.
				before := len(client.recordedGroupings())
				if whole {
					moved.reply <- fakeReply{err: ierror.ErrNotPrimary}
				} else {
					moved.answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.ErrNotPrimary})
				}
				moved = client.nextRead(t)
				require.Equal(t, []uint32{1}, partitionsOf(moved))
				assert.Greater(t, len(client.recordedGroupings()), before, "the refused partition waits for the end of the next lookup")
			}
			staying.answer(section(2, 0))

			assert.Equal(t, []uint32{2}, partitionsOf(client.nextRead(t)), "the other partition reads on")
			assert.Equal(t, []uint64{0}, offsets(next(t, reader)))
			groupings := client.recordedGroupings()
			assert.GreaterOrEqual(t, groupings[2].Sub(groupings[1]), testRetryInterval)
		})
	}
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
	sought := make(chan error, 1)
	// The seek waits for the old read, which commits.
	go func() { sought <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(10)) }()
	old.answer(section(1, 0, 1))
	require.NoError(t, <-sought)
	client.starts[1] = 10
	assign(t, client, reader, 2)
	assign(t, client, reader, 1)
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
			// The server stored the read in flight, whose reply Close cancels.
			client.offsets[1] = start
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

// A read again of what a lost reply stored that the server serves from after
// its start shows that the history no longer holds that start, as after
// retention. The reader reports that part once, then hands over the messages
// that came and the batch read after the range.
func TestPartitionReader_ReportsOnceThePartOfALostRangeThatTheHistoryNoLongerHoldsUnderOnRead(t *testing.T) {
	empty := func(next uint64) iggcon.FetchMessagesSection {
		return iggcon.FetchMessagesSection{PartitionId: 1, Context: testContext, CurrentOffset: 6, NextOffset: next}
	}
	for _, test := range []struct {
		name string
		// replies answer the reads again in turn, the first one from offset 2.
		replies []iggcon.FetchMessagesSection
		lost    OffsetRange
		batches [][]uint64
	}{
		{"whole_range/empty_read", []iggcon.FetchMessagesSection{empty(5)}, OffsetRange{First: 2, Last: 4}, [][]uint64{{5, 6}}},
		{"whole_range/read_after_it", []iggcon.FetchMessagesSection{section(1, 5, 6)}, OffsetRange{First: 2, Last: 4}, [][]uint64{{5, 6}}},
		{"start/empty_read", []iggcon.FetchMessagesSection{empty(3), section(1, 3, 4)}, OffsetRange{First: 2, Last: 2}, [][]uint64{{3, 4}, {5, 6}}},
		{"start/read_inside_it", []iggcon.FetchMessagesSection{section(1, 3, 4)}, OffsetRange{First: 2, Last: 2}, [][]uint64{{3, 4}, {5, 6}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			polled := nextInBackground(reader)
			client.nextRead(t).answer(section(1, 0, 1))
			require.NoError(t, (<-polled).err)
			polled = nextInBackground(reader)
			client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
			client.nextRead(t).answer(section(1, 5, 6))
			from := uint64(2)
			for _, reply := range test.replies {
				again := client.nextRead(t)
				require.False(t, again.commit)
				require.Equal(t, from, again.targets[0].Strategy.Value)
				again.answer(reply)
				from = reply.NextOffset
			}

			err := (<-polled).err
			require.ErrorIs(t, err, ierror.FetchProgressUncertain{})
			var readError *ReadError
			require.ErrorAs(t, err, &readError)
			require.NotNil(t, readError.PartitionID)
			assert.Equal(t, uint32(1), *readError.PartitionID)
			assert.Contains(t, err.Error(), fmt.Sprintf("offsets %d to %d ", test.lost.First, test.lost.Last))
			assert.Equal(t, &test.lost, readError.Lost)
			for _, batch := range test.batches {
				assert.Equal(t, batch, offsets(next(t, reader)))
			}
			nextInBackground(reader)
			assert.True(t, client.nextRead(t).commit, "the reads go on after the range without a second report")
		})
	}
}

// A member refused as not the owner keeps the position of the partition and
// reads it again without its owner generation, since a new session installs
// the owner under a new one.
func TestPartitionReader_ReadsAPartitionRefusedAsNotOwnedAgainWithoutItsOwnerGeneration(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 0))
	next(t, reader)
	continued := client.nextRead(t)
	require.Equal(t, testContext, *continued.targets[0].Strategy.Context)
	continued.answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.ErrConsumerGroupPartitionNotOwned})

	retry := client.nextRead(t)

	unowned := testContext
	unowned.OwnerGeneration = 0
	assert.Equal(t, iggcon.POLLING_OFFSET, retry.targets[0].Strategy.Kind, "the partition keeps its position")
	assert.Equal(t, uint64(1), retry.targets[0].Strategy.Value, "the partition keeps its position")
	require.NotNil(t, retry.targets[0].Strategy.Context, "the partition keeps its position")
	assert.Equal(t, unowned, *retry.targets[0].Strategy.Context)
}

// Each refusal as not the owner in a row doubles the wait before the next read
// of the partition, up to 32 times the retry interval. The sixth in a row
// reports one error, and the partition is read again.
func TestPartitionReader_ReportsAPartitionThatStaysRefusedAsNotOwnedAfterADoublingWait(t *testing.T) {
	const interval = 10 * time.Millisecond
	client := newFakeClient()
	reader := memberWith(t, client, []uint32{1}, WithPollOptions(waitOneSecond), WithRetryInterval(interval))
	read := client.nextRead(t)
	for refusal := range lastingRefusalsBeforeStop {
		refused := time.Now()
		read.answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.ErrConsumerGroupPartitionNotOwned})
		read = client.nextRead(t)
		assert.GreaterOrEqual(t, time.Since(refused), interval<<refusal, "the wait after refusal %d in a row", refusal+1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	_, err := reader.Next(ctx)

	var readError *ReadError
	require.ErrorAs(t, err, &readError)
	require.ErrorIs(t, err, ierror.ErrConsumerGroupPartitionNotOwned)
	assert.Equal(t, uint32(1), *readError.PartitionID)
	read.answer(section(1, 0))
	assert.Equal(t, []uint64{0}, offsets(next(t, reader)), "the partition is read again after the report")
}

// Under OnRead the partitions of a group share a read, so a refusal of the
// probe of one partition counts against that partition only.
func TestPartitionReader_StopsOnlyThePartitionWhoseProbeStaysRefusedAndKeepsReadingItsGroupUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.refusedProbes = map[uint32][]error{2: slices.Repeat([]error{ierror.ErrUnauthorized}, lastingRefusalsBeforeStop)}
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(1, 2),
		WithPollOptions(withCommit(waitOneSecond, iggcon.OnReadCommit())), WithRetryInterval(time.Millisecond))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })

	var refusal *ReadError
	var delivered []uint64
	reportedAfter := 0
	for refusal == nil || len(delivered) == reportedAfter {
		polled := nextInBackground(reader)
		var result outcome
		select {
		case read := <-client.reads:
			require.Equal(t, []uint32{1}, partitionsOf(read), "the refused partition holds back no read of its group")
			read.answer(section(1, uint64(len(delivered))))
			result = <-polled
		case result = <-polled:
		}
		if result.err == nil {
			delivered = append(delivered, offsets(result.polled)...)
			continue
		}
		require.Nil(t, refusal, "only the refused partition is reported")
		require.ErrorAs(t, result.err, &refusal)
		require.Equal(t, uint32(2), *refusal.PartitionID, "only the refused partition stops")
		reportedAfter = len(delivered)
	}

	require.ErrorIs(t, refusal, ierror.ErrUnauthorized)
	probes := client.recordedProbes()
	slices.Sort(probes)
	assert.Equal(t, append([]uint32{1}, slices.Repeat([]uint32{2}, lastingRefusalsBeforeStop)...), probes)
}

// A probe refused as not the owner resets its own partition only, so the
// other partitions of its group read on from their positions.
func TestPartitionReader_KeepsThePositionsOfTheGroupOfAPartitionWhoseProbeIsRefusedAsNotOwned(t *testing.T) {
	client := newFakeClient()
	client.refusedProbes = map[uint32][]error{2: {ierror.ErrConsumerGroupPartitionNotOwned}}
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0))
	require.NoError(t, (<-polled).err)
	assign(t, client, reader, 1, 2)
	// The reads go on with the last groups while the reader looks up the new
	// ones, so only a read after the lookup holds the new partition.
	require.Eventually(t, func() bool {
		reader.mu.Lock()
		defer reader.mu.Unlock()
		return slices.ContainsFunc(reader.groups, func(group []uint32) bool { return slices.Contains(group, 2) })
	}, testTimeout, 5*time.Millisecond, "the groups hold the new partition")

	polled = nextInBackground(reader)
	read := client.nextRead(t)

	assert.Equal(t, []uint32{1}, partitionsOf(read), "the refused partition holds back no read of its group")
	assert.Equal(t, []uint32{1, 2}, client.recordedProbes(), "the other partition reads on without a new probe")
	read.answer(section(1, 1))
	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{1}, offsets(result.polled))
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

// A probe needs only the context and the offset of the first message, so a
// first message that does not decode does not fail the read of the group. The
// committing read stops only the partition of that message, with a skip past
// it, and the other partitions of the read go on.
func TestPartitionReader_StopsOnlyThePartitionWhoseFirstMessageDoesNotDecodeUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.undecodableProbes = map[uint32]bool{1: true}
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	polled := nextInBackground(reader)

	read := client.nextRead(t)
	require.True(t, read.commit)
	require.Equal(t, []uint32{1, 2}, partitionsOf(read))
	read.answer(sectionOf(1, 1, 1, undecodable(0)), section(2, 0))

	var batches []*iggcon.PolledMessage
	var failures []*ReadError
	for _, result := range []outcome{<-polled, <-nextInBackground(reader)} {
		var readError *ReadError
		if errors.As(result.err, &readError) {
			failures = append(failures, readError)
			continue
		}
		require.NoError(t, result.err)
		batches = append(batches, result.polled)
	}
	require.Len(t, batches, 1)
	assert.Equal(t, uint32(2), batches[0].PartitionId)
	assert.Equal(t, []uint64{0}, offsets(batches[0]))
	require.Len(t, failures, 1)
	require.NotNil(t, failures[0].PartitionID, "the error names its partition, not the whole reader")
	assert.Equal(t, uint32(1), *failures[0].PartitionID)
	require.NotNil(t, failures[0].Skip)
	assert.Equal(t, uint64(1), failures[0].Skip.Value)
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

// A commit with an unknown outcome blocks the later offset writes of its
// partition on the client. The reader abandons it and returns its error, so a
// retry of the commit stores.
func TestPartitionReader_AbandonsACommitWithAnUnknownOutcomeSoThatItsRetryStores(t *testing.T) {
	client := newFakeClient()
	client.uncertainStores = 1
	reader := standalone(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 3, 4))
	checkpoint, _ := next(t, reader).Checkpoint()

	require.ErrorIs(t, reader.Commit(context.Background(), checkpoint), ierror.OperationUncertain{})
	require.NoError(t, reader.Commit(context.Background(), checkpoint), "no lane block refuses the retry")

	assert.Equal(t, []store{{1, 4}, {1, 4}}, client.recordedStores())
	assert.Equal(t, []uint64{uncertainOperation}, client.recordedAbandons())
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

// A lost read of a partition that a seek waits for, or that a new assignment
// replaced, still holds the progress of the partition, so the reader abandons it.
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
			var sought chan error
			if change == "seek" {
				sought = make(chan error, 1)
				go func() { sought <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(10)) }()
			} else {
				assign(t, client, reader, 2)
			}

			inFlight.answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.FetchProgressUncertain{OperationID: 42}})

			require.Eventually(t, func() bool { return slices.Equal(client.recordedAbandons(), []uint64{42}) },
				testTimeout, 5*time.Millisecond)
			if sought != nil {
				require.NoError(t, <-sought)
			}
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

// A group write with an unknown outcome blocks the metadata writes of the
// client, also those of the application. No reader remains to resolve a first
// join, so the reader abandons it.
func TestPartitionReader_AbandonsAFirstJoinWithAnUnknownOutcome(t *testing.T) {
	client := newFakeClient()
	client.uncertainJoins = 1
	group, err := iggcon.NewIdentifier(uint32(9))
	require.NoError(t, err)

	_, err = NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewGroupConsumer(group), WithPollOptions(waitOneSecond), WithRetryInterval(testRetryInterval))

	assert.Equal(t, ierror.OperationUncertain{OperationID: uncertainOperation}, err)
	assert.Equal(t, []uint64{uncertainOperation}, client.abandoned)
	assert.Nil(t, client.metadataBlocked, "the metadata writes of the client go on")
}

// A sync that finds the member does not join again, so the next refresh
// resolves a rejoin with an unknown outcome before its sync.
func TestPartitionReader_ResolvesARejoinWithAnUnknownOutcomeAtTheNextRefresh(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, waitOneSecond, 1)
	rejoinUncertainly(t, client)

	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return client.metadataBlocked == nil
	}, testTimeout, 5*time.Millisecond, "the metadata writes of the client go on")
	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []uint64{uncertainOperation}, client.resolved)
	assert.Empty(t, client.abandoned)
	assert.Equal(t, []string{"join", "join", "leave"}, client.membership)
}

// Close abandons a rejoin whose outcome is still unknown, so its leave can go
// out, and a leave with an unknown outcome, which it returns.
func TestPartitionReader_AbandonsAGroupWriteWithAnUnknownOutcomeOnClose(t *testing.T) {
	for _, test := range []struct {
		name   string
		rejoin bool
		err    error
	}{
		{"rejoin still unknown", true, nil},
		{"leave", false, ierror.OperationUncertain{OperationID: uncertainOperation}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			client.unresolved = true
			reader := member(t, client, waitOneSecond, 1)
			if test.rejoin {
				rejoinUncertainly(t, client)
			} else {
				client.mu.Lock()
				client.uncertainLeaves = 1
				client.mu.Unlock()
			}

			assert.Equal(t, test.err, reader.Close(context.Background()))

			assert.Equal(t, []uint64{uncertainOperation}, client.abandoned)
			assert.Nil(t, client.metadataBlocked, "the metadata writes of the client go on")
			assert.Equal(t, "leave", client.membership[len(client.membership)-1])
		})
	}
}

// rejoinUncertainly makes a sync of a group reader after its first one miss
// the member, as after a lost session. The reader joins again, and that join
// applies with an unknown outcome.
func rejoinUncertainly(t *testing.T, client *fakeClient) {
	t.Helper()
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.syncs) > 0
	}, testTimeout, 5*time.Millisecond, "the first sync reads the assignment")
	client.mu.Lock()
	client.memberMissing = true
	client.uncertainJoins = 1
	client.mu.Unlock()
	// A sync that waits already for the next assignment misses no member.
	client.assignments <- []uint32{1}
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.membership) == 2
	}, testTimeout, 5*time.Millisecond, "the reader joins again")
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
	const slots = 32
	client := newFakeClient()
	partitions := make([]uint32, slots+1)
	sections := []iggcon.FetchMessagesSection{ahead(1, 100, 0)}
	for index := range partitions {
		partitions[index] = uint32(index + 1)
		if index > 0 {
			sections = append(sections, behind(partitions[index], 50))
		}
	}
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(partitions...), WithPollOptions(waitOneSecond),
		WithPrefetchMessages(slots*waitOneSecond.Count), WithRetryInterval(testRetryInterval))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })

	client.nextRead(t).answer(sections...)

	reads := make([][]uint32, 0, slots-1)
	for range slots - 1 {
		reads = append(reads, partitionsOf(client.nextRead(t)))
	}
	expected := [][]uint32{{2, slots + 1}, {1, 3}}
	for partition := uint32(4); partition <= slots; partition++ {
		expected = append(expected, []uint32{partition})
	}
	assert.ElementsMatch(t, expected, reads, "the queued batch holds one slot, and the last served partition goes last")
	assert.True(t, client.readsNothingFor(300*time.Millisecond))
}

// Under OnRead a seek waits for the committing read in flight before it stores
// the offset before its start. Close waits for the seek, which still stores
// when Close starts before that read ends.
func TestPartitionReader_StoresASeekThatWaitsForTheReadInFlightWhenCloseStartsMeanwhile(t *testing.T) {
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
			sought := make(chan error, 1)
			go func() { sought <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(10)) }()
			assert.Never(t, func() bool { return len(sought) > 0 }, 300*time.Millisecond, 5*time.Millisecond,
				"the seek waits for the read in flight")
			assert.Empty(t, client.recordedStores(), "the seek stores after the read in flight")
			closed := make(chan error, 1)
			go func() { closed <- reader.Close(context.Background()) }()
			require.Eventually(t, func() bool { return reader.run.Err() != nil }, testTimeout, time.Millisecond)

			if lost {
				inFlight.reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
			} else {
				inFlight.answer(section(1, 2, 3))
			}

			require.NoError(t, <-sought)
			require.NoError(t, <-closed)
			assert.Equal(t, []store{{1, 9}}, client.recordedStores(), "the seek decides where the next session starts")
			if lost {
				assert.Equal(t, []uint64{42}, client.recordedAbandons())
			}
		})
	}
}

// The offset before a seek is the last position of the new generation, so
// Close moves the offset back to it over what a read in flight stored.
func TestPartitionReader_MovesTheOffsetBackBeforeTheSeekOffsetOnCloseWhileAReadIsInFlight(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(10)))
	nextInBackground(reader)
	assert.Equal(t, iggcon.POLLING_NEXT, client.nextRead(t).targets[0].Strategy.Kind)
	// The server stored the read in flight, whose reply Close cancels.
	client.mu.Lock()
	client.offsets[1] = 11
	client.mu.Unlock()

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{1, 9}, {1, 9}}, client.recordedStores())
}

// There is no offset before 0, and the other strategies name no offset, so
// such a seek stores nothing under OnRead.
func TestPartitionReader_StoresNothingForASeekThatNamesNoOffsetAboveZeroUnderOnRead(t *testing.T) {
	for _, start := range []iggcon.PollingStrategy{
		iggcon.FirstPollingStrategy(), iggcon.OffsetPollingStrategy(0), iggcon.LastPollingStrategy(), iggcon.NextPollingStrategy(),
	} {
		t.Run(fmt.Sprintf("%v", start.Kind), func(t *testing.T) {
			client := newFakeClient()
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			require.NoError(t, reader.Seek(context.Background(), 1, start))
			nextInBackground(reader)

			read := client.nextRead(t)
			kind := start.Kind
			switch kind {
			case iggcon.POLLING_FIRST:
				kind = iggcon.POLLING_OFFSET
			case iggcon.POLLING_LAST:
				// A fetch reads only from an offset or Next, so a poll that stores nothing finds the offset of Last.
				require.True(t, read.poll)
				require.False(t, read.commit)
				require.Equal(t, iggcon.LastPollingStrategy(), read.targets[0].Strategy)
				read.answer(section(1, 3))
				read = client.nextRead(t)
				kind = iggcon.POLLING_OFFSET
			}
			assert.Equal(t, kind, read.targets[0].Strategy.Kind)
			assert.True(t, read.commit)
			assert.Empty(t, client.recordedStores())
		})
	}
}

// Under OnRead a read never moves the stored offset back, so a seek to offset
// 0 deletes the stored offset, and the reads replay the whole partition.
func TestPartitionReader_ReplaysThePartitionAfterASeekToOffsetZeroUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	readPartition := func() {
		t.Helper()
		for _, batch := range [][]uint64{{0, 1}, {2, 3}} {
			polled := nextInBackground(reader)
			require.Equal(t, batch, client.serve(client.nextRead(t)), "the server reads on after its stored offset")
			got := <-polled
			require.NoError(t, got.err)
			assert.Equal(t, batch, offsets(got.polled))
		}
	}
	readPartition()

	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(0)))
	readPartition()

	assert.Equal(t, []uint32{1}, client.lookups)
	assert.Equal(t, []uint32{1}, client.deleted)
}

// The server refuses to delete a missing offset, so a seek to offset 0 of a
// partition without a stored offset deletes nothing.
func TestPartitionReader_DeletesNoMissingOffsetForASeekToOffsetZeroUnderOnRead(t *testing.T) {
	for _, start := range []iggcon.PollingStrategy{iggcon.FirstPollingStrategy(), iggcon.OffsetPollingStrategy(0)} {
		t.Run(fmt.Sprintf("%v", start.Kind), func(t *testing.T) {
			client := newFakeClient()
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)

			require.NoError(t, reader.Seek(context.Background(), 1, start))

			assert.Equal(t, []uint32{1}, client.lookups)
			assert.Empty(t, client.deleted)
		})
	}
}

func TestPartitionReader_FailsASeekToOffsetZeroWhoseDeleteFailsAndKeepsTheStoredOffsetUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.failedDeletes = 1
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.serve(client.nextRead(t))
	require.NoError(t, (<-polled).err)

	require.ErrorIs(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(0)), errDeleteFailed)

	assert.Equal(t, map[uint32]uint64{1: servedPerRead - 1}, client.offsets)
}

// Under OnRead a seek holds back the reads of its partition until its store
// ends, so no read of the old position reaches the server before the store.
// The first read after the seek starts at the seek point.
func TestPartitionReader_StartsNoReadOfAPartitionWhileItsSeekStoresUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.storeStarted = make(chan context.Context, 4)
	client.storeRelease = make(chan struct{})
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	require.Equal(t, []uint64{0, 1}, client.serve(client.nextRead(t)))
	require.NoError(t, (<-polled).err)
	sought := make(chan error, 1)
	go func() { sought <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(3)) }()
	select {
	case <-client.storeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the seek stored nothing")
	}

	polled = nextInBackground(reader)

	assert.True(t, client.readsNothingFor(300*time.Millisecond), "no read goes out during the store")
	close(client.storeRelease)
	require.NoError(t, <-sought)
	read := client.nextRead(t)
	assert.Equal(t, iggcon.POLLING_NEXT, read.targets[0].Strategy.Kind)
	assert.Equal(t, []uint64{3}, client.serve(read), "the first read starts at the seek point")
	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{3}, offsets(result.polled))
}

// Under OnRead a seek moves only after its write. A failed write leaves the
// partition as it was: its queued batch stays, and the reads go on from the old
// position, so no batch is lost.
func TestPartitionReader_KeepsThePositionAndTheQueueOfAPartitionWhoseSeekWriteFailsUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name  string
		start uint64
		err   error
	}{
		{"store", 10, errStoreFailed},
		{"delete", 0, errDeleteFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			polled := nextInBackground(reader)
			require.Equal(t, []uint64{0, 1}, client.serve(client.nextRead(t)))
			require.NoError(t, (<-polled).err)
			ctx, cancel := context.WithCancel(context.Background())
			canceled := make(chan error, 1)
			go func() {
				_, err := reader.Next(ctx)
				canceled <- err
			}()
			read := client.nextRead(t)
			cancel()
			require.ErrorIs(t, <-canceled, context.Canceled)
			require.Equal(t, []uint64{2, 3}, client.serve(read))
			require.Eventually(t, func() bool { return queued(reader) == 1 }, testTimeout, 5*time.Millisecond)
			client.mu.Lock()
			if test.start == 0 {
				client.failedDeletes = 1
			} else {
				client.failedStores = 1
			}
			client.mu.Unlock()

			require.ErrorIs(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(test.start)), test.err)

			require.Equal(t, 1, queued(reader), "the failed seek keeps the queued batch")
			assert.Equal(t, []uint64{2, 3}, offsets(next(t, reader)))
			polled = nextInBackground(reader)
			read = client.nextRead(t)
			assert.Equal(t, iggcon.POLLING_NEXT, read.targets[0].Strategy.Kind)
			read.answer(section(1, 4, 5))
			result := <-polled
			require.NoError(t, result.err)
			assert.Equal(t, []uint64{4, 5}, offsets(result.polled), "the reads go on from the old position")
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

// Under OnRead a stored offset wins over the start, so a partition with one
// resumes after it as with a Next start and reads nothing from the start.
func TestPartitionReader_ResumesAfterTheStoredOffsetInsteadOfTheStartUnderOnRead(t *testing.T) {
	const stored = 5
	for _, start := range []iggcon.PollingStrategy{
		iggcon.FirstPollingStrategy(), iggcon.OffsetPollingStrategy(0), iggcon.TimestampPollingStrategy(10), iggcon.LastPollingStrategy(),
	} {
		t.Run(fmt.Sprintf("%v", start.Kind), func(t *testing.T) {
			client := newFakeClient()
			client.offsets[1] = stored
			reader := onReadFrom(t, client, start)
			polled := nextInBackground(reader)

			read := client.nextRead(t)

			assert.True(t, read.commit)
			assert.Equal(t, iggcon.POLLING_NEXT, read.targets[0].Strategy.Kind, "no read starts at the start")
			assert.Equal(t, []uint32{1}, client.recordedProbes(), "the partition reads as with a Next start")
			read.answer(section(1, stored+1, stored+2))
			result := <-polled
			require.NoError(t, result.err)
			assert.Equal(t, []uint64{stored + 1, stored + 2}, offsets(result.polled))
		})
	}
}

func TestPartitionReader_ReadsFromTheStartWithoutAStoredOffsetUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := onReadFrom(t, client, iggcon.OffsetPollingStrategy(2))
	polled := nextInBackground(reader)

	read := client.nextRead(t)

	assert.Equal(t, []uint32{1}, client.lookups, "the first committing read looks up the stored offset first")
	assert.Equal(t, iggcon.OffsetPollingStrategy(2), read.targets[0].Strategy)
	assert.Equal(t, []uint64{2, 3}, client.serve(read))
	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{2, 3}, offsets(result.polled))
}

// A failed lookup of the stored offset takes the path of a failed read of its
// partition: Next reports it, and the reader looks the offset up again after
// the retry interval.
func TestPartitionReader_ReportsAFailedLookupAsAFailedReadAndLooksUpAgainAfterTheRetryIntervalUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.offsets[1] = 5
	client.failedLookups = 1
	reader := onReadFrom(t, client, iggcon.OffsetPollingStrategy(0))
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	_, err := reader.Next(ctx)

	var readError *ReadError
	require.ErrorAs(t, err, &readError)
	require.ErrorIs(t, err, errLookupFailed)
	nextInBackground(reader)
	read := client.nextRead(t)
	assert.GreaterOrEqual(t, time.Since(started), testRetryInterval)
	assert.Equal(t, iggcon.POLLING_NEXT, read.targets[0].Strategy.Kind)
	assert.Equal(t, []uint32{1, 1}, client.lookups)
}

// The start lookups of a committing read count as reads of the reader, as its probes do.
func TestPartitionReader_RunsAtMost32StartLookupsAtOnceUnderOnRead(t *testing.T) {
	const maxReads = 32
	client := newFakeClient()
	partitions := make([]uint32, maxReads+1)
	for index := range partitions {
		partitions[index] = uint32(index + 1)
	}
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(partitions...), WithStart(iggcon.OffsetPollingStrategy(0)),
		WithPollOptions(withCommit(waitOneSecond, iggcon.OnReadCommit())), WithRetryInterval(testRetryInterval))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	nextInBackground(reader)

	read := client.nextRead(t)

	assert.Len(t, client.lookups, maxReads, "the lookups of one read take at most 32 requests")
	assert.Equal(t, partitions[:maxReads], partitionsOf(read), "a partition without a lookup waits for a later read")
}

// A member takes the start at each assignment, so under OnRead it looks up
// the stored offset each time and resumes after it. A refusal as not the owner
// keeps the position, which under OnRead resumes after the stored offset too.
func TestPartitionReader_ResumesAMemberAfterTheStoredOffsetAtEachAssignmentUnderOnRead(t *testing.T) {
	for _, change := range []string{"reassignment", "not_owned"} {
		t.Run(change, func(t *testing.T) {
			client := newFakeClient()
			client.offsets[1] = 5
			client.assignments <- []uint32{1}
			reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
				iggcon.NewGroupConsumer(identifier(t, 9)), WithStart(iggcon.OffsetPollingStrategy(0)),
				WithPollOptions(withCommit(waitOneSecond, iggcon.OnReadCommit())), WithRetryInterval(testRetryInterval))
			require.NoError(t, err)
			t.Cleanup(func() { _ = reader.Close(context.Background()) })
			resumes := func(polled <-chan outcome, first uint64) {
				t.Helper()
				read := client.nextRead(t)
				require.Equal(t, iggcon.POLLING_NEXT, read.targets[0].Strategy.Kind, "the partition resumes after its stored offset")
				read.answer(section(1, first, first+1))
				result := <-polled
				require.NoError(t, result.err)
				assert.Equal(t, []uint64{first, first + 1}, offsets(result.polled))
			}
			resumes(nextInBackground(reader), 6)

			if change == "reassignment" {
				assign(t, client, reader, 2)
				assign(t, client, reader, 1)
				resumes(nextInBackground(reader), 8)
			} else {
				polled := nextInBackground(reader)
				client.nextRead(t).answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.ErrConsumerGroupPartitionNotOwned})
				resumes(polled, 8)
			}
		})
	}
}

// Under OnRead a partition that delivered none of its read-ahead moves its
// offset before the first undelivered batch on Close, for every start. At
// offset 0 Close deletes the offset that the read stored.
func TestPartitionReader_MovesTheOffsetBeforeTheFirstUndeliveredBatchOnCloseForEveryStartUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name    string
		start   iggcon.PollingStrategy
		first   uint64
		stores  []store
		deleted []uint32
	}{
		{"offset", iggcon.OffsetPollingStrategy(2), 2, []store{{1, 1}}, nil},
		{"timestamp", iggcon.TimestampPollingStrategy(10), 2, []store{{1, 1}}, nil},
		{"last", iggcon.LastPollingStrategy(), 500, []store{{1, 499}}, nil},
		{"offset_zero", iggcon.OffsetPollingStrategy(0), 0, nil, []uint32{1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			reader := onReadFrom(t, client, test.start)
			ctx, cancel := context.WithCancel(context.Background())
			polled := make(chan error, 1)
			go func() {
				_, err := reader.Next(ctx)
				polled <- err
			}()
			read := findsTheStart(t, client, test.first)
			cancel()
			require.ErrorIs(t, <-polled, context.Canceled)
			read.answer(section(1, test.first, test.first+1))
			require.Eventually(t, func() bool { return queued(reader) == 1 }, testTimeout, 5*time.Millisecond)

			require.NoError(t, reader.Close(context.Background()))

			assert.Equal(t, test.stores, client.recordedStores())
			assert.Equal(t, test.deleted, client.deleted)
		})
	}
}

// Under OnRead the first read of an offset can store before its reply
// arrives, so Close moves the offset back before that offset, also after a
// seek to offset 0.
func TestPartitionReader_MovesTheOffsetBeforeAFirstReadInFlightAtAnOffsetOnCloseUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name    string
		seek    bool
		offset  uint64
		stores  []store
		deleted []uint32
	}{
		{"start_offset_zero", false, 0, nil, []uint32{1}},
		{"start_offset_five", false, 5, []store{{1, 4}}, nil},
		{"seek_to_offset_zero", true, 0, nil, []uint32{1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			atOffset := iggcon.OffsetPollingStrategy(test.offset)
			start := atOffset
			if test.seek {
				start = iggcon.NextPollingStrategy()
			}
			reader := onReadFrom(t, client, start)
			if test.seek {
				require.NoError(t, reader.Seek(context.Background(), 1, atOffset))
			}
			polled := nextInBackground(reader)
			require.Equal(t, atOffset, client.nextRead(t).targets[0].Strategy)
			// The server stored the read in flight, whose reply Close cancels.
			client.mu.Lock()
			client.offsets[1] = test.offset + 1
			client.mu.Unlock()

			require.NoError(t, reader.Close(context.Background()))

			require.Error(t, (<-polled).err)
			assert.Equal(t, test.stores, client.recordedStores())
			assert.Equal(t, test.deleted, client.deleted)
		})
	}
}

// A poll finds the offset of a timestamp or Last start, so the first read
// starts at that offset. When that read lost its reply, or has none at Close,
// and no batch arrived, Close moves the offset before the found offset.
func TestPartitionReader_MovesTheOffsetBeforeTheFoundOffsetOfALostFirstReadAtATimestampOrLastStartOnCloseUnderOnRead(t *testing.T) {
	const found uint64 = 5
	for _, start := range []iggcon.PollingStrategy{iggcon.TimestampPollingStrategy(10), iggcon.LastPollingStrategy()} {
		for _, lost := range []bool{true, false} {
			t.Run(fmt.Sprintf("%v/lost_%t", start.Kind, lost), func(t *testing.T) {
				client := newFakeClient()
				reader := onReadFrom(t, client, start)
				polled := nextInBackground(reader)
				read := findsTheStart(t, client, found)
				// The server stored the read whose reply is lost or has not come.
				client.mu.Lock()
				client.offsets[1] = 7
				client.mu.Unlock()
				if lost {
					read.reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
					require.Eventually(t, func() bool { return len(client.recordedAbandons()) == 1 }, testTimeout, 5*time.Millisecond)
				}

				require.NoError(t, reader.Close(context.Background()))

				require.Error(t, (<-polled).err)
				assert.Equal(t, []store{{1, found - 1}}, client.recordedStores())
				assert.Empty(t, client.deleted)
			})
		}
	}
}

// A start or a seek can name an offset past the end of the partition, where a
// read stores nothing, and the server refuses a store past the end. So without
// a known batch, Close looks up the stored offset and moves it back only over
// what a read stored.
func TestPartitionReader_MovesTheOffsetBackOnlyOverWhatAReadFromANamedOffsetStoredOnCloseUnderOnRead(t *testing.T) {
	const named uint64 = 10
	for _, test := range []struct {
		name string
		seek bool
		last uint64
		// lost loses the reply of the first read, and stored makes that read store.
		lost, stored bool
		stores       []store
		lookups      []uint32
	}{
		{"start_past_the_end/empty_read", false, 3, false, false, nil, []uint32{1, 1}},
		{"start_past_the_end/lost_read", false, 3, true, false, nil, []uint32{1, 1}},
		{"start/lost_read_that_stored", false, 12, true, true, []store{{1, named - 1}}, []uint32{1, 1}},
		// The seek looks up the stored offset too, since a Next start names no offset before its first read.
		{"seek/lost_read", true, 12, true, false, []store{{1, named - 1}}, []uint32{1, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			client.lastOffsets[1] = test.last
			start := iggcon.OffsetPollingStrategy(named)
			if test.seek {
				start = iggcon.NextPollingStrategy()
			}
			reader := onReadFrom(t, client, start)
			if test.seek {
				require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(named)))
			}
			polled := nextInBackground(reader)
			read := client.nextRead(t)
			if test.stored {
				client.mu.Lock()
				client.offsets[1] = named + 1
				client.mu.Unlock()
			}
			if test.lost {
				read.reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
				require.Eventually(t, func() bool { return len(client.recordedAbandons()) == 1 }, testTimeout, 5*time.Millisecond)
			} else {
				read.answer(iggcon.FetchMessagesSection{PartitionId: 1, Context: testContext, CurrentOffset: test.last, NextOffset: named})
				// The reader read the empty reply when it reads again.
				client.nextRead(t)
			}

			require.NoError(t, reader.Close(context.Background()))

			require.Error(t, (<-polled).err)
			assert.Equal(t, test.stores, client.recordedStores())
			assert.Empty(t, client.deleted)
			assert.Equal(t, test.lookups, client.lookups, "Close looks up the stored offset before it moves it")
		})
	}
}

// A first batch that ends during Close is known, so a partition at a Last
// start moves its offset before it instead of deleting the offset.
func TestPartitionReader_MovesTheOffsetBeforeAFirstBatchThatEndsDuringCloseAtALastStartUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.holdsCommits = true
	reader := onReadFrom(t, client, iggcon.LastPollingStrategy())
	nextInBackground(reader)
	parked := findsTheStart(t, client, 500)
	closed := make(chan error, 1)
	go func() { closed <- reader.Close(context.Background()) }()
	require.Eventually(t, func() bool { return reader.run.Err() != nil }, testTimeout, time.Millisecond)

	parked.answer(section(1, 500, 501))

	require.NoError(t, <-closed)
	assert.Equal(t, []store{{1, 499}}, client.recordedStores())
	assert.Empty(t, client.deleted)
}

// The server refuses to delete a missing offset, so a Close that moves the
// offset before offset 0 deletes only an offset that exists.
func TestPartitionReader_DeletesNoMissingOffsetWhenItMovesTheOffsetBeforeOffsetZeroOnCloseUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t)

	require.NoError(t, reader.Close(context.Background()), "the read in flight stored nothing")

	require.Error(t, (<-polled).err)
	assert.Equal(t, []uint32{1}, client.lookups)
	assert.Empty(t, client.deleted)
}

// Under OnRead a committing read at a timestamp behind the stored offset would
// jump to it, so a seek to a timestamp moves the stored offset before the
// first message there, and the reader reads on from it to the end.
func TestPartitionReader_ReadsFromTheTimestampOfASeekBehindTheStoredOffsetToTheEndUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	readOn := func(batches ...[]uint64) {
		t.Helper()
		for _, batch := range batches {
			polled := nextInBackground(reader)
			require.Equal(t, batch, client.serve(client.nextRead(t)))
			result := <-polled
			require.NoError(t, result.err)
			assert.Equal(t, batch, offsets(result.polled))
		}
	}
	readOn([]uint64{0, 1}, []uint64{2, 3})
	sought := make(chan error, 1)
	go func() { sought <- reader.Seek(context.Background(), 1, iggcon.TimestampPollingStrategy(10)) }()

	probe := client.nextRead(t)
	require.True(t, probe.poll, "a fetch reads only from an offset or Next")
	require.False(t, probe.commit)
	require.Equal(t, iggcon.TimestampPollingStrategy(10), probe.targets[0].Strategy)
	probe.answer(section(1, 1))
	require.NoError(t, <-sought)

	readOn([]uint64{1, 2}, []uint64{3})
	assert.Equal(t, []store{{1, 0}}, client.recordedStores())
}

// A seek to a timestamp without a message there stores nothing and reads at
// the timestamp, since a message that arrives later lies after the stored
// offset. A poll that stores nothing reads there, since a fetch reads only
// from an offset or Next. A failed read of the timestamp fails the seek and
// moves nothing.
func TestPartitionReader_StoresNothingForASeekToATimestampWithoutAMessageOrWhoseReadFailsUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name   string
		reply  fakeReply
		err    error
		read   iggcon.MessagePolling
		commit bool
	}{
		{"no_message", fakeReply{sections: []iggcon.FetchMessagesSection{{PartitionId: 1, Context: testContext}}}, nil, iggcon.POLLING_TIMESTAMP, false},
		{"failed_read", fakeReply{err: errProbeFailed}, errProbeFailed, iggcon.POLLING_NEXT, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			sought := make(chan error, 1)
			go func() { sought <- reader.Seek(context.Background(), 1, iggcon.TimestampPollingStrategy(10)) }()
			probe := client.nextRead(t)
			require.True(t, probe.poll)
			require.False(t, probe.commit)
			probe.reply <- test.reply
			require.ErrorIs(t, <-sought, test.err)
			nextInBackground(reader)

			read := client.nextRead(t)

			assert.Equal(t, test.commit, read.commit)
			assert.Equal(t, !test.commit, read.poll)
			assert.Equal(t, test.read, read.targets[0].Strategy.Kind)
			assert.Empty(t, client.recordedStores())
		})
	}
}

// The server reads a fetch only from an offset or Next. So a poll that stores
// nothing finds the first message at a timestamp or Last position, and the
// fetches read on from its offset. Without a message the partition polls again.
func TestPartitionReader_FindsTheOffsetOfATimestampOrLastPositionWithAPollBeforeItFetches(t *testing.T) {
	for _, start := range []iggcon.PollingStrategy{iggcon.TimestampPollingStrategy(10), iggcon.LastPollingStrategy()} {
		t.Run(fmt.Sprintf("%v", start.Kind), func(t *testing.T) {
			client := newFakeClient()
			reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
				iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(1), WithStart(start),
				WithPollOptions(waitOneSecond), WithRetryInterval(testRetryInterval))
			require.NoError(t, err)
			t.Cleanup(func() { _ = reader.Close(context.Background()) })
			for _, found := range []bool{false, true} {
				poll := client.nextRead(t)
				require.True(t, poll.poll)
				require.False(t, poll.commit)
				require.Equal(t, start, poll.targets[0].Strategy)
				// A timestamp needs one message, and a Last start delivers the last Count messages.
				count := uint32(1)
				if start.Kind == iggcon.POLLING_LAST {
					count = waitOneSecond.Count
				}
				require.Equal(t, count, poll.count)
				if found {
					poll.answer(section(1, 2))
				} else {
					poll.answer(iggcon.FetchMessagesSection{PartitionId: 1, Context: testContext})
				}
			}

			read := client.nextRead(t)

			assert.False(t, read.poll)
			assert.Equal(t, iggcon.OffsetPollingStrategy(2).WithContext(testContext), read.targets[0].Strategy)
			assert.Equal(t, []uint64{2, 3}, client.serve(read))
			assert.Equal(t, []uint64{2, 3}, offsets(next(t, reader)))
		})
	}
}

// A lost read blocks the offset writes of its partition until the reader
// abandons it, so a seek that waits for the read stores only after the abandon.
func TestPartitionReader_StoresASeekOnlyAfterTheAbandonOfALostReadThatItWaitsForUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.abandonRelease = make(chan struct{})
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	require.Equal(t, []uint64{0, 1}, client.serve(client.nextRead(t)))
	require.NoError(t, (<-polled).err)
	nextInBackground(reader)
	inFlight := client.nextRead(t)
	sought := make(chan error, 1)
	go func() { sought <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(10)) }()

	inFlight.answer(iggcon.FetchMessagesSection{PartitionId: 1, Err: ierror.FetchProgressUncertain{OperationID: 42}})

	assert.Never(t, func() bool { return len(sought) > 0 }, 300*time.Millisecond, 5*time.Millisecond,
		"the seek waits for the abandon of the lost read")
	close(client.abandonRelease)
	require.NoError(t, <-sought)
	assert.Equal(t, []store{{1, 9}}, client.recordedStores())
	assert.Equal(t, []uint64{42}, client.recordedAbandons())
}

// A seek whose caller stops waiting after the store was sent returns at once.
// The seek still holds the reads until its store ends, and then moves.
func TestPartitionReader_FinishesASeekWhoseCallerStoppedWaitingAfterItsStoreWasSentUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.storeStarted = make(chan context.Context, 4)
	client.storeRelease = make(chan struct{})
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	require.Equal(t, []uint64{0, 1}, client.serve(client.nextRead(t)))
	require.NoError(t, (<-polled).err)
	ctx, cancel := context.WithCancel(context.Background())
	sought := make(chan error, 1)
	go func() { sought <- reader.Seek(ctx, 1, iggcon.OffsetPollingStrategy(3)) }()
	select {
	case <-client.storeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the seek stored nothing")
	}

	cancel()

	require.ErrorIs(t, <-sought, context.Canceled)
	polled = nextInBackground(reader)
	assert.True(t, client.readsNothingFor(300*time.Millisecond), "the seek holds the reads until its store ends")
	close(client.storeRelease)
	assert.Equal(t, []uint64{3}, client.serve(client.nextRead(t)), "the seek moved after its store")
	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{3}, offsets(result.polled))
	assert.Equal(t, []store{{1, 2}}, client.recordedStores())
}

// A seek store with an unknown outcome fails the seek and keeps the position.
// The reader abandons the store, so a later seek and Close still write, and it
// reads again what the store made the next read skip.
func TestPartitionReader_ReadsAgainWhatASeekStoreWithAnUnknownOutcomeSkippedUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.uncertainStores = 1
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	require.Equal(t, []uint64{0, 1}, client.serve(client.nextRead(t)))
	require.NoError(t, (<-polled).err)

	require.ErrorIs(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(3)), ierror.OperationUncertain{})

	assert.Equal(t, []uint64{uncertainOperation}, client.recordedAbandons(), "the reader releases the offset writes of the partition")
	polled = nextInBackground(reader)
	read := client.nextRead(t)
	require.True(t, read.commit)
	require.Equal(t, []uint64{3}, client.serve(read), "the store moved the stored offset")
	gap := client.nextRead(t)
	require.False(t, gap.commit)
	assert.Equal(t, iggcon.OffsetPollingStrategy(2).WithContext(testContext), gap.targets[0].Strategy)
	gap.answer(section(1, 2))
	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{2}, offsets(result.polled), "the skipped message comes first")
	assert.Equal(t, []uint64{3}, offsets(next(t, reader)))
	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(1)))
	require.NoError(t, reader.Close(context.Background()))
	assert.Equal(t, []store{{1, 2}, {1, 0}}, client.recordedStores())
}

// A seek waits for the store of an earlier seek of its partition only as long
// as its context lasts.
func TestPartitionReader_EndsTheWaitOfASeekForTheStoreOfAnEarlierSeekOfItsPartitionWithItsContextUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.storeStarted = make(chan context.Context, 4)
	client.storeRelease = make(chan struct{})
	released := false
	release := func() {
		if !released {
			released = true
			close(client.storeRelease)
		}
	}
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	// Before the Close of the cleanup, so that it finds the first store released.
	t.Cleanup(release)
	first := make(chan error, 1)
	go func() { first <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(3)) }()
	select {
	case <-client.storeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the first seek stored nothing")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)

	go func() { second <- reader.Seek(ctx, 1, iggcon.OffsetPollingStrategy(5)) }()

	select {
	case err := <-second:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(testTimeout):
		t.Fatal("the second seek waits past the end of its context")
	}
	release()
	require.NoError(t, <-first)
	assert.Equal(t, []store{{1, 2}}, client.recordedStores(), "the second seek stores nothing")
}

// A seek of one partition does not wait for the seek of another one, whose
// store the server refused as not the owner and whose read for the installed
// owner the server keeps refusing for now.
func TestPartitionReader_SeeksAPartitionWhileASeekOfAnotherOneRetriesItsReadForTheInstalledOwnerUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.refusedProbes = map[uint32][]error{1: keptRefusals}
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	require.Eventually(t, func() bool { return slices.Equal(reader.Partitions(), []uint32{1, 2}) }, testTimeout,
		5*time.Millisecond, "the member holds the partitions")
	installsTheOwnerAgain(client)
	first := make(chan error, 1)
	go func() { first <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(5)) }()
	require.Eventually(t, func() bool { return len(client.recordedProbes()) > 0 }, testTimeout, 5*time.Millisecond,
		"the read for the installed owner is refused")
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	require.NoError(t, reader.Seek(ctx, 2, iggcon.OffsetPollingStrategy(3)))

	assert.Equal(t, []store{{2, 2}}, client.recordedStores())
	require.NoError(t, reader.Close(context.Background()))
	require.ErrorIs(t, <-first, ierror.ErrConsumerGroupPartitionNotOwned, "Close ends the retry of the first seek")
}

// A seek holds every read of its partition, also the read of what a lost reply
// stored, which reads the partition alone.
func TestPartitionReader_StartsNoReadOfALostRangeWhileASeekOfItsPartitionStoresUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.storeStarted = make(chan context.Context, 4)
	client.storeRelease = make(chan struct{})
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	require.Equal(t, []uint64{0, 1}, client.serve(client.nextRead(t)))
	require.NoError(t, (<-polled).err)
	polled = nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	// The lost read stored up to 3, so the next read jumps past the lost range from 2.
	client.mu.Lock()
	client.offsets[1] = 3
	client.mu.Unlock()
	jumped := client.nextRead(t)
	sought := make(chan error, 1)
	go func() { sought <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(5)) }()
	require.Eventually(t, func() bool {
		reader.mu.Lock()
		defer reader.mu.Unlock()
		return reader.seeking[1] > 0
	}, testTimeout, time.Millisecond)

	jumped.answer(section(1, 4, 5))

	select {
	case <-client.storeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the seek stored nothing")
	}
	assert.True(t, client.readsNothingFor(300*time.Millisecond), "no read of the partition goes out during the store")
	close(client.storeRelease)
	require.NoError(t, <-sought)
	read := client.nextRead(t)
	assert.True(t, read.commit)
	assert.Equal(t, iggcon.POLLING_NEXT, read.targets[0].Strategy.Kind)
	read.answer(section(1, 5))
	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{5}, offsets(result.polled), "the first read starts at the seek point")
}

// A Last start delivers the last Count messages of its partition under every
// commit policy, so the poll that finds its offset asks for Count messages.
func TestPartitionReader_DeliversTheLastCountMessagesOfALastStartUnderEveryCommitPolicy(t *testing.T) {
	const count, last = 3, 9
	for _, commit := range []iggcon.CommitPolicy{iggcon.ManualCommit(), iggcon.OnReadCommit()} {
		t.Run(fmt.Sprintf("%v", commit.Kind), func(t *testing.T) {
			client := newFakeClient()
			options := withCommit(waitOneSecond, commit)
			options.Count = count
			reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
				iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(1), WithStart(iggcon.LastPollingStrategy()),
				WithPollOptions(options), WithRetryInterval(testRetryInterval))
			require.NoError(t, err)
			t.Cleanup(func() { _ = reader.Close(context.Background()) })
			// As the server does, a read returns the messages from first up to the last one, at most its count.
			answer := func(read *fakeRead, first uint64) {
				var found []uint64
				for offset := first; offset <= last && len(found) < int(read.count); offset++ {
					found = append(found, offset)
				}
				read.answer(section(1, found...))
			}
			polled := nextInBackground(reader)

			poll := client.nextRead(t)
			require.True(t, poll.poll)
			answer(poll, last+1-min(uint64(poll.count), last+1))
			read := client.nextRead(t)
			require.Equal(t, iggcon.POLLING_OFFSET, read.targets[0].Strategy.Kind)
			answer(read, read.targets[0].Strategy.Value)

			result := <-polled
			require.NoError(t, result.err)
			assert.Equal(t, []uint64{7, 8, 9}, offsets(result.polled), "a Last start delivers the last Count messages")
		})
	}
}

// A seek before the first read of a partition whose position follows the
// stored offset looks that offset up before its store. After a store with an
// unknown outcome, the partition reads on after the offset from before the
// store. No read skips a message, also after a first read from an offset,
// which shows no jump of the stored offset.
func TestPartitionReader_ReadsOnAfterTheOffsetStoredBeforeASeekStoreWithAnUnknownOutcomeUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name    string
		start   iggcon.PollingStrategy
		stored  bool
		batches [][]uint64
	}{
		{"next start after stored offset 0", iggcon.NextPollingStrategy(), true, [][]uint64{{1, 2}, {3}}},
		{"offset start without a stored offset", iggcon.OffsetPollingStrategy(0), false, [][]uint64{{0, 1}, {2}, {3}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			if test.stored {
				client.offsets[1] = 0
			}
			client.uncertainStores = 1
			reader := onReadFrom(t, client, test.start)

			require.ErrorIs(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(3)), ierror.OperationUncertain{})

			for _, batch := range test.batches {
				polled := nextInBackground(reader)
				var result outcome
				for received := false; !received; {
					select {
					case result = <-polled:
						received = true
					case read := <-client.reads:
						client.serve(read)
					}
				}
				require.NoError(t, result.err)
				require.Equal(t, batch, offsets(result.polled), "no read skips a message that the store passed over")
			}
		})
	}
}

// After a seek store with an unknown outcome before the first read, Close
// moves the offset back before the offset where the partition reads on, as
// after a lost read from there.
func TestPartitionReader_MovesTheOffsetBackBeforeTheReadPositionAfterASeekStoreWithAnUnknownOutcomeOnCloseUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name     string
		stored   bool
		seekZero bool
		stores   []store
		deleted  []uint32
	}{
		{"next start after stored offset 0", true, false, []store{{1, 2}, {1, 0}}, nil},
		{"offset 0 after a seek to it", false, true, []store{{1, 2}}, []uint32{1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			if test.stored {
				client.offsets[1] = 0
			}
			reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
			if test.seekZero {
				require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(0)))
			}
			client.mu.Lock()
			client.uncertainStores = 1
			client.mu.Unlock()
			require.ErrorIs(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(3)), ierror.OperationUncertain{})

			require.NoError(t, reader.Close(context.Background()))

			assert.Equal(t, test.stores, client.recordedStores())
			assert.Equal(t, test.deleted, client.deleted, "Close restores the stored offset from before the seek")
		})
	}
}

// A rewind store of Close with an unknown outcome fails Close. The reader
// abandons the store, so a later reader on the same client can still write the
// offset of the partition.
func TestPartitionReader_AbandonsARewindStoreWithAnUnknownOutcomeOnCloseUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	require.Eventually(t, func() bool { return len(client.recordedAbandons()) == 1 }, testTimeout, 5*time.Millisecond)
	client.mu.Lock()
	client.uncertainStores = 1
	client.mu.Unlock()

	require.ErrorIs(t, reader.Close(context.Background()), ierror.OperationUncertain{})

	assert.Equal(t, []uint64{42, uncertainOperation}, client.recordedAbandons())
	later := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	require.NoError(t, later.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(1)), "no write of the closed reader blocks the partition")
}

// An automatic store with an unknown outcome blocks the later offset writes of
// its partition on the client. The reader abandons it, in the background and
// in Close, so the next store of the partition goes out.
func TestPartitionReader_AbandonsAnAutomaticStoreWithAnUnknownOutcomeInTheBackgroundAndOnClose(t *testing.T) {
	client := newFakeClient()
	client.uncertainStores = 2
	auto := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryBatch}, After: iggcon.CommitAfterDelivery}
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.AutoCommitPolicy(auto)), 1)
	client.nextRead(t).answer(section(1, 0, 1))
	next(t, reader)
	require.Eventually(t, func() bool { return len(client.recordedAbandons()) == 1 }, testTimeout, 5*time.Millisecond,
		"the reader abandons the automatic store")

	require.ErrorIs(t, reader.Close(context.Background()), ierror.OperationUncertain{})

	assert.Equal(t, []store{{1, 1}, {1, 1}}, client.recordedStores(), "Close stores the counted position again")
	assert.Equal(t, []uint64{uncertainOperation, uncertainOperation}, client.recordedAbandons())
	later := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	require.NoError(t, later.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(1)), "no store of the closed reader blocks the partition")
}

// A seek store with an unknown outcome can store an offset at a start whose
// lookup found none. Until a batch of the partition arrived, Close deletes that
// offset, also after a poll found the offset of the start or a read from it
// stored nothing, so a restart applies the start again. A batch that arrived
// still decides the rewind.
func TestPartitionReader_DeletesTheOffsetOfASeekStoreWithAnUnknownOutcomeAtAStartWithoutAStoredOffsetOnCloseUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name  string
		start iggcon.PollingStrategy
		// lookUpFirst runs the start lookup and the first poll or read before
		// the seek, and no demand follows them.
		lookUpFirst bool
		// first answers that poll or read with these offsets. Without any, it finds nothing.
		first []uint64
		// found answers the poll after the seek with a message.
		found bool
		// stores are the stores of Close. Without any, Close deletes the offset.
		stores []store
	}{
		{"timestamp start that the seek looks up", iggcon.TimestampPollingStrategy(10), false, nil, false, nil},
		{"last start looked up before the seek", iggcon.LastPollingStrategy(), true, nil, false, nil},
		{"timestamp start whose poll finds its offset after the seek", iggcon.TimestampPollingStrategy(10), false, nil, true, nil},
		{"timestamp start whose poll found its offset before the seek", iggcon.TimestampPollingStrategy(10), true, []uint64{1}, false, nil},
		{"offset start that the seek looks up", iggcon.OffsetPollingStrategy(5), false, nil, false, nil},
		{"offset start whose first read stored nothing", iggcon.OffsetPollingStrategy(5), true, nil, false, nil},
		{"offset start whose first batch is queued", iggcon.OffsetPollingStrategy(5), true, []uint64{5, 6}, false, []store{{1, 4}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			reader := onReadFrom(t, client, test.start)
			if test.lookUpFirst {
				ctx, cancel := context.WithCancel(context.Background())
				go func() { _, _ = reader.Next(ctx) }()
				first := client.nextRead(t)
				cancel()
				require.Eventually(t, func() bool { return waiting(reader) == 0 }, testTimeout, time.Millisecond)
				if len(test.first) > 0 {
					first.answer(section(1, test.first...))
					require.Eventually(t, func() bool {
						reader.mu.Lock()
						defer reader.mu.Unlock()
						return reader.reads == 0
					}, testTimeout, 5*time.Millisecond)
				} else {
					first.reply <- fakeReply{}
				}
			}
			client.mu.Lock()
			client.uncertainStores = 1
			client.mu.Unlock()

			require.ErrorIs(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(3)), ierror.OperationUncertain{})
			if test.found {
				poll := client.nextRead(t)
				require.True(t, poll.poll)
				poll.answer(section(1, 1))
				require.Eventually(t, func() bool {
					reader.mu.Lock()
					defer reader.mu.Unlock()
					return reader.partitions[1].position.Kind == iggcon.POLLING_OFFSET
				}, testTimeout, 5*time.Millisecond)
			}

			require.NoError(t, reader.Close(context.Background()))

			assert.Equal(t, append([]store{{1, 2}}, test.stores...), client.recordedStores())
			if len(test.stores) > 0 {
				assert.Empty(t, client.deleted, "a batch that arrived decides the rewind")
			} else {
				assert.Equal(t, []uint32{1}, client.deleted, "Close restores the missing offset that the lookup found")
			}
		})
	}
}

// An error of a whole read of an earlier generation says nothing of the
// position that a seek set meanwhile. So it does not stop the partition, and
// the offset that the check poll of the seek found counts.
func TestPartitionReader_KeepsThePositionOfASeekAfterAnErrorOfAReadOfAnEarlierGeneration(t *testing.T) {
	for _, test := range []struct {
		name    string
		refusal error
	}{
		{"invalid size stops", ierror.ErrInvalidSizeBytes},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			reader := standalone(t, client, waitOneSecond, 1)
			earlier := client.nextRead(t)
			require.NoError(t, reader.Seek(context.Background(), 1, iggcon.TimestampPollingStrategy(20)))
			poll := client.nextRead(t)
			require.True(t, poll.poll)

			earlier.reply <- fakeReply{err: test.refusal}
			require.Eventually(t, func() bool {
				reader.mu.Lock()
				defer reader.mu.Unlock()
				return reader.reads == 1
			}, testTimeout, 5*time.Millisecond, "the refused read ends")
			poll.answer(section(1, 25))

			assert.Equal(t, iggcon.OffsetPollingStrategy(25).WithContext(testContext), client.nextRead(t).targets[0].Strategy,
				"the partition reads from the offset that the poll found")
		})
	}
}

// A failed read whose partitions all moved to a newer generation meanwhile
// says nothing of the reader, so the reader reports nothing for it.
func TestPartitionReader_ReportsNoErrorOfAReadWhosePartitionsAllMovedToANewerGeneration(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, waitOneSecond, 1)
	earlier := client.nextRead(t)
	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(5)))
	fresh := client.nextRead(t)
	require.Equal(t, iggcon.OffsetPollingStrategy(5), fresh.targets[0].Strategy)

	earlier.reply <- fakeReply{err: errReadFailed}
	require.Eventually(t, func() bool {
		reader.mu.Lock()
		defer reader.mu.Unlock()
		return reader.reads == 1
	}, testTimeout, 5*time.Millisecond, "the failed read ends")
	fresh.answer(section(1, 5, 6))

	assert.Equal(t, []uint64{5, 6}, offsets(next(t, reader)), "no error of the earlier read comes first")
}

// The refusal of a check poll of an earlier generation says nothing of the
// position that a seek set meanwhile, so it does not stop the partition.
func TestPartitionReader_DropsTheRefusalOfAPollOfAnEarlierGenerationUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := onReadFrom(t, client, iggcon.TimestampPollingStrategy(10))
	polled := nextInBackground(reader)
	earlier := client.nextRead(t)
	require.True(t, earlier.poll, "a lookup without a stored offset leaves the start to a poll")
	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(5)))
	fetch := client.nextRead(t)
	require.True(t, fetch.commit)

	earlier.reply <- fakeReply{err: ierror.ErrInvalidSizeBytes}
	require.Eventually(t, func() bool {
		reader.mu.Lock()
		defer reader.mu.Unlock()
		return reader.reads == 1
	}, testTimeout, 5*time.Millisecond, "the refused poll ends")
	fetch.answer(section(1, 5, 6))

	result := <-polled
	require.NoError(t, result.err)
	assert.Equal(t, []uint64{5, 6}, offsets(result.polled))
}

// The refusal of a read of an earlier generation says nothing of the partition
// after a seek. So it does not drop the owner that the probe of the seek gave.
func TestPartitionReader_KeepsTheProbeOfASeekAfterARefusalOfAReadOfAnEarlierGenerationUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.offsets[1] = 4
	reader := onReadFrom(t, client, iggcon.TimestampPollingStrategy(10))
	nextInBackground(reader)
	// A section that does not decode leaves a read of one message without a store in flight.
	client.nextRead(t).answer(sectionOf(1, 7, 2, undecodable(5), record(6)))
	earlier := client.nextRead(t)
	require.False(t, earlier.commit)
	client.mu.Lock()
	client.probeStarted = make(chan struct{}, 4)
	client.probeRelease = make(chan struct{})
	client.mu.Unlock()
	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.NextPollingStrategy()))
	select {
	case <-client.probeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the reader sent no probe")
	}

	earlier.reply <- fakeReply{err: ierror.ErrConsumerGroupPartitionNotOwned}
	require.Eventually(t, func() bool {
		reader.mu.Lock()
		defer reader.mu.Unlock()
		return reader.reads == 1
	}, testTimeout, 5*time.Millisecond, "the refused read ends")
	close(client.probeRelease)

	read := client.nextRead(t)
	assert.True(t, read.commit, "the partition reads after the probe of the seek")
	assert.Equal(t, iggcon.NextPollingStrategy().WithContext(testContext), read.targets[0].Strategy)
}

// A seek to First of a partition whose position names no offset looks the
// stored offset up before its delete. After a delete with an unknown outcome,
// Close stores that offset again, also when the delete removed it. So a
// restart neither applies the start nor replays the partition.
func TestPartitionReader_StoresTheOffsetFromBeforeASeekDeleteWithAnUnknownOutcomeAgainOnCloseUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name  string
		start iggcon.PollingStrategy
	}{
		{"offset start that the seek looks up", iggcon.OffsetPollingStrategy(100)},
		{"next start", iggcon.NextPollingStrategy()},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			client.offsets[1] = 10
			client.uncertainDeletes = 1
			reader := onReadFrom(t, client, test.start)

			require.ErrorIs(t, reader.Seek(context.Background(), 1, iggcon.FirstPollingStrategy()), ierror.OperationUncertain{})
			require.NoError(t, reader.Close(context.Background()))

			assert.Equal(t, []uint32{1}, client.deleted)
			assert.Equal(t, []store{{1, 10}}, client.recordedStores(), "Close stores the offset from before the delete again")
		})
	}
}

// No read came from a position that a check poll found, so that position is no
// close point. After a seek write with an unknown outcome, Close restores the
// stored offset from before the write, or deletes the offset without one. A
// check poll that ends during the write leaves the same close point.
func TestPartitionReader_RestoresTheStoredOffsetFromBeforeASeekWriteWithAnUnknownOutcomeAfterASeekToLastOnCloseUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name     string
		stored   bool
		seek     iggcon.PollingStrategy
		dropPoll bool
		stores   []store
		deleted  []uint32
	}{
		{"store after a found position", true, iggcon.OffsetPollingStrategy(20), false, []store{{1, 19}, {1, 50}}, nil},
		{"store without a stored offset", false, iggcon.OffsetPollingStrategy(20), false, []store{{1, 19}}, []uint32{1}},
		{"delete after a found position", true, iggcon.FirstPollingStrategy(), false, []store{{1, 50}}, []uint32{1}},
		{"store during the poll", true, iggcon.OffsetPollingStrategy(20), true, []store{{1, 19}, {1, 50}}, nil},
		{"delete during the poll", true, iggcon.FirstPollingStrategy(), true, []store{{1, 50}}, []uint32{1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			if test.stored {
				client.offsets[1] = 50
			}
			if test.seek.Kind == iggcon.POLLING_FIRST {
				client.uncertainDeletes = 1
			} else {
				client.uncertainStores = 1
			}
			reader := onReadFrom(t, client, iggcon.NextPollingStrategy())
			require.NoError(t, reader.Seek(context.Background(), 1, iggcon.LastPollingStrategy()))
			poll := client.nextRead(t)
			require.True(t, poll.poll)

			if test.dropPoll {
				// The abandon of the write holds the seek until the poll ended.
				client.abandonRelease = make(chan struct{})
				sought := make(chan error, 1)
				go func() { sought <- reader.Seek(context.Background(), 1, test.seek) }()
				require.Eventually(t, func() bool {
					client.mu.Lock()
					defer client.mu.Unlock()
					return len(client.stores)+len(client.deleted) == 1
				}, testTimeout, 5*time.Millisecond, "the seek writes")
				poll.answer(section(1, 90))
				require.Eventually(t, func() bool {
					reader.mu.Lock()
					defer reader.mu.Unlock()
					return reader.reads == 0
				}, testTimeout, 5*time.Millisecond, "the poll ends during the seek")
				close(client.abandonRelease)
				require.ErrorIs(t, <-sought, ierror.OperationUncertain{})
			} else {
				poll.answer(section(1, 90))
				require.Eventually(t, func() bool {
					reader.mu.Lock()
					defer reader.mu.Unlock()
					return reader.partitions[1].position.Kind == iggcon.POLLING_OFFSET
				}, testTimeout, 5*time.Millisecond, "the poll finds the position")
				require.ErrorIs(t, reader.Seek(context.Background(), 1, test.seek), ierror.OperationUncertain{})
			}
			require.NoError(t, reader.Close(context.Background()))

			assert.Equal(t, test.stores, client.recordedStores())
			assert.Equal(t, test.deleted, client.deleted)
		})
	}
}

// A lost read stored past the messages that the reader delivered. After a
// seek write with an unknown outcome, Close moves the offset back before the
// first message of that read, and not to the stored offset that it left.
func TestPartitionReader_MovesTheOffsetBeforeALostReadAfterASeekWriteWithAnUnknownOutcomeOnCloseUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.offsets[1] = 4
	reader := onReadFrom(t, client, iggcon.NextPollingStrategy())
	ctx, cancel := context.WithCancel(context.Background())
	waited := make(chan error, 1)
	go func() { _, err := reader.Next(ctx); waited <- err }()
	read := client.nextRead(t)
	cancel()
	require.ErrorIs(t, <-waited, context.Canceled)
	client.mu.Lock()
	client.offsets[1] = 6
	client.uncertainStores = 1
	client.mu.Unlock()
	read.reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	require.Eventually(t, func() bool { return len(client.recordedAbandons()) == 1 }, testTimeout, 5*time.Millisecond)

	require.ErrorIs(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(2)), ierror.OperationUncertain{})
	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{1, 1}, {1, 4}}, client.recordedStores())
}

// The unknown outcome of a seek write belongs to the generation in which the
// write started. A partition that leaves and comes back meanwhile keeps its
// new state: no close point, no pin and no lost read.
func TestPartitionReader_IgnoresAnUnknownOutcomeOfASeekWriteOfAnEarlierGenerationUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.offsets[1] = 10
	client.uncertainStores = 1
	client.storeStarted, client.storeRelease = make(chan context.Context, 2), make(chan struct{})
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	require.Eventually(t, func() bool { return slices.Equal(reader.Partitions(), []uint32{1}) }, testTimeout, 5*time.Millisecond)
	sought := make(chan error, 1)
	go func() { sought <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(20)) }()
	<-client.storeStarted
	assign(t, client, reader)
	assign(t, client, reader, 1)
	close(client.storeRelease)
	require.ErrorIs(t, <-sought, ierror.OperationUncertain{})

	reader.mu.Lock()
	kept := *reader.partitions[1]
	reader.mu.Unlock()
	assert.Equal(t, iggcon.NextPollingStrategy(), kept.position, "the new generation keeps its start")
	assert.False(t, kept.lostRead)
	require.NoError(t, reader.Close(context.Background()))
	assert.Equal(t, []store{{1, 19}}, client.recordedStores(), "Close restores nothing for the new generation")
}

// A seek owns the position of its partition while it holds it. A check poll
// that ends during the hold does not count. So after a seek store with an
// unknown outcome, the partition polls its timestamp again, and Close restores
// the missing offset of the start instead of moving it before the poll result.
func TestPartitionReader_DropsACheckPollThatEndsWhileASeekHoldsItsPartitionUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.storeStarted = make(chan context.Context, 4)
	client.storeRelease = make(chan struct{})
	client.uncertainStores = 1
	reader := onReadFrom(t, client, iggcon.TimestampPollingStrategy(10))
	nextInBackground(reader)
	poll := client.nextRead(t)
	require.True(t, poll.poll, "a lookup without a stored offset leaves the start to a poll")
	sought := make(chan error, 1)
	go func() { sought <- reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(3)) }()
	select {
	case <-client.storeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the seek stored nothing")
	}

	poll.answer(section(1, 10))
	require.Eventually(t, func() bool {
		reader.mu.Lock()
		defer reader.mu.Unlock()
		return reader.reads == 0
	}, testTimeout, 5*time.Millisecond, "the poll ends during the hold")
	close(client.storeRelease)
	require.ErrorIs(t, <-sought, ierror.OperationUncertain{})

	read := client.nextRead(t)
	assert.True(t, read.poll, "the partition polls its timestamp again")
	assert.Equal(t, iggcon.POLLING_TIMESTAMP, read.targets[0].Strategy.Kind)
	require.NoError(t, reader.Close(context.Background()))
	assert.Equal(t, []store{{1, 2}}, client.recordedStores())
	assert.Equal(t, []uint32{1}, client.deleted, "Close restores the missing offset of the start")
}

// A seek whose caller stopped waiting finishes in the background, and the
// logger reports a write of it that fails, with its partition.
func TestPartitionReader_LogsAFailedWriteOfASeekWhoseCallerStoppedWaitingUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.storeStarted = make(chan context.Context, 4)
	client.storeRelease = make(chan struct{})
	client.failedStores = 1
	logged := make(logLines, 16)
	reader, err := NewPartitionReader(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(1),
		WithPollOptions(withCommit(waitOneSecond, iggcon.OnReadCommit())), WithRetryInterval(testRetryInterval),
		WithLogger(slog.New(slog.NewTextHandler(logged, nil))))
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close(context.Background()) })
	ctx, cancel := context.WithCancel(context.Background())
	sought := make(chan error, 1)
	go func() { sought <- reader.Seek(ctx, 1, iggcon.OffsetPollingStrategy(3)) }()
	select {
	case <-client.storeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the seek stored nothing")
	}
	cancel()
	require.ErrorIs(t, <-sought, context.Canceled)

	close(client.storeRelease)

	select {
	case line := <-logged:
		assert.Contains(t, line, "partition=1")
		assert.Contains(t, line, errStoreFailed.Error())
	case <-time.After(testTimeout):
		t.Fatal("the reader logged no failure of the seek")
	}
}

// storedByOtherMember is the offset that another member stores while a
// partition is away, and returnedOwner the owner generation of the partition
// after it came back.
const storedByOtherMember uint64 = 9

var returnedOwner = testContext.OwnerGeneration + 1

// ownerOp is the metadata op of a join or of the Register of a new session,
// which installs the owners of the member after every op of a context that a
// read gave before.
var ownerOp = testContext.MetadataOp + 1

// rejoinedWrite is the context of a write after a new session, before a read
// names the installed owner: owner generation 0 at the op of the rejoin, in
// the incarnation of the read before. installedRead is the context of a read
// for the installed owner.
var (
	rejoinedWrite = iggcon.PartitionContext{Incarnation: testContext.Incarnation, MetadataOp: ownerOp}
	installedRead = iggcon.PartitionContext{Incarnation: testContext.Incarnation, OwnerGeneration: returnedOwner, MetadataOp: ownerOp}
)

// returnsWithAQueuedBatch makes partition 2 of a member under OnRead leave
// while a batch of it that the server stored waits in the queue. Another
// member stores meanwhile, and the partition comes back under a new owner.
func returnsWithAQueuedBatch(t *testing.T) (*fakeClient, *PartitionReader) {
	t.Helper()
	client := newFakeClient()
	client.starts[2] = 4
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1), section(2, 4, 5))
	require.Equal(t, []uint64{0, 1}, offsets((<-polled).polled))
	assign(t, client, reader, 1)
	client.mu.Lock()
	client.offsets[2] = storedByOtherMember
	client.owners = map[uint32]uint64{2: returnedOwner}
	client.mu.Unlock()
	assign(t, client, reader, 1, 2)
	require.Equal(t, 1, queued(reader), "the batch of the earlier assignment stays queued")
	return client, reader
}

// A queued batch of an earlier assignment decides nothing on Close, because
// another member can have moved the offset since.
func TestPartitionReader_KeepsTheOffsetOfAnotherMemberOverAQueuedBatchOfAnEarlierAssignmentOnCloseUnderOnRead(t *testing.T) {
	client, reader := returnsWithAQueuedBatch(t)

	require.NoError(t, reader.Close(context.Background()))

	assert.Empty(t, client.recordedStores())
	assert.Equal(t, storedByOtherMember, client.offsets[2])
}

// The reader still hands over a batch of an earlier assignment, but the server
// refuses a store under its owner, so it is no close point. Close moves the
// offset back before the queued batch of the new assignment, under its owner.
func TestPartitionReader_MovesTheOffsetBackUnderTheNewOwnerAfterHandingOverABatchOfAnEarlierAssignmentOnCloseUnderOnRead(t *testing.T) {
	client, reader := returnsWithAQueuedBatch(t)
	readsPartition(t, reader, 2)
	assert.Equal(t, []uint64{4, 5}, offsets(next(t, reader)), "the batch of the earlier assignment is handed over")

	polled := nextInBackground(reader)
	returned := section(2, 10, 11)
	returned.Context.OwnerGeneration = returnedOwner
	client.nextRead(t).answer(section(1, 2, 3), returned)
	require.Equal(t, []uint64{2, 3}, offsets((<-polled).polled))
	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{2, 9}}, client.recordedStores())
}

// After a seek delete with an unknown outcome, the reader keeps its old
// position, pinned after the stored offset from before the delete. An empty
// read keeps the pin, so the next read does not start at 0 and deliver the
// partition again.
func TestPartitionReader_KeepsThePinOfALostSeekDeleteThroughAnEmptyReadUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.offsets[1] = 4
	client.uncertainDeletes = 1
	reader := onReadFrom(t, client, iggcon.NextPollingStrategy())

	require.ErrorIs(t, reader.Seek(context.Background(), 1, iggcon.FirstPollingStrategy()), ierror.OperationUncertain{})
	nextInBackground(reader)
	empty := client.nextRead(t)
	require.Equal(t, iggcon.POLLING_OFFSET, empty.targets[0].Strategy.Kind)
	require.Equal(t, uint64(5), empty.targets[0].Strategy.Value, "the pin follows the stored offset from before the delete")
	empty.answer(iggcon.FetchMessagesSection{PartitionId: 1, CurrentOffset: 4, NextOffset: 5, Context: testContext})

	read := client.nextRead(t)
	assert.Equal(t, iggcon.POLLING_OFFSET, read.targets[0].Strategy.Kind)
	assert.Equal(t, uint64(5), read.targets[0].Strategy.Value, "an empty read keeps the pin")
}

// readsPartition waits until the groups of reader hold partition, so that its reads take it.
func readsPartition(t *testing.T, reader *PartitionReader, partition uint32) {
	t.Helper()
	require.Eventually(t, func() bool {
		reader.mu.Lock()
		defer reader.mu.Unlock()
		return slices.ContainsFunc(reader.groups, func(group []uint32) bool { return slices.Contains(group, partition) })
	}, testTimeout, 5*time.Millisecond, "the reads take partition %d", partition)
}

// losesItsSession makes the next sync of a member miss it, as after a lost
// session, and waits until the reader joined again and follows assigned after
// the join. The server installs the owner of each assigned partition again
// under a new generation then, and refuses a write under the old one.
func losesItsSession(t *testing.T, client *fakeClient, reader *PartitionReader, assigned ...uint32) {
	t.Helper()
	syncs := func() int {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.syncs)
	}
	require.Eventually(t, func() bool { return syncs() > 0 }, testTimeout, 5*time.Millisecond, "the first sync reads the assignment")
	client.mu.Lock()
	client.memberMissing = true
	client.installOp = ownerOp
	client.owners = make(map[uint32]uint64, len(assigned))
	for _, partition := range assigned {
		client.owners[partition] = returnedOwner
	}
	client.mu.Unlock()
	// A sync that waits already for the next assignment misses no member.
	client.assignments <- reader.Partitions()
	require.Eventually(t, func() bool {
		client.mu.Lock()
		defer client.mu.Unlock()
		return len(client.membership) == 2
	}, testTimeout, 5*time.Millisecond, "the reader joins again")
	before := syncs()
	// The sync after the join, and one more, which starts only after the reader followed the first.
	client.assignments <- assigned
	client.assignments <- assigned
	require.Eventually(t, func() bool { return syncs() >= before+2 && slices.Equal(reader.Partitions(), assigned) },
		testTimeout, 5*time.Millisecond, "the reader follows the assignment after the join")
}

// A read lost across a new session stored on the server, and the partition
// stays with the member. The close rewind then names owner generation 0,
// which the server binds to the owner that it installed for the new session.
// It names the metadata op of the rejoin too, since the server binds owner 0
// only to an owner installed at or below that op, and keeps the incarnation
// of the read, which the server checks.
func TestPartitionReader_MovesTheOffsetBackOverAReadLostAcrossANewSessionUnderTheInstalledOwnerOnCloseUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	losesItsSession(t, client, reader, 1)

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
	assert.Equal(t, []iggcon.PartitionContext{rejoinedWrite}, client.recordedStoreContexts(), "the first rewind names the op of the rejoin")
}

// A read can be refused as not the owner before the reader sees that its
// session changed. The partition keeps its lost read through the refusal, so
// the close rewind still moves the offset back under the installed owner.
func TestPartitionReader_KeepsAReadLostBeforeARefusalAsNotOwnedAndMovesTheOffsetBackOnCloseUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	client.nextRead(t).reply <- fakeReply{err: ierror.ErrConsumerGroupPartitionNotOwned}
	losesItsSession(t, client, reader, 1)

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
}

// A seek after a new session names owner generation 0 until a read of the new
// session names the installed owner.
func TestPartitionReader_StoresASeekAfterANewSessionUnderTheInstalledOwnerUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	losesItsSession(t, client, reader, 1)

	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(5)))

	assert.Equal(t, []store{{1, 4}}, client.recordedStores())
	assert.Equal(t, []iggcon.PartitionContext{rejoinedWrite}, client.recordedStoreContexts(), "the first store names the op of the rejoin")
}

// A new session that the reader did not see yet installs the owner again, so
// the server refuses the close rewind under the old owner as not owned. That
// is no leave, so the reader reads the partition for the owner that the server
// installed and sends the rewind once more under it. Close retries a transient
// refusal of that read under its own context, also a refusal of the whole
// request, which the server sends for a read of one partition over its quota,
// and a refusal by a primary that moved. Only the context of that read counts,
// so a message that does not decode does not fail it.
func TestPartitionReader_SendsARewindRefusedAsNotOwnedOnceMoreUnderTheInstalledOwnerOnCloseUnderOnRead(t *testing.T) {
	for _, test := range []struct {
		name            string
		failed, refused []error
		undecodable     bool
		reads           int
	}{
		{"at once", nil, nil, false, 1},
		{"after a transient refusal of the request", []error{ierror.ErrDeferredPollCapacityExceeded}, nil, false, 2},
		{"after a transient refusal of the partition", nil, []error{ierror.ErrTransientNotAccepted}, false, 2},
		{"after the primary moved, as the request shows", []error{ierror.ErrNotPrimary}, nil, false, 2},
		{"after the primary moved, as the partition shows", nil, []error{ierror.ErrNotPrimary}, false, 2},
		{"when the next message does not decode", nil, nil, true, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, reader := losesAReadAfterADelivery(t)
			installsTheOwnerAgain(client)
			client.mu.Lock()
			client.failedProbes = map[uint32][]error{1: test.failed}
			client.refusedProbes = map[uint32][]error{1: test.refused}
			client.undecodableProbes = map[uint32]bool{1: test.undecodable}
			client.mu.Unlock()
			probed := len(client.recordedProbes())

			require.NoError(t, reader.Close(context.Background()))

			assert.Equal(t, []store{{1, 1}}, client.recordedStores())
			assert.Equal(t, []iggcon.PartitionContext{testContext, installedRead}, client.recordedStoreContexts())
			assert.Len(t, client.recordedProbes()[probed:], test.reads, "the reads for the installed owner")
		})
	}
}

// losesAReadAfterADelivery makes a member under OnRead deliver offsets 0 and 1
// of partition 1 and lose its next read, so Close moves the offset back to 1.
func losesAReadAfterADelivery(t *testing.T) (*fakeClient, *PartitionReader) {
	t.Helper()
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	return client, reader
}

// The caller of Close can end the retry of the read for the installed owner.
// The refusal of the rewind then stands. The rewind of the other partition
// still goes once, without a read for the installed owner after its refusal,
// and the leave goes too. Both partitions refuse their rewind as not owned, so
// whichever goes first, the other one goes after the cancel.
func TestPartitionReader_EndsTheRetryOfARewindRefusedAsNotOwnedWhenTheContextOfCloseEndsUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1), section(2, 0, 1))
	require.NoError(t, (<-polled).err)
	next(t, reader)
	nextInBackground(reader)
	client.nextRead(t).reply <- fakeReply{err: ierror.FetchProgressUncertain{OperationID: 42}}
	client.mu.Lock()
	client.owners = map[uint32]uint64{1: returnedOwner, 2: returnedOwner}
	client.installOp, client.watermark = ownerOp, ownerOp
	client.refusedProbes = map[uint32][]error{1: keptRefusals, 2: keptRefusals}
	client.mu.Unlock()
	probed := len(client.recordedProbes())
	ctx, cancel := context.WithCancel(context.Background())
	closed := make(chan error, 1)
	go func() { closed <- reader.Close(ctx) }()
	require.Eventually(t, func() bool { return len(client.recordedProbes()) > probed }, testTimeout, 5*time.Millisecond,
		"the read for the installed owner is refused")

	cancel()

	select {
	case err := <-closed:
		require.ErrorIs(t, err, ierror.ErrConsumerGroupPartitionNotOwned)
	case <-time.After(testTimeout):
		t.Fatal("Close retries the read after its context ended")
	}
	during := client.recordedProbes()[probed:]
	assert.Len(t, slices.Compact(slices.Clone(during)), 1, "only the first rewind reads for the installed owner")
	assert.Never(t, func() bool { return len(client.recordedProbes()) > probed+len(during) }, 3*testRetryInterval,
		testRetryInterval/2, "no read for the installed owner follows")
	assert.Len(t, client.recordedStoreContexts(), 2, "the rewind of each partition goes once")
	assert.Empty(t, client.recordedStores())
	assert.Equal(t, []string{"join", "leave"}, client.membership)
}

// The close budget ends the retry of the read for the installed owner also
// when the context of Close never ends. The refusal of the rewind then stands,
// and the leave still goes.
func TestPartitionReader_EndsTheRetryOfARewindRefusedAsNotOwnedAtTheCloseBudgetUnderOnRead(t *testing.T) {
	client, reader := losesAReadAfterADelivery(t)
	installsTheOwnerAgain(client)
	client.mu.Lock()
	client.refusedProbes = map[uint32][]error{1: slices.Repeat([]error{ierror.ErrTransientNotAccepted}, int(2*closeBudget/testRetryInterval))}
	client.mu.Unlock()
	started := time.Now()
	closed := make(chan error, 1)

	go func() { closed <- reader.Close(context.Background()) }()

	select {
	case err := <-closed:
		require.ErrorIs(t, err, ierror.ErrConsumerGroupPartitionNotOwned)
	case <-time.After(closeBudget + testTimeout):
		t.Fatal("Close retries the read for the installed owner past its budget")
	}
	assert.GreaterOrEqual(t, time.Since(started), closeBudget, "the retry lasts until the budget ends")
	assert.Equal(t, []iggcon.PartitionContext{testContext}, client.recordedStoreContexts(), "the rewind goes once")
	assert.Equal(t, []string{"join", "leave"}, client.membership)
}

// A cancel while the read for the installed owner is in flight ends the retry
// too, so the rewind goes no more after the reply, and its refusal stands.
func TestPartitionReader_SendsNoRewindAgainWhenTheContextOfCloseEndsDuringTheReadForTheInstalledOwnerUnderOnRead(t *testing.T) {
	client, reader := losesAReadAfterADelivery(t)
	installsTheOwnerAgain(client)
	client.mu.Lock()
	client.probeStarted, client.probeRelease = make(chan struct{}, 1), make(chan struct{})
	client.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	closed := make(chan error, 1)
	go func() { closed <- reader.Close(ctx) }()
	select {
	case <-client.probeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the reader sent no read for the installed owner")
	}
	require.Len(t, client.recordedStoreContexts(), 1, "the read follows the refused rewind")

	cancel()
	close(client.probeRelease)

	select {
	case err := <-closed:
		require.ErrorIs(t, err, ierror.ErrConsumerGroupPartitionNotOwned)
	case <-time.After(testTimeout):
		t.Fatal("Close does not end")
	}
	assert.Equal(t, []iggcon.PartitionContext{testContext}, client.recordedStoreContexts(), "the rewind goes once")
}

// A close whose context ended before it started still moves the offset back
// and leaves, each write once.
func TestPartitionReader_MovesTheOffsetBackAndLeavesOnACloseWhoseContextEndedUnderOnRead(t *testing.T) {
	client, reader := losesAReadAfterADelivery(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, reader.Close(ctx))

	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
	assert.Equal(t, []string{"join", "leave"}, client.membership)
}

// A rebalance by another member can give the member a partition at an op that
// no reply of the client named, since a sync reply names none. A seek before
// the first read of it names no history, no owner and an older op. The client
// names the history of the route, and the server refuses the seek as not
// owned. That is no leave, so the reader reads the partition for the owner
// that the server installed and sends the write once more under the context
// of that read.
func TestPartitionReader_StoresASeekBeforeTheFirstReadOfAPartitionThatARebalanceGaveUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	// Under OnRead no read starts before a Next waits.
	require.Eventually(t, func() bool { return slices.Contains(reader.Partitions(), 1) }, testTimeout, 5*time.Millisecond,
		"the member holds the partition")
	client.mu.Lock()
	client.owners = map[uint32]uint64{1: returnedOwner}
	client.installOp = ownerOp
	client.mu.Unlock()

	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(5)))

	assert.Equal(t, []store{{1, 4}}, client.recordedStores())
	assert.Equal(t, []iggcon.PartitionContext{{}, installedRead}, client.recordedStoreContexts(),
		"the second store names the history of the read")
}

// A fully zero context of a seek names no history and no owner, so that seek
// and the seeks after it store under the context of the last read.
func TestPartitionReader_StoresASeekWithAZeroContextUnderTheContextOfTheLastReadUnderOnRead(t *testing.T) {
	client := newFakeClient()
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1)
	polled := nextInBackground(reader)
	require.Equal(t, []uint64{0, 1}, client.serve(client.nextRead(t)))
	require.NoError(t, (<-polled).err)

	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(5).WithContext(iggcon.PartitionContext{})))
	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(3)))

	assert.Equal(t, []store{{1, 4}, {1, 2}}, client.recordedStores())
	assert.Equal(t, []iggcon.PartitionContext{testContext, testContext}, client.recordedStoreContexts())
}

// A session change alone is no leave. A queued batch of a partition that stays
// with the member still decides Close, under owner generation 0 until a read
// of the new session names the installed owner.
func TestPartitionReader_MovesTheOffsetBackBeforeABatchQueuedAcrossANewSessionUnderTheInstalledOwnerOnCloseUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.starts[2] = 4
	reader := member(t, client, withCommit(waitOneSecond, iggcon.OnReadCommit()), 1, 2)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1), section(2, 4, 5))
	require.Equal(t, []uint64{0, 1}, offsets((<-polled).polled))
	losesItsSession(t, client, reader, 1, 2)
	require.Equal(t, 1, queued(reader), "the batch of the partition that stayed is still queued")

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{2, 3}}, client.recordedStores())
	assert.Equal(t, []iggcon.PartitionContext{rejoinedWrite}, client.recordedStoreContexts(), "the first rewind names the op of the rejoin")
}

// countsWithoutStoring counts each delivered message and stores only on Close
// or when a partition leaves.
var countsWithoutStoring = iggcon.AutoCommit{
	Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessages, Messages: math.MaxUint32},
	After: iggcon.CommitAfterDelivery,
}

// storesEachBatch makes each delivered batch ask for a store in the background.
var storesEachBatch = iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryBatch}, After: iggcon.CommitAfterDelivery}

// The flush on Close stores a counted position of a partition that stayed
// across a new session under owner generation 0, which the server binds to
// the owner that it installed for the new session.
func TestPartitionReader_StoresTheCountedPositionAfterANewSessionUnderTheInstalledOwnerOnClose(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.AutoCommitPolicy(countsWithoutStoring)), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1))
	require.NoError(t, (<-polled).err)
	losesItsSession(t, client, reader, 1)

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
	assert.Equal(t, []iggcon.PartitionContext{rejoinedWrite}, client.recordedStoreContexts(), "the first store names the op of the rejoin")
}

// A commit after a new session of a checkpoint that a read gave before it
// names owner generation 0, which the server binds to the owner that it
// installed for the new session.
func TestPartitionReader_CommitsACheckpointOfAReadBeforeANewSessionUnderTheInstalledOwner(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, withCommit(waitOneSecond, iggcon.ManualCommit()), 1)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 3, 4))
	result := <-polled
	require.NoError(t, result.err)
	checkpoint, _ := result.polled.Checkpoint()
	losesItsSession(t, client, reader, 1)

	require.NoError(t, reader.Commit(context.Background(), checkpoint))

	assert.Equal(t, []store{{1, 4}}, client.recordedStores())
	assert.Equal(t, []iggcon.PartitionContext{rejoinedWrite}, client.recordedStoreContexts(), "the first store names the op of the rejoin")
}

// A counted position of an earlier assignment is never stored after its
// partition came back, because another member can have moved the offset since.
func TestPartitionReader_NeverStoresTheCountedPositionOfAnEarlierAssignmentAfterItsPartitionCameBack(t *testing.T) {
	client := newFakeClient()
	client.starts[2] = 4
	client.storeStarted, client.storeRelease = make(chan context.Context, 4), make(chan struct{})
	reader := member(t, client, withCommit(waitOneSecond, iggcon.AutoCommitPolicy(storesEachBatch)), 1, 2)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1), section(2, 4, 5))
	require.NoError(t, (<-polled).err)
	// One flush runs at a time, so the held store of the first batch holds the
	// flush that the leave asks for until the partition is back.
	storeStarts(t, client)
	assert.Equal(t, []uint64{4, 5}, offsets(next(t, reader)))
	assign(t, client, reader, 1)
	assign(t, client, reader, 1, 2)
	close(client.storeRelease)

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
}

// A counted position of a partition that left is never stored under an owner
// of a lost membership: neither one that waits for its store when the
// membership ends, nor one of a partition that leaves after that.
func TestPartitionReader_NeverStoresTheCountedPositionOfALeftPartitionUnderALostOwner(t *testing.T) {
	client := newFakeClient()
	client.starts[2] = 4
	client.starts[3] = 7
	client.storeStarted, client.storeRelease = make(chan context.Context, 4), make(chan struct{})
	reader := member(t, client, withCommit(waitOneSecond, iggcon.AutoCommitPolicy(storesEachBatch)), 1, 2, 3)
	polled := nextInBackground(reader)
	client.nextRead(t).answer(section(1, 0, 1), section(2, 4, 5), section(3, 7, 8))
	require.NoError(t, (<-polled).err)
	// One flush runs at a time, so the held store of the first batch holds the
	// flush that the first leave asks for until the membership is lost.
	storeStarts(t, client)
	next(t, reader)
	next(t, reader)
	assign(t, client, reader, 1, 3)
	losesItsSession(t, client, reader, 1)
	close(client.storeRelease)

	require.NoError(t, reader.Close(context.Background()))

	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
}

// installsTheOwnerAgain makes the server install the owner of partition 1
// again, as a new session that the reader did not see yet does. The reply of
// its Register raises the metadata op of the client to the op of the install.
func installsTheOwnerAgain(client *fakeClient) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.owners = map[uint32]uint64{1: returnedOwner}
	client.installOp = ownerOp
	client.watermark = ownerOp
}

// storeStarts waits until a store of client starts.
func storeStarts(t *testing.T, client *fakeClient) {
	t.Helper()
	select {
	case <-client.storeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the reader sent no store")
	}
}

// A write refused as not the owner goes once more only under the owner that
// a read of the same history names. If that read is refused as not the owner
// too, or names another history, the first refusal stands.
func TestPartitionReader_FailsACommitRefusedAsNotOwnedWhoseReadForTheInstalledOwnerFails(t *testing.T) {
	for _, test := range []struct {
		name        string
		refused     []error
		incarnation uint64
	}{
		{"refused as not the owner", []error{ierror.ErrConsumerGroupPartitionNotOwned}, testContext.Incarnation},
		{"of another history", nil, testContext.Incarnation - 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			client.refusedProbes = map[uint32][]error{1: test.refused}
			reader := member(t, client, waitOneSecond, 1)
			read := section(1, 3, 4)
			read.Context.Incarnation = test.incarnation
			client.nextRead(t).answer(read)
			checkpoint, _ := next(t, reader).Checkpoint()
			installsTheOwnerAgain(client)

			require.ErrorIs(t, reader.Commit(context.Background(), checkpoint), ierror.ErrConsumerGroupPartitionNotOwned)

			assert.Empty(t, client.recordedStores())
			assert.Len(t, client.recordedStoreContexts(), 1, "the store goes no more")
			assert.Equal(t, []uint32{1}, client.recordedProbes())
		})
	}
}

// A partition that left while its write was out belongs to the member no
// more, so a refusal as not the owner stands without a read for the owner.
func TestPartitionReader_SendsNoReadForTheInstalledOwnerOfACommitWhosePartitionLeft(t *testing.T) {
	client := newFakeClient()
	client.storeStarted, client.storeRelease = make(chan context.Context, 1), make(chan struct{})
	reader := member(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 3, 4))
	checkpoint, _ := next(t, reader).Checkpoint()
	installsTheOwnerAgain(client)
	committed := make(chan error, 1)
	go func() { committed <- reader.Commit(context.Background(), checkpoint) }()
	storeStarts(t, client)
	assign(t, client, reader)
	close(client.storeRelease)

	require.ErrorIs(t, <-committed, ierror.ErrConsumerGroupPartitionNotOwned)
	assert.Empty(t, client.recordedProbes())
}

// A write refused as not the owner goes once more under the installed owner,
// and only once: a second refusal fails it.
func TestPartitionReader_SendsACommitRefusedAsNotOwnedOnlyOnceMore(t *testing.T) {
	client := newFakeClient()
	client.storeStarted, client.storeRelease = make(chan context.Context, 1), make(chan struct{})
	reader := member(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 3, 4))
	checkpoint, _ := next(t, reader).Checkpoint()
	installsTheOwnerAgain(client)
	committed := make(chan error, 1)
	go func() { committed <- reader.Commit(context.Background(), checkpoint) }()
	storeStarts(t, client)
	client.storeRelease <- struct{}{}
	storeStarts(t, client)
	// The server installs the owner once more before the second store lands.
	client.mu.Lock()
	client.owners[1] = returnedOwner + 1
	client.mu.Unlock()
	client.storeRelease <- struct{}{}

	select {
	case err := <-committed:
		require.ErrorIs(t, err, ierror.ErrConsumerGroupPartitionNotOwned)
	case <-time.After(testTimeout):
		t.Fatal("the commit sent its store a third time")
	}
	assert.Equal(t, []iggcon.PartitionContext{testContext, installedRead}, client.recordedStoreContexts())
	assert.Equal(t, []uint32{1}, client.recordedProbes())
}

// The caller of Commit can end it. A cancel ends the retry of the read for the
// installed owner at once, and the refusal of the store stands.
func TestPartitionReader_EndsTheRetryOfACommitRefusedAsNotOwnedWhenItsContextEnds(t *testing.T) {
	client := newFakeClient()
	client.refusedProbes = map[uint32][]error{1: keptRefusals}
	reader := member(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 3, 4))
	checkpoint, _ := next(t, reader).Checkpoint()
	installsTheOwnerAgain(client)
	ctx, cancel := context.WithCancel(context.Background())
	committed := make(chan error, 1)
	go func() { committed <- reader.Commit(ctx, checkpoint) }()
	require.Eventually(t, func() bool { return len(client.recordedProbes()) > 0 }, testTimeout, 5*time.Millisecond,
		"the read for the installed owner is refused")

	cancel()

	select {
	case err := <-committed:
		require.ErrorIs(t, err, ierror.ErrConsumerGroupPartitionNotOwned)
	case <-time.After(testTimeout):
		t.Fatal("Commit retries the read after its context ended")
	}
	probed := len(client.recordedProbes())
	assert.Never(t, func() bool { return len(client.recordedProbes()) > probed }, 3*testRetryInterval, testRetryInterval/2,
		"no read for the installed owner follows")
	assert.Empty(t, client.recordedStores())
}

// keptRefusals are more transient refusals of a read than a test waits
// through, so the retry of that read ends only when its caller cancels.
var keptRefusals = slices.Repeat([]error{ierror.ErrTransientNotAccepted}, int(2*testTimeout/testRetryInterval))

// closeDuringTheRetry closes reader while a read for the installed owner keeps
// a transient refusal, and fails the test if Close waits for that read.
func closeDuringTheRetry(t *testing.T, client *fakeClient, reader *PartitionReader) error {
	t.Helper()
	closed := make(chan error, 1)
	go func() { closed <- reader.Close(context.Background()) }()
	select {
	case err := <-closed:
		return err
	case <-time.After(testTimeout):
		// The read then gets its reply, so the reader closes after the test.
		client.mu.Lock()
		client.refusedProbes = nil
		client.mu.Unlock()
		t.Fatal("Close waits for the retry of a write in the background")
		return nil
	}
}

// requireLogged waits until logged receives a line that holds each of parts.
func requireLogged(t *testing.T, logged logLines, parts ...string) {
	t.Helper()
	deadline := time.After(testTimeout)
	for {
		select {
		case line := <-logged:
			if !slices.ContainsFunc(parts, func(part string) bool { return !strings.Contains(line, part) }) {
				return
			}
		case <-deadline:
			t.Fatalf("the reader logged no line with %q", parts)
		}
	}
}

// Once the caller of a seek stopped waiting, the reader is the caller of its
// write. So Close ends the retry of the read for the installed owner after a
// refusal of the write as not the owner, and the logger reports the refusal.
func TestPartitionReader_EndsTheRetryOfASeekWriteRefusedAsNotOwnedWhenCloseStartsUnderOnRead(t *testing.T) {
	client := newFakeClient()
	client.refusedProbes = map[uint32][]error{1: keptRefusals}
	logged := make(logLines, 16)
	reader := memberWith(t, client, []uint32{1}, WithPollOptions(withCommit(waitOneSecond, iggcon.OnReadCommit())),
		WithLogger(slog.New(slog.NewTextHandler(logged, nil))))
	require.Eventually(t, func() bool { return slices.Contains(reader.Partitions(), 1) }, testTimeout, 5*time.Millisecond,
		"the member holds the partition")
	installsTheOwnerAgain(client)
	ctx, cancel := context.WithCancel(context.Background())
	sought := make(chan error, 1)
	go func() { sought <- reader.Seek(ctx, 1, iggcon.OffsetPollingStrategy(5)) }()
	require.Eventually(t, func() bool { return len(client.recordedProbes()) > 0 }, testTimeout, 5*time.Millisecond,
		"the read for the installed owner is refused")
	cancel()
	require.ErrorIs(t, <-sought, context.Canceled)

	require.NoError(t, closeDuringTheRetry(t, client, reader))

	requireLogged(t, logged, "partition=1", ierror.ErrConsumerGroupPartitionNotOwned.Error())
	assert.Empty(t, client.recordedStores())
}

// The reader is the caller of a store of CommitAuto in the background. So
// Close ends the retry of the read for the installed owner after a refusal of
// that store as not the owner, and the logger reports the refusal. The store
// of Close then names owner generation 0 at the op of the new session.
func TestPartitionReader_EndsTheRetryOfAnAutomaticStoreRefusedAsNotOwnedInTheBackgroundWhenCloseStarts(t *testing.T) {
	client := newFakeClient()
	client.refusedProbes = map[uint32][]error{1: keptRefusals}
	logged := make(logLines, 16)
	auto := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryBatch}, After: iggcon.CommitAfterDelivery}
	reader := memberWith(t, client, []uint32{1}, WithPollOptions(withCommit(waitOneSecond, iggcon.AutoCommitPolicy(auto))),
		WithLogger(slog.New(slog.NewTextHandler(logged, nil))))
	client.nextRead(t).answer(section(1, 0, 1))
	installsTheOwnerAgain(client)
	next(t, reader)
	require.Eventually(t, func() bool { return len(client.recordedProbes()) > 0 }, testTimeout, 5*time.Millisecond,
		"the read for the installed owner is refused")

	require.NoError(t, closeDuringTheRetry(t, client, reader))

	requireLogged(t, logged, "Failed to store the counted offsets", ierror.ErrConsumerGroupPartitionNotOwned.Error())
	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
	assert.Equal(t, []iggcon.PartitionContext{testContext, rejoinedWrite}, client.recordedStoreContexts())
}

// While an automatic store in the background waits for its reply, as when the
// server is down, the ticks and due stores start no more stores. So Close
// waits only for that store, and then stores the latest position once.
func TestPartitionReader_QueuesNoAutomaticStoresBehindAStoreThatWaitsForItsReply(t *testing.T) {
	client := newFakeClient()
	client.storeStarted, client.storeRelease = make(chan context.Context, 256), make(chan struct{})
	client.failedStores = math.MaxInt
	auto := iggcon.AutoCommit{Interval: time.Millisecond, Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessage},
		After: iggcon.CommitAfterDelivery}
	reader := standalone(t, client, withCommit(waitOneSecond, iggcon.AutoCommitPolicy(auto)), 1)
	client.nextRead(t).answer(section(1, 0))
	next(t, reader)
	storeStarts(t, client)
	for offset := uint64(1); offset <= 3; offset++ {
		client.nextRead(t).answer(section(1, offset))
		next(t, reader)
	}
	time.Sleep(20 * auto.Interval)
	closed := make(chan error, 1)
	go func() { closed <- reader.Close(context.Background()) }()
	select {
	case <-reader.loopDone:
	case <-time.After(testTimeout):
		t.Fatal("Close does not stop the loop")
	}

	// The held store ends, as at the request timeout of the client.
	client.storeRelease <- struct{}{}
	storeStarts(t, client)
	client.storeRelease <- struct{}{}

	select {
	case err := <-closed:
		require.ErrorIs(t, err, errStoreFailed)
	case <-time.After(testTimeout):
		close(client.storeRelease)
		t.Fatal("Close waits for stores that queued in the background")
	}
	assert.Len(t, client.recordedStoreContexts(), 2, "the held store and the store of Close")
}

// A seek under CommitManual rewrites its partition while a commit of it reads
// for the installed owner. The commit takes the partition only under the lock,
// so the race detector finds no race, and the commit goes once more. The race
// detector keeps only a few accesses of each word, so it can miss a race in
// one run.
func TestPartitionReader_SeeksAPartitionWhileACommitOfItReadsForTheInstalledOwner(t *testing.T) {
	client := newFakeClient()
	reader := member(t, client, waitOneSecond, 1)
	client.nextRead(t).answer(section(1, 3, 4))
	checkpoint, _ := next(t, reader).Checkpoint()
	installsTheOwnerAgain(client)
	client.mu.Lock()
	client.probeStarted, client.probeRelease = make(chan struct{}, 1), make(chan struct{})
	client.mu.Unlock()
	committed := make(chan error, 1)
	go func() { committed <- reader.Commit(context.Background(), checkpoint) }()
	// The length of the channel orders the seek after the start of the read
	// without a happens-before edge, so the race detector still sees an access
	// of the commit to the partition that the lock does not order.
	require.Eventually(t, func() bool { return len(client.probeStarted) == 1 }, testTimeout, time.Millisecond)

	require.NoError(t, reader.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(0)))
	close(client.probeRelease)

	require.NoError(t, <-committed)
	assert.Equal(t, []store{{1, 4}}, client.recordedStores(), "the commit goes once more under the installed owner")
}
