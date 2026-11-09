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

// Package reader reads the partitions of a topic with a PartitionReader, which
// groups them by primary and reads the partitions of a group together, and
// hands over one message at a time with an IggyConsumer.
package reader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"
	"sync"
	"time"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	// DefaultMaxWait is the wait of a read of a reader unless its options say otherwise.
	DefaultMaxWait = 5 * time.Second
	// DefaultPrefetchMessages is the requested message budget before the minimum read capacity applies.
	DefaultPrefetchMessages = 16_000
	// DefaultRetryInterval is the pause before a refused or failed read is sent again.
	DefaultRetryInterval      = time.Second
	lastingRefusalsBeforeStop = 6
	maxBackoffShift           = 5
	wholeReader               = -1
	minimumPrefetchSlots      = 32
	// maxReadsInFlight is half of the deferred requests that a client holds at
	// once, so one reader leaves room for the readers and polls that share it.
	maxReadsInFlight = 32
	// assignmentSettlePause is the pause of a group member after a sync that
	// followed its join or changed its partitions. A join moves ownership only
	// after the partition logs persist the new owner, a few milliseconds later.
	assignmentSettlePause = 10 * time.Millisecond
	// closeBudget bounds the reads of Close for the installed owner, from the
	// start of Close, so a refusal that stays transient cannot hold Close.
	closeBudget = 10 * time.Second
)

// ReadError is an error of a read. The reader stays usable after it and retries by itself.
type ReadError struct {
	// PartitionID is the partition of the error, or nil for an error of the whole reader.
	PartitionID *uint32
	Err         error
	// Skip is, for a message that does not decode, the continuation after it.
	// The partition stops until a Seek, to Skip to step over the message.
	Skip *iggcon.PollingStrategy
	// Lost is, for a known lost range, the offsets that a read whose reply was
	// lost stored and that the reader does not deliver. Err then wraps
	// errors.FetchProgressUncertain.
	Lost *OffsetRange
}

// OffsetRange holds the offsets of a partition from First to Last, both included.
type OffsetRange struct {
	First uint64
	Last  uint64
}

func (e *ReadError) Error() string {
	if e.PartitionID != nil {
		return fmt.Sprintf("partition %d: %v", *e.PartitionID, e.Err)
	}
	return e.Err.Error()
}

func (e *ReadError) Unwrap() error { return e.Err }

// transport is what the reader needs of the client: a read of the partitions
// of one primary in one request, the grouping of partitions by primary, and
// the metadata op that the client saw committed.
type transport interface {
	FetchMessages(ctx context.Context, streamId, topicId iggcon.Identifier, consumer iggcon.Consumer,
		targets []iggcon.FetchMessagesTarget, count uint32, autoCommit bool, options iggcon.DeferredPollOptions,
	) ([]iggcon.FetchMessagesSection, error)
	ReadinessGroups(ctx context.Context, streamId, topicId iggcon.Identifier, partitions []uint32) ([][]uint32, error)
	MetadataWatermark() uint64
}

// Option configures a PartitionReader.
type Option func(*config)

type config struct {
	partitions       []uint32
	start            iggcon.PollingStrategy
	options          iggcon.PollOptions
	prefetchMessages uint32
	retryInterval    time.Duration
	logger           *slog.Logger
}

// WithPartitions sets the partitions of a standalone consumer, which requires
// them. A consumer group reads the assignment of its member.
func WithPartitions(partitions ...uint32) Option {
	return func(c *config) { c.partitions = append([]uint32(nil), partitions...) }
}

// WithStart sets the start of a partition without a position, NextPollingStrategy by default.
// With CommitOnRead, the start applies only to a partition without a stored
// offset. A partition with one resumes after it. A timestamp or Last start
// first finds its message with a poll that stores nothing, since a fetch reads
// only from an offset or Next. A Last start delivers the last Count messages
// of the poll options under every commit policy.
func WithStart(start iggcon.PollingStrategy) Option {
	return func(c *config) { c.start = start }
}

// WithPollOptions sets the options of each read and the commit policy,
// WaitPollOptions(DefaultMaxWait) by default, which stores nothing.
func WithPollOptions(options iggcon.PollOptions) Option {
	return func(c *config) { c.options = options }
}

// WithPrefetchMessages sets the requested message budget for reads in flight
// and queued batches, DefaultPrefetchMessages (16,000) by default. The
// effective budget holds at least 32 full batches so quiet primaries do not
// occupy the only slot. Larger budgets allow more prefetched batches. At most
// 32 reads of one reader run at once.
//
// With CommitOnRead the server stores the offset of each read before Next
// returns its batch. A crash, or a reader that stops without Close, loses the
// batches that the reader held: up to the effective budget, which holds at
// least 32 batches. CommitManual and CommitAuto store offsets after delivery
// instead.
func WithPrefetchMessages(messages uint32) Option {
	return func(c *config) { c.prefetchMessages = messages }
}

// WithRetryInterval sets the pause before a refused or failed read is sent again.
func WithRetryInterval(interval time.Duration) Option {
	return func(c *config) { c.retryInterval = interval }
}

// WithLogger sets the logger of the retries.
func WithLogger(logger *slog.Logger) Option {
	return func(c *config) { c.logger = logger }
}

type partition struct {
	id         uint32
	generation uint64
	position   iggcon.PollingStrategy
	// fromRead marks a position that a read gave, so it carries the history of the partition.
	fromRead bool
	inFlight bool
	// parked marks a partition whose read in flight holds quiet partitions, so it may wait.
	parked bool
	// misrouted marks a partition that its primary refused, which waits for
	// the end of the next lookup of the groups instead of reading the old
	// primary at every round trip.
	misrouted bool
	retryAt   time.Time
	// servedAt is the read turn that last returned messages of the partition.
	servedAt uint64
	// backlogged marks a partition whose last section ended before its last
	// message, so its next read returns at once.
	backlogged      bool
	lastingRefusals int
	stopped         *ReadError
	context         iggcon.PartitionContext
	// seenOwner is the highest owner generation that a read of the partition
	// gave, and lostOwner the highest one of a membership that the member lost,
	// or that the server refused as not the owner. The server installs the
	// owner again under a higher generation then, so a read or write never
	// names an owner up to lostOwner.
	seenOwner uint64
	lostOwner uint64
	// received is the last offset that a read of this generation returned.
	received *uint64
	baseline *uint64
	// probed marks a baseline at the message that a probe returned. Any other
	// baseline is an offset that a start or a seek named.
	probed         bool
	readStarted    bool
	firstDelivered *uint64
	delivered      *iggcon.Checkpoint
	committed      *uint64
	// lostRead marks a committing read whose outcome is unknown, until a later
	// committing read returns messages without a gap. Only then does a forward
	// jump mean that the lost read stored.
	lostRead bool
	// gapFrom and held: under OnRead, the start of the messages that a lost
	// read stored, and the batch that was read after them.
	gapFrom *uint64
	gapEnd  uint64
	held    *iggcon.PolledMessage
	// aloneUntil is the end of a section that did not decode, which the
	// partition reads one message at a time.
	aloneUntil *uint64
	// checkStored marks a partition at the start whose first committing read
	// looks up its stored offset first.
	checkStored bool
	// restore is the first offset that a restart reads after a seek write with
	// an unknown outcome, set when the partition had no close point before the
	// write. Unlike a read, that write can move the stored offset back or delete
	// it, so until a batch arrives Close restores this point: the offset after
	// the stored offset from before the write, or 0 without one.
	restore    *uint64
	counted    *iggcon.Checkpoint
	stored     *iggcon.Checkpoint
	sinceStore uint64
}

func (p *partition) deliveredHere(checkpoint iggcon.Checkpoint) bool {
	return p.delivered != nil && p.firstDelivered != nil && sameIncarnationAndOwner(checkpoint.Context(), p.delivered.Context()) &&
		checkpoint.Offset() >= *p.firstDelivered && checkpoint.Offset() <= p.delivered.Offset()
}

// pin gives a position that follows the stored offset the offset after the
// one that a seek found before its write. Without a stored offset, Next reads
// from offset 0, as the server answers it, and a start stays the start.
func (p *partition) pin(before *iggcon.ConsumerOffsetInfo) {
	p.checkStored = false
	switch {
	case before != nil:
		p.position = iggcon.OffsetPollingStrategy(before.StoredOffset + 1)
	case p.position.Kind == iggcon.POLLING_NEXT:
		p.position = iggcon.OffsetPollingStrategy(0)
	}
	p.fromRead = p.position.Context != nil
}

// takeContext keeps the context that a read gave. A context of a lost
// membership names owner generation 0 instead, which the server binds to the
// owner that it installed for the member, until a read names that owner.
func (p *partition) takeContext(context iggcon.PartitionContext) {
	if p.ownerLost(context) {
		context.OwnerGeneration = 0
	}
	p.seenOwner = max(p.seenOwner, context.OwnerGeneration)
	p.context = context
}

// writeContext returns the context of an offset write of p from source, a
// context that a read of p gave, with the current owner of p instead of an
// owner of a lost membership. The server binds owner 0 only to an owner that
// it installed at or below the metadata op of the write, and a new session
// installs it after every op of the session before. The partition stayed, so
// owner 0 goes with the op that the client saw committed when that is higher,
// and with the incarnation of source, which the server checks.
func (r *PartitionReader) writeContext(p *partition, source iggcon.PartitionContext) iggcon.PartitionContext {
	if !p.ownerLost(source) {
		return source
	}
	source.OwnerGeneration = p.context.OwnerGeneration
	if source.OwnerGeneration == 0 {
		source.MetadataOp = max(source.MetadataOp, r.transport.MetadataWatermark())
	}
	return source
}

// ownerLost reports whether context names an owner of a membership that the member lost.
func (p *partition) ownerLost(context iggcon.PartitionContext) bool {
	return p.lostOwner > 0 && context.OwnerGeneration <= p.lostOwner
}

type entry struct {
	partition  int64
	generation uint64
	batch      *iggcon.PolledMessage
	err        *ReadError
}

type uncounted struct {
	partition  uint32
	generation uint64
	checkpoint iggcon.Checkpoint
	messages   uint64
}

// PartitionReader reads the partitions of a topic in batches, with routing,
// prefetch and the position of each partition inside. Next returns the next
// batch of one partition, in offset order within one generation of the
// partition: from its first read, from each Seek, and from each new assignment.
//
// With CommitManual, Commit stores the offset of a delivered checkpoint. With
// CommitOnRead the server stores each read, the reader reads only when Next
// waits, and after a read whose reply was lost it reads again by offset,
// without a store, the messages that the lost read stored, and delivers them
// first. A partition that leaves the assignment still hands over the reads
// that the server stored for it. Close moves the offset of each partition with
// undelivered batches back to its last delivered message. A partition that
// delivered none moves before the first of those batches. A partition whose
// reads returned no batch moves back only over what they stored. Close does
// not move the offset of a partition that left, so its batches that Next did
// not return before Close are lost for the group. With CommitAuto the reader
// counts each batch as the policy says, or each Mark, and stores the latest
// counted position at its interval or event, before a rebalance takes a
// partition away, and in Close. It abandons a store with an unknown outcome,
// so that store does not block the next store of the partition.
type PartitionReader struct {
	client    iggcon.Client
	transport transport
	stream    iggcon.Identifier
	topic     iggcon.Identifier
	consumer  iggcon.Consumer
	config    config
	slots     int
	group     bool
	onRead    bool
	auto      *iggcon.AutoCommit

	mu              sync.Mutex
	partitions      map[uint32]*partition
	seekGenerations map[uint32]uint64
	queue           []entry
	waiting         int
	changed         chan struct{}
	groups          [][]uint32
	regroup         bool
	grouping        bool
	regroupAt       time.Time
	groupCursor     int
	serves          uint64
	reads           int
	refreshing      bool
	refreshAt       time.Time
	lastGen         uint64
	uncounted       *uncounted
	retiring        []iggcon.Checkpoint
	closed          bool
	// singleReads are the reads in flight of one partition alone: of a gap, of
	// a section that did not decode, and the polls that find the offset of a
	// timestamp or Last position.
	singleReads int
	// nextSingle is the partition from which the reads of one partition alone take turns.
	nextSingle uint32
	// refreshPause is the pause after the last sync that read the assignment.
	refreshPause time.Duration
	// joined marks a join whose assignment no sync has read yet.
	joined bool
	// uncertainJoin is the operation of a rejoin with an unknown outcome. The
	// client blocks its metadata writes until the reader resolves or abandons
	// it, also those of the application. Only one refresh runs at a time, and
	// Close reads it after the last one.
	uncertainJoin *uint64
	// seeking counts the seeks of each partition. No read of the partition
	// starts until they move or fail.
	seeking map[uint32]int
	// committing counts the committing reads in flight of each partition, also
	// of a generation that left the assignment, which a seek waits for.
	committing map[uint32]int
	// seekWriting marks each partition whose seek writes and moves under
	// OnRead, so the writes and moves of two seeks of a partition never
	// interleave. A later seek of the partition waits for it only as long as
	// its context lasts. The seeks of other partitions do not wait.
	seekWriting map[uint32]bool

	storeDue chan struct{}
	wake     chan struct{}
	run      context.Context
	stop     context.CancelFunc
	inFlight sync.WaitGroup
	loopDone chan struct{}
	// closeDone closes when the first Close ends, with its result in closeErr.
	closeDone chan struct{}
	closeErr  error
}

// NewPartitionReader starts a reader. A consumer group reader joins its group first.
// It abandons a join with an unknown outcome and returns its error, so that the
// join does not block the metadata writes of the client. If that join landed,
// the client stays a member of the group until its session ends.
func NewPartitionReader(
	ctx context.Context,
	client iggcon.Client,
	streamId, topicId iggcon.Identifier,
	consumer iggcon.Consumer,
	options ...Option,
) (*PartitionReader, error) {
	for {
		wrapper, wraps := client.(interface{ Unwrap() iggcon.Client })
		if _, reads := client.(transport); reads || !wraps {
			break
		}
		client = wrapper.Unwrap()
	}
	fetcher, ok := client.(transport)
	if !ok {
		return nil, fmt.Errorf("%w: the client cannot read partitions in groups", ierror.ErrFeatureUnavailable)
	}
	cfg := config{
		start:         iggcon.NextPollingStrategy(),
		options:       iggcon.WaitPollOptions(DefaultMaxWait),
		retryInterval: DefaultRetryInterval,
		logger:        slog.New(slog.DiscardHandler),
	}
	for _, option := range options {
		option(&cfg)
	}
	if err := cfg.options.Validate(); err != nil {
		return nil, err
	}
	group := consumer.Kind == iggcon.ConsumerKindGroup
	if _, bound := cfg.start.Partition(); bound || group == (len(cfg.partitions) > 0) || cfg.retryInterval <= 0 {
		return nil, fmt.Errorf("%w: a standalone reader needs its partitions, a group reader reads its assignment, "+
			"and the start cannot be the continuation of one partition", ierror.ErrInvalidCommand)
	}
	cfg.start = withoutZeroContext(cfg.start)
	if cfg.start.Kind == iggcon.POLLING_FIRST {
		// Under OnRead a stored offset wins over the start, and without one the
		// server answers Next from offset 0.
		if cfg.options.Commit.Kind == iggcon.CommitOnRead {
			cfg.start = iggcon.NextPollingStrategy()
		} else {
			cfg.start = iggcon.OffsetPollingStrategy(0)
		}
	}
	prefetch := uint64(cfg.prefetchMessages)
	if prefetch == 0 {
		prefetch = DefaultPrefetchMessages
	}
	// A budget beyond the int range would wrap to no slots on a 32-bit platform.
	slots := min(max(minimumPrefetchSlots, prefetch/uint64(cfg.options.Count)), math.MaxInt)
	run, stop := context.WithCancel(context.WithoutCancel(ctx))
	r := &PartitionReader{
		client: client, transport: fetcher, stream: streamId, topic: topicId, consumer: consumer, config: cfg,
		slots: int(slots), group: group,
		onRead:     cfg.options.Commit.Kind == iggcon.CommitOnRead,
		partitions: make(map[uint32]*partition), changed: make(chan struct{}), regroup: true,
		seekGenerations: make(map[uint32]uint64), seeking: make(map[uint32]int), committing: make(map[uint32]int),
		storeDue: make(chan struct{}, 1), wake: make(chan struct{}, 1), run: run, stop: stop,
		loopDone: make(chan struct{}), closeDone: make(chan struct{}), seekWriting: make(map[uint32]bool),
	}
	if cfg.options.Commit.Kind == iggcon.CommitAuto {
		auto := cfg.options.Commit.Auto
		r.auto = &auto
	}
	if group {
		if err := client.JoinConsumerGroup(ctx, streamId, topicId, consumer.Id); err != nil {
			// No reader remains to resolve a join with an unknown outcome later.
			r.abandonUncertain(err)
			stop()
			return nil, err
		}
		r.joined = true
	} else {
		for _, id := range cfg.partitions {
			r.addPartition(id)
		}
	}
	go r.loop()
	r.signal()
	return r, nil
}

// Next returns the next batch of one partition, never empty. A cancelled call
// leaves its batch in the reader. It fails with a *ReadError for an error of a read.
func (r *PartitionReader) Next(ctx context.Context) (*iggcon.PolledMessage, error) {
	batch, _, err := r.next(ctx, true)
	return batch, err
}

// next returns the next batch and the generation of its partition that read it.
func (r *PartitionReader) next(ctx context.Context, delivers bool) (*iggcon.PolledMessage, uint64, error) {
	r.mu.Lock()
	if delivers {
		r.countUncountedLocked()
	}
	for {
		if r.closed {
			r.mu.Unlock()
			return nil, 0, ierror.ErrClientShutdown
		}
		for len(r.queue) > 0 {
			head := r.queue[0]
			r.queue = r.queue[1:]
			if !r.currentLocked(head) {
				continue
			}
			if head.err != nil {
				r.mu.Unlock()
				r.signal()
				return nil, 0, head.err
			}
			// A batch of an earlier assignment names an owner that the server
			// replaced, so it is no close point of the partition.
			if p := r.partitions[uint32(head.partition)]; delivers && p != nil && p.generation == head.generation {
				r.deliveredLocked(p, head.batch)
			}
			r.mu.Unlock()
			r.signal()
			return head.batch, head.generation, nil
		}
		r.waiting++
		changed := r.changed
		r.mu.Unlock()
		r.signal()
		select {
		case <-changed:
		case <-ctx.Done():
			r.mu.Lock()
			r.waiting--
			r.mu.Unlock()
			return nil, 0, ctx.Err()
		}
		r.mu.Lock()
		r.waiting--
	}
}

// Mark counts a checkpoint that this reader delivered, under CommitAuto. With
// CommitAfterMarked only marks count. It fails with ierror.ErrInvalidCommand
// without CommitAuto, and for a checkpoint of another generation.
func (r *PartitionReader) Mark(checkpoint iggcon.Checkpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.auto == nil {
		return ierror.ErrInvalidCommand
	}
	p := r.partitions[checkpoint.PartitionID()]
	if p == nil || !p.deliveredHere(checkpoint) {
		return ierror.ErrInvalidCommand
	}
	r.countLocked(p, checkpoint, 1, false)
	return nil
}

// Commit stores the offset of a checkpoint that this reader delivered. A
// checkpoint of an older generation, or at or below the last commit of its
// generation, stores nothing. It fails with ierror.ErrInvalidCommand under
// CommitOnRead and for a partition that the reader does not read. It abandons
// a store with an unknown outcome and returns its error, so a retry can store.
func (r *PartitionReader) Commit(ctx context.Context, checkpoint iggcon.Checkpoint) error {
	r.mu.Lock()
	if r.onRead {
		r.mu.Unlock()
		return ierror.ErrInvalidCommand
	}
	p := r.partitions[checkpoint.PartitionID()]
	if p == nil {
		r.mu.Unlock()
		return ierror.ErrInvalidCommand
	}
	if !p.deliveredHere(checkpoint) || (p.committed != nil && checkpoint.Offset() <= *p.committed) {
		r.mu.Unlock()
		return nil
	}
	generation := p.generation
	position := checkpointPosition(checkpoint)
	position.Context = r.writeContext(p, position.Context)
	r.mu.Unlock()
	if err := r.writePosition(ctx, ctx, r.client.StoreConsumerPosition, p, position); err != nil {
		// A retry of the commit, and every later write of the partition, can then go out.
		r.abandonUncertain(err)
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if p.generation == generation && (p.committed == nil || checkpoint.Offset() > *p.committed) {
		offset := checkpoint.Offset()
		p.committed = &offset
	}
	return nil
}

// Seek sets the next read position of a partition and starts a new generation
// of it. It drops the queued batches of the partition and resumes a stopped
// one. From its start until it moves or fails, no read of the partition
// starts. Under CommitOnRead the seek first waits for the committing read in
// flight of the partition and for the write of an earlier seek of the
// partition. That read can wait on the server up to MaxWait. The seek does not
// wait for the seeks of other partitions. Then a seek to an offset above 0 stores the
// offset before it, so the next read of the server starts there. If the
// partition has a stored offset, a seek to offset 0 or First deletes it
// instead, so the reads replay the partition. A seek to a timestamp first
// finds the first message at it with a read that stores nothing. Then it seeks
// to the offset of that message. Without such a message it stores nothing. The
// seek moves only after these writes succeed. A failed seek returns the error
// and leaves the partition as it was. After a write with an unknown outcome,
// the reader reads again what a jump of the stored offset skips. Close then
// restores the close point from before the write: the last delivered message,
// or the offset before the first message that the reader read and did not
// deliver. Without such a point, the seek looks up the stored offset before
// its write, and Close stores that offset again, or deletes the offset if none
// was stored. A batch that arrives after the write decides instead. A
// partition whose position follows the stored offset, such as a Next start
// before its first read, names no offset. The seek looks up the stored offset
// for it too, and after a write with an unknown outcome the partition reads on
// after that stored offset. A seek whose ctx ends
// before its write changes nothing. A seek whose ctx ends after it sent its
// write returns at once and finishes in the background, and a later seek of
// the partition and Close wait for it. The logger reports a failure of that
// write.
func (r *PartitionReader) Seek(ctx context.Context, partitionID uint32, start iggcon.PollingStrategy) error {
	return r.seek(ctx, partitionID, start, func(move func()) { move() })
}

// seek runs a seek. It holds back the reads of the partition from its start,
// and under OnRead it writes the stored offset first. moved runs the move of
// the seek, and a caller wraps it to drop what it holds of the partition. A
// failed seek ends the hold and changes nothing.
func (r *PartitionReader) seek(ctx context.Context, partitionID uint32, start iggcon.PollingStrategy, moved func(move func())) error {
	if err := start.RequirePartition(partitionID); err != nil {
		return err
	}
	start = withoutZeroContext(start)
	if start.Kind == iggcon.POLLING_FIRST {
		start = iggcon.OffsetPollingStrategy(0)
	}
	r.mu.Lock()
	if r.partitions[partitionID] == nil || r.closed {
		r.mu.Unlock()
		return ierror.ErrInvalidCommand
	}
	r.seeking[partitionID]++
	// Close waits for the seek, so its rewind starts from where the seek moved.
	r.inFlight.Add(1)
	r.mu.Unlock()
	if !r.onRead {
		moved(func() { r.moveSeek(partitionID, start) })
		r.endSeek(partitionID)
		return nil
	}
	written, left := make(chan error), make(chan struct{})
	go func() {
		err := r.writeSeek(ctx, partitionID, start, moved)
		select {
		case written <- err:
		case <-left:
			// The caller has the end of its ctx, so only the log reports a failed write.
			if err != nil && !errors.Is(err, ctx.Err()) {
				r.config.logger.Warn("A seek failed after its caller stopped waiting", "partition", partitionID, "error", err)
			}
		}
	}()
	select {
	case err := <-written:
		return err
	case <-ctx.Done():
		close(left)
		return ctx.Err()
	}
}

// withoutZeroContext drops a fully zero context from start. It names no
// history and no owner, so it counts as none.
func withoutZeroContext(start iggcon.PollingStrategy) iggcon.PollingStrategy {
	if start.Context != nil && *start.Context == (iggcon.PartitionContext{}) {
		start.Context = nil
	}
	return start
}

// writeSeek writes the stored offset of a seek under OnRead, moves the
// partition on success, and ends the hold. A committing read never moves the
// stored offset back, and the server answers the next read after it. So the
// write waits for the committing reads in flight and goes before the next
// read. A sent write ignores the end of ctx, so the seek still moves after its
// caller stopped waiting.
func (r *PartitionReader) writeSeek(ctx context.Context, partitionID uint32, start iggcon.PollingStrategy, moved func(move func())) error {
	defer r.endSeek(partitionID)
	start, err := r.seekStart(ctx, partitionID, start)
	if err != nil {
		return err
	}
	r.mu.Lock()
	for r.committing[partitionID] > 0 || r.seekWriting[partitionID] {
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
		r.mu.Lock()
	}
	r.seekWriting[partitionID] = true
	defer func() {
		r.mu.Lock()
		delete(r.seekWriting, partitionID)
		r.broadcastLocked()
		r.mu.Unlock()
	}()
	p := r.partitions[partitionID]
	if p == nil {
		r.mu.Unlock()
		return ierror.ErrInvalidCommand
	}
	source := p.context
	if start.Context != nil {
		source = *start.Context
	}
	position := iggcon.ConsumerPosition{PartitionId: partitionID, Context: r.writeContext(p, source)}
	generation := p.generation
	// A position that follows the stored offset names no offset, so after a
	// store or delete with an unknown outcome only the stored offset from before
	// the write tells where the next read starts.
	pins := start.Kind == iggcon.POLLING_OFFSET &&
		(p.checkStored || p.baseline == nil && p.position.Kind == iggcon.POLLING_NEXT)
	// No offset comes before 0 and a read never moves the stored offset back,
	// so only a delete lets the reads replay the partition. The server refuses
	// the delete of a missing offset, so the seek looks it up first.
	deletes := start.Kind == iggcon.POLLING_OFFSET && start.Value == 0
	// A delivered message, or the first message of a read that the reader did
	// not deliver, tells Close where to move the offset back. A position that a
	// check poll found is no such point, because no read came from it.
	_, queued := r.firstQueuedLocked(p)
	closePoint := queued || p.restore != nil || p.delivered != nil ||
		p.baseline != nil && p.readStarted && (p.received != nil || p.probed)
	r.mu.Unlock()
	var before *iggcon.ConsumerOffsetInfo
	if pins || deletes || start.Kind == iggcon.POLLING_OFFSET && !closePoint {
		if before, err = r.client.GetConsumerOffset(ctx, r.consumer, r.stream, r.topic, &partitionID); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Once the caller of the seek stops waiting, the reader is the caller of
	// the write, so Close ends the read of a write refused as not the owner.
	write := context.WithoutCancel(ctx)
	switch {
	case start.Kind != iggcon.POLLING_OFFSET:
	case deletes:
		if before != nil {
			err = r.writePosition(write, r.run, r.client.DeleteConsumerPosition, p, position)
		}
	default:
		position.Offset = start.Value - 1
		err = r.writePosition(write, r.run, r.client.StoreConsumerPosition, p, position)
	}
	if r.abandonUncertain(err) {
		r.mu.Lock()
		// The outcome belongs to the generation of the write, so a partition that
		// left and came back meanwhile keeps its new state.
		if p := r.partitions[partitionID]; p != nil && p.generation == generation {
			if !closePoint {
				// A read from the baseline that stored, as the stored offset shows,
				// moves back before the baseline, as in the rewind.
				first := uint64(0)
				switch {
				case before == nil:
				case p.baseline != nil && p.readStarted && before.StoredOffset >= *p.baseline:
					first = *p.baseline
				default:
					first = before.StoredOffset + 1
				}
				p.restore = &first
			}
			if pins {
				p.pin(before)
			}
			if p.baseline == nil && p.position.Kind == iggcon.POLLING_OFFSET {
				first := p.position.Value
				p.baseline = &first
			}
			// The write can have moved the stored offset, as a lost read from
			// the baseline can. So the gap check reads again what the next read
			// skips.
			p.lostRead = true
			p.readStarted = p.readStarted || p.baseline != nil
		}
		r.mu.Unlock()
	}
	if err != nil {
		return err
	}
	moved(func() { r.moveSeek(partitionID, start) })
	return nil
}

// seekStart returns the start that a seek applies under OnRead. A committing
// read never moves the stored offset back, so a read at a timestamp behind it
// would jump to it. A seek to a timestamp becomes a seek to the offset of the
// first message at it, and stays as it is while no message is there.
func (r *PartitionReader) seekStart(ctx context.Context, partitionID uint32, start iggcon.PollingStrategy) (iggcon.PollingStrategy, error) {
	if start.Kind != iggcon.POLLING_TIMESTAMP {
		return start, nil
	}
	// A message that arrives later lies after the stored offset, so the seek waits for none.
	polled, err := r.pollFirst(ctx, partitionID, start, 0)
	if err != nil || polled == nil || len(polled.Messages) == 0 {
		return start, err
	}
	return iggcon.OffsetPollingStrategy(polled.Messages[0].Header.Offset).WithContext(polled.Context), nil
}

// moveSeek moves the partition of a seek to start, unless it left the assignment.
func (r *PartitionReader) moveSeek(partitionID uint32, start iggcon.PollingStrategy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p := r.partitions[partitionID]; p != nil {
		r.moveLocked(p, start)
	}
}

func (r *PartitionReader) endSeek(partitionID uint32) {
	r.mu.Lock()
	r.seeking[partitionID]--
	r.mu.Unlock()
	r.inFlight.Done()
	r.signal()
}

// Partitions are the partitions that the reader reads now.
func (r *PartitionReader) Partitions() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]uint32, 0, len(r.partitions))
	for id := range r.partitions {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Close stops the reader and waits for its reads, which it lets end, and for
// each seek in progress. With CommitAuto it counts the last batch under
// CommitAfterProcessing and stores the latest counted position of each
// partition. With CommitOnRead it moves the offset of each partition with
// undelivered batches back to its last delivered message. A partition that
// delivered none moves before the first of those batches. A partition whose
// reads returned no batch moves back only over what they stored. A partition
// that left keeps its offset, so its batches that Next did not return are lost
// for the group. A group reader then leaves its group. It returns the first
// failed lookup, store, delete or leave. It abandons a store or delete with an
// unknown outcome, so that write does not block a later write of the partition
// on the same client. For the same reason it abandons a rejoin that is still
// unknown and a leave with an unknown outcome, which block the metadata writes
// of the client. The reads for the installed owner after a refusal as not the
// owner end at the end of ctx, or 10 s after the start of Close, and that
// refusal stands. Each lookup and write of Close and the leave still go once,
// within the request timeout of the client.
// With CommitAuto it first waits for the background stores that run, each
// within that timeout too.
// A later Close waits for the first one and returns its result, or the error
// of its own ctx when that ends first.
func (r *PartitionReader) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		select {
		case <-r.closeDone:
			return r.closeErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.closed = true
	r.countUncountedLocked()
	r.broadcastLocked()
	r.mu.Unlock()
	retry, cancel := context.WithTimeout(ctx, closeBudget)
	defer cancel()
	r.stop()
	<-r.loopDone
	r.inFlight.Wait()
	// The end of retry ends only the reads for the installed owner. Each write
	// of Close and the leave still go once, within the request timeout of the
	// client, so a cancelled Close still moves the offsets back and leaves.
	detached := context.WithoutCancel(ctx)
	var failure error
	if r.onRead {
		if err := r.rewind(detached, retry); err != nil && failure == nil {
			failure = err
		}
	}
	if r.auto != nil {
		if err := r.flush(detached, retry); err != nil && failure == nil {
			failure = err
		}
	}
	if r.group {
		if r.uncertainJoin != nil {
			r.abandon(*r.uncertainJoin)
		}
		// A member that the server no longer knows, after a lost session, has left already.
		err := r.client.LeaveConsumerGroup(detached, r.stream, r.topic, r.consumer.Id)
		r.abandonUncertain(err)
		if err != nil && !errors.Is(err, ierror.ErrConsumerGroupMemberNotFound) && failure == nil {
			failure = err
		}
	}
	r.closeErr = failure
	close(r.closeDone)
	return failure
}

func (r *PartitionReader) addPartition(id uint32) {
	r.lastGen++
	p := &partition{id: id, generation: r.lastGen}
	r.takeStart(p)
	r.partitions[id] = p
}

// takeStart puts p at the start. Under OnRead the server answers Next from a
// stored offset, which wins over any other start, so the first committing read
// of a partition at such a start looks the stored offset up first.
func (r *PartitionReader) takeStart(p *partition) {
	p.position = r.config.start
	p.fromRead = r.config.start.Context != nil
	p.checkStored = r.onRead && r.config.start.Kind != iggcon.POLLING_NEXT
}

func (r *PartitionReader) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *PartitionReader) broadcastLocked() {
	close(r.changed)
	r.changed = make(chan struct{})
}

func (r *PartitionReader) loop() {
	defer close(r.loopDone)
	timer := time.NewTimer(r.config.retryInterval)
	defer timer.Stop()
	var ticks <-chan time.Time
	if r.auto != nil && r.auto.Interval > 0 {
		ticker := time.NewTicker(r.auto.Interval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	// flushed closes when the flush in the background ends. Until then a tick
	// or a due store waits, so one flush runs at a time, and the next one
	// stores the latest positions.
	var flushed <-chan struct{}
	for {
		tick, due := ticks, r.storeDue
		if flushed != nil {
			tick, due = nil, nil
		}
		select {
		case <-r.run.Done():
			return
		case <-r.wake:
		case <-timer.C:
		case <-flushed:
			flushed = nil
		case <-tick:
			flushed = r.flushInBackground()
		case <-due:
			flushed = r.flushInBackground()
		}
		next := r.pump()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(next)
	}
}

// pump sends what the budget and the demand allow, and returns when it wants
// to be called again at the latest.
func (r *PartitionReader) pump() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.config.retryInterval
	}
	now := time.Now()
	if r.group && !r.refreshing && !now.Before(r.refreshAt) {
		r.refreshing = true
		r.inFlight.Add(1)
		go r.refreshAssignment()
	}
	if r.regroup && !r.grouping && len(r.partitions) > 0 && !now.Before(r.regroupAt) {
		r.grouping = true
		r.regroup = false
		ids := make([]uint32, 0, len(r.partitions))
		for id := range r.partitions {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		r.inFlight.Add(1)
		go r.regroupPartitions(ids)
	}
	// These reads commit nothing, so they go without demand. All reads go on
	// with the last good groups while a lookup runs or after it failed.
	r.readSinglesLocked(now)
	// Under OnRead each read stores its batch, so the reads go only on demand.
	if len(r.groups) > 0 && (!r.onRead || (r.waiting > 0 && len(r.queue) == 0)) {
		r.readGroupsLocked(now)
	}
	wait := r.config.retryInterval
	// A sync or a lookup in flight wakes the loop when it ends.
	if r.group && !r.refreshing {
		wait = min(wait, r.refreshAt.Sub(now))
	}
	if r.regroup && !r.grouping && len(r.partitions) > 0 {
		wait = min(wait, r.regroupAt.Sub(now))
	}
	for _, p := range r.partitions {
		if !p.inFlight && p.stopped == nil && p.retryAt.After(now) {
			wait = min(wait, p.retryAt.Sub(now))
		}
	}
	return max(wait, time.Millisecond)
}

// readSinglesLocked is the only place that reads a gap, a section that did
// not decode, or the first message at a timestamp or Last position, one
// partition per read. These reads leave a request per group, so they may not
// all fit, and they take turns in partition order from the partition after the
// last one that read.
func (r *PartitionReader) readSinglesLocked(now time.Time) {
	var waiting []*partition
	for _, p := range r.partitions {
		if !p.inFlight && p.stopped == nil && !p.misrouted && r.seeking[p.id] == 0 && !p.retryAt.After(now) &&
			(p.gapFrom != nil || p.aloneUntil != nil || !p.checkStored && !fetchable(p.position)) {
			waiting = append(waiting, p)
		}
	}
	sort.Slice(waiting, func(i, j int) bool { return waiting[i].id < waiting[j].id })
	first := sort.Search(len(waiting), func(i int) bool { return waiting[i].id >= r.nextSingle })
	for turn := range len(waiting) {
		p := waiting[(first+turn)%len(waiting)]
		var started bool
		if p.gapFrom != nil {
			started = r.readGapLocked(p)
		} else {
			started = r.readLocked([]iggcon.FetchMessagesTarget{{PartitionId: p.id, Strategy: p.position}}, 1, false, false, true)
		}
		if !started {
			return
		}
		// After the largest partition ID this wraps to 0, so the turn starts over.
		r.nextSingle = p.id + 1
	}
}

// readGroupsLocked gives every group with ready partitions one read, the
// groups without a read that may wait in flight first, and deals the free
// slots left to the backlogged partitions, one read each with the whole count
// and bytes. The quiet partitions of a group share one read, which may wait on
// the server. Under OnRead that read stores, so a group holds one at a time.
func (r *PartitionReader) readGroupsLocked(now time.Time) {
	budget := min(r.slots-len(r.queue), maxReadsInFlight) - r.reads
	if budget <= 0 {
		return
	}
	type groupRead struct {
		turn              int
		index             int
		ready             []iggcon.FetchMessagesTarget
		quiet, backlogged []iggcon.FetchMessagesTarget
		extra             int
	}
	var idle, reading []*groupRead
	first := r.groupCursor
	for turn := range len(r.groups) {
		index := (first + turn) % len(r.groups)
		parked := r.groupParksLocked(r.groups[index])
		ready := r.readyTargetsLocked(r.groups[index], now, parked)
		if len(ready) == 0 {
			continue
		}
		read := &groupRead{turn: turn, index: index, ready: ready}
		for _, target := range ready {
			if r.partitions[target.PartitionId].backlogged {
				read.backlogged = append(read.backlogged, target)
			} else {
				read.quiet = append(read.quiet, target)
			}
		}
		if parked {
			reading = append(reading, read)
		} else {
			idle = append(idle, read)
		}
	}
	chosen := append(idle, reading...)
	chosen = chosen[:min(len(chosen), budget)]
	sort.Slice(chosen, func(i, j int) bool { return chosen[i].turn < chosen[j].turn })
	// Every read holds at least one partition.
	for extra, dealt := budget-len(chosen), true; extra > 0 && dealt; {
		dealt = false
		for _, read := range chosen {
			limit := len(read.backlogged)
			if len(read.quiet) == 0 {
				limit--
			}
			if extra > 0 && read.extra < limit {
				read.extra++
				extra--
				dealt = true
			}
		}
	}
	for _, read := range chosen {
		var reads [][]iggcon.FetchMessagesTarget
		switch {
		case read.extra == 0:
			reads = [][]iggcon.FetchMessagesTarget{read.ready}
		case len(read.quiet) == 0:
			reads = dealTargets(read.backlogged, read.extra+1)
		default:
			reads = append([][]iggcon.FetchMessagesTarget{read.quiet}, dealTargets(read.backlogged, read.extra)...)
		}
		for at, targets := range reads {
			r.readLocked(targets, r.config.options.Count, r.onRead, at == 0 && len(read.quiet) > 0, false)
		}
		r.groupCursor = read.index + 1
	}
}

// groupParksLocked reports whether a read of the group that may wait is in flight.
func (r *PartitionReader) groupParksLocked(group []uint32) bool {
	for _, id := range group {
		if p := r.partitions[id]; p != nil && p.inFlight && p.parked {
			return true
		}
	}
	return false
}

// readyTargetsLocked puts the partitions served last at the end, since a fetch
// that reaches its count serves its first targets. Under OnRead a quiet
// partition waits for the read of the group that may wait, since a second one
// would store. A partition that a seek holds waits for the seek, and one at a
// timestamp or Last position for the poll that finds its offset.
func (r *PartitionReader) readyTargetsLocked(group []uint32, now time.Time, parked bool) []iggcon.FetchMessagesTarget {
	var ready []*partition
	for _, id := range group {
		p := r.partitions[id]
		if p != nil && !p.inFlight && p.stopped == nil && !p.misrouted && r.seeking[id] == 0 && p.gapFrom == nil && p.aloneUntil == nil &&
			(p.checkStored || fetchable(p.position)) && !p.retryAt.After(now) && (p.backlogged || !r.onRead || !parked) {
			ready = append(ready, p)
		}
	}
	sort.SliceStable(ready, func(i, j int) bool { return ready[i].servedAt < ready[j].servedAt })
	targets := make([]iggcon.FetchMessagesTarget, 0, len(ready))
	for _, p := range ready {
		targets = append(targets, iggcon.FetchMessagesTarget{PartitionId: p.id, Strategy: p.position})
	}
	return targets
}

// readLocked starts a read and reports whether it started.
func (r *PartitionReader) readLocked(targets []iggcon.FetchMessagesTarget, count uint32, commit, parked, single bool) bool {
	// A read of one partition alone can wait on the server, so these reads leave
	// a request for the read of each group. A read that does not start here stays
	// in the state of its partitions, and the pump sends it once a read ends.
	if r.reads >= maxReadsInFlight || single && r.singleReads >= max(maxReadsInFlight-len(r.groups), 1) {
		return false
	}
	if single {
		r.singleReads++
	}
	generations := make(map[uint32]uint64, len(targets))
	for _, target := range targets {
		p := r.partitions[target.PartitionId]
		p.inFlight = true
		p.parked = parked
		generations[target.PartitionId] = p.generation
		if commit {
			r.committing[target.PartitionId]++
		}
	}
	r.reads++
	r.inFlight.Add(1)
	options := r.fetchOptions(count)
	go func() {
		defer r.inFlight.Done()
		prepared := targets
		var err error
		if commit {
			prepared, err = r.prepareRead(targets, generations, options)
		}
		var sections []iggcon.FetchMessagesSection
		var found *iggcon.PolledMessage
		// Only a read of one partition alone takes a position that a fetch cannot read from.
		finds := len(prepared) == 1 && !fetchable(prepared[0].Strategy)
		switch {
		case err != nil || len(prepared) == 0:
		case finds:
			found, err = r.pollFirst(r.run, prepared[0].PartitionId, prepared[0].Strategy, options.MaxWait)
		default:
			sections, err = r.transport.FetchMessages(r.run, r.stream, r.topic, r.consumer, prepared, count, commit, options)
			if commit {
				r.abandonLost(sections, err)
			}
		}
		r.mu.Lock()
		r.reads--
		if single {
			r.singleReads--
		}
		for id, generation := range generations {
			// A seek or a new assignment started a new generation, whose own read can be in flight.
			if p := r.partitions[id]; p != nil && p.generation == generation {
				p.inFlight = false
			}
			if commit {
				r.committing[id]--
			}
		}
		switch {
		case !r.closed:
			switch {
			case finds:
				r.foundLocked(prepared[0], generations[prepared[0].PartitionId], found, err)
			case err != nil:
				r.refuseLocked(generations, err)
			default:
				r.acceptLocked(generations, commit, sections)
			}
		case commit:
			r.settleOnCloseLocked(generations, sections, err)
		}
		r.broadcastLocked()
		r.mu.Unlock()
		r.signal()
	}()
	return true
}

// fetchOptions are the options of a fetch of count messages.
func (r *PartitionReader) fetchOptions(count uint32) iggcon.DeferredPollOptions {
	options := r.config.options.Deferred()
	// A read of a lost range or of one message asks for fewer messages than the reader.
	options.MinCount = min(options.MinCount, count)
	// A fetch needs the reply margin also without a wait.
	options.RequestTimeout = max(options.RequestTimeout, options.MaxWait+iggcon.MinDeferredPollReplyMargin+time.Second)
	return options
}

// probe reads one message of target without a store. It returns a refusal of
// the partition apart from an error of the request. Only the context and the
// offsets of the section count, so it does not decode the message, and a
// message that does not decode fails only the read that delivers it.
func (r *PartitionReader) probe(ctx context.Context, target iggcon.FetchMessagesTarget, options iggcon.DeferredPollOptions,
) (probed *iggcon.FetchMessagesSection, refusal, err error) {
	options.MinCount = 1
	sections, err := r.transport.FetchMessages(ctx, r.stream, r.topic, r.consumer, []iggcon.FetchMessagesTarget{target}, 1, false, options)
	for index := range sections {
		section := &sections[index]
		if section.PartitionId != target.PartitionId {
			continue
		}
		if section.Err != nil {
			return nil, section.Err, err
		}
		return section, nil, err
	}
	return nil, nil, err
}

// firstOffset is the offset of the first message of section.
func firstOffset(section iggcon.FetchMessagesSection) uint64 {
	return section.NextOffset - min(uint64(section.MessageCount), section.NextOffset)
}

// pollFirst polls a partition at strategy without a store, and the first
// message of the reply is where the position starts. The server reads a fetch
// only from an offset or Next, so a timestamp or Last position takes a poll of
// its partition alone. A timestamp needs one message. A Last poll asks for the
// count of the reader, so the reads from its first message deliver the last
// count messages.
func (r *PartitionReader) pollFirst(ctx context.Context, partitionID uint32, strategy iggcon.PollingStrategy, maxWait time.Duration,
) (*iggcon.PolledMessage, error) {
	options := r.config.options
	options.MinCount, options.MaxWait, options.Commit = 1, maxWait, iggcon.ManualCommit()
	if strategy.Kind != iggcon.POLLING_LAST {
		options.Count = 1
	}
	return r.client.PollMessages(ctx, r.stream, r.topic, partitionID, r.consumer, strategy, options)
}

// foundLocked moves a partition at a timestamp or Last position to the offset
// of the first message that its poll found. Without one the position stays,
// and the partition polls again. A seek, a new assignment or a reset can move
// the partition while the poll runs, so the poll counts only if the partition
// keeps the generation and the position that the poll started from. A seek
// owns the position while it holds the partition, so a poll that ends then
// does not count either, and the partition polls again after the seek.
func (r *PartitionReader) foundLocked(target iggcon.FetchMessagesTarget, generation uint64, polled *iggcon.PolledMessage, err error) {
	p := r.partitions[target.PartitionId]
	if p == nil || p.generation != generation || p.position != target.Strategy || r.seeking[p.id] > 0 {
		return
	}
	if err != nil {
		r.refuseLocked(map[uint32]uint64{p.id: generation}, err)
		return
	}
	if polled == nil || len(polled.Messages) == 0 {
		if r.config.options.MaxWait == 0 {
			p.retryAt = time.Now().Add(r.config.retryInterval)
		}
		return
	}
	p.lastingRefusals = 0
	p.takeContext(polled.Context)
	p.position = iggcon.OffsetPollingStrategy(polled.Messages[0].Header.Offset).WithContext(p.context)
	p.fromRead = true
}

// prepareRead readies the partitions of a committing read and returns its
// targets. A partition at the start looks up its stored offset first, and
// with one it reads as with a Next start. A partition at Next without a known
// first offset probes it. A partition at a timestamp or Last position waits
// for the poll that finds its offset.
func (r *PartitionReader) prepareRead(targets []iggcon.FetchMessagesTarget, generations map[uint32]uint64, options iggcon.DeferredPollOptions) ([]iggcon.FetchMessagesTarget, error) {
	type preparation struct {
		target           iggcon.FetchMessagesTarget
		lookedUp, stored bool
		lookupErr        error
		probed           bool
		section          *iggcon.FetchMessagesSection
		refusal, err     error
	}
	results := make(chan preparation, len(targets))
	steps := 0
	r.mu.Lock()
	for _, target := range targets {
		p := r.partitions[target.PartitionId]
		if p == nil || p.generation != generations[target.PartitionId] ||
			!p.checkStored && (p.received != nil || p.baseline != nil || p.position.Kind != iggcon.POLLING_NEXT) {
			continue
		}
		// The read sends nothing else while it prepares, so the first partition
		// takes its request. A partition left without one waits for a later read.
		if steps > 0 {
			if r.reads >= maxReadsInFlight {
				break
			}
			r.reads++
		}
		steps++
		lookUp := p.checkStored
		go func() {
			result := preparation{target: target, lookedUp: lookUp}
			if lookUp {
				stored, err := r.client.GetConsumerOffset(r.run, r.consumer, r.stream, r.topic, &target.PartitionId)
				result.stored, result.lookupErr = stored != nil, err
				if err != nil || stored == nil {
					results <- result
					return
				}
				// The server answers Next after the stored offset.
				target.Strategy = iggcon.NextPollingStrategy()
			}
			result.probed = true
			result.target = target
			result.section, result.refusal, result.err = r.probe(r.run, target, options)
			results <- result
		}()
	}
	r.mu.Unlock()
	var failure error
	for remaining := steps; remaining > 0; remaining-- {
		result := <-results
		r.mu.Lock()
		if remaining > 1 {
			r.reads--
			r.signal()
		}
		p := r.partitions[result.target.PartitionId]
		if result.err != nil && failure == nil {
			failure = result.err
		}
		if p != nil && p.generation == generations[result.target.PartitionId] {
			switch {
			case result.lookupErr != nil:
				// A failed lookup holds back its partition only, as a failed read of it does.
				r.refuseLocked(map[uint32]uint64{p.id: p.generation}, result.lookupErr)
			case result.lookedUp && !result.stored:
				p.checkStored = false
			case result.lookedUp:
				p.checkStored = false
				p.position = iggcon.NextPollingStrategy()
				p.fromRead = false
			}
			// A reset of the partition during the probe leaves a position that the probe did not start from.
			if result.probed && result.err == nil && p.position == result.target.Strategy {
				switch {
				case result.refusal != nil:
					// A refusal of one probe affects only its partition, so the other partitions of the read go on.
					r.refuseSectionLocked(p, result.refusal, time.Now())
				case result.section != nil && result.section.MessageCount > 0:
					first := firstOffset(*result.section)
					p.baseline = &first
					p.probed = true
					p.takeContext(result.section.Context)
					p.position = iggcon.NextPollingStrategy().WithContext(p.context)
				default:
					p.retryAt = time.Now().Add(r.config.retryInterval)
				}
			}
		}
		r.mu.Unlock()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if failure != nil || r.closed || r.waiting == 0 {
		return nil, failure
	}
	prepared := make([]iggcon.FetchMessagesTarget, 0, len(targets))
	for _, target := range targets {
		p := r.partitions[target.PartitionId]
		if p == nil || p.generation != generations[target.PartitionId] || r.seeking[p.id] > 0 || p.checkStored || !fetchable(p.position) {
			continue
		}
		if p.received == nil && p.baseline == nil {
			switch p.position.Kind {
			case iggcon.POLLING_NEXT:
				continue
			case iggcon.POLLING_OFFSET:
				// The read starts at its offset, so Close can move the offset back before it.
				first := p.position.Value
				p.baseline = &first
			}
		}
		p.readStarted = true
		prepared = append(prepared, iggcon.FetchMessagesTarget{PartitionId: p.id, Strategy: p.position})
	}
	return prepared, nil
}

func (r *PartitionReader) acceptLocked(generations map[uint32]uint64, commit bool, sections []iggcon.FetchMessagesSection) {
	now := time.Now()
	answered := make(map[uint32]bool, len(sections))
	for _, section := range sections {
		answered[section.PartitionId] = true
		lost := errors.Is(section.Err, ierror.FetchProgressUncertain{})
		p := r.partitions[section.PartitionId]
		generation, read := generations[section.PartitionId]
		if !read || !r.keepsLocked(section.PartitionId, generation) {
			continue
		}
		if commit && (p == nil || p.generation != generation) {
			// The server stored this read, so a partition that left the assignment still hands it over.
			r.receiveStoredLocked(section, generation)
			continue
		}
		if p == nil || p.generation != generations[section.PartitionId] {
			continue
		}
		if lost {
			// The next read of the server shows what the lost read stored, and the gap check reads it again.
			p.lostRead = true
			p.retryAt = now.Add(r.config.retryInterval)
			continue
		}
		if section.Err != nil {
			r.refuseSectionLocked(p, section.Err, now)
			continue
		}
		p.lastingRefusals = 0
		p.takeContext(section.Context)
		r.receiveLocked(p, section, commit, now)
	}
	for id := range generations {
		if p := r.partitions[id]; p != nil && !answered[id] && r.config.options.MaxWait == 0 {
			p.retryAt = now.Add(r.config.retryInterval)
		}
	}
}

func (r *PartitionReader) receiveLocked(p *partition, section iggcon.FetchMessagesSection, commit bool, now time.Time) {
	// An empty partition reports a current offset of 0.
	p.backlogged = section.NextOffset <= section.CurrentOffset && (section.MessageCount > 0 || section.CurrentOffset > 0)
	polled, err := binaryserialization.DeserializeSectionMessages(section)
	if err != nil {
		r.refuseUndecodedLocked(p, section, commit, err)
		return
	}
	expected := p.baseline
	if p.received != nil {
		next := *p.received + 1
		expected = &next
	}
	if len(polled.Messages) == 0 {
		if commit && p.lostRead && p.gapFrom == nil && expected != nil && section.NextOffset > *expected {
			gap := *expected
			p.gapFrom = &gap
			p.gapEnd = section.NextOffset
			p.held = polled
			last := section.NextOffset - 1
			p.received = &last
			return
		}
		if !commit && p.gapFrom != nil {
			if section.NextOffset > *p.gapFrom {
				r.acceptGapLocked(p, polled, section.NextOffset)
			} else {
				p.retryAt = now.Add(r.config.retryInterval)
			}
			return
		}
		if r.config.options.MaxWait == 0 {
			p.retryAt = now.Add(r.config.retryInterval)
		}
		return
	}
	r.serves++
	p.servedAt = r.serves
	first := polled.Messages[0].Header.Offset
	last := polled.Messages[len(polled.Messages)-1].Header.Offset
	switch {
	case commit && p.lostRead && p.gapFrom == nil && expected != nil && first > *expected:
		// A read whose reply was lost stored up to here, so the messages in between are read again first.
		gap := *expected
		p.gapFrom = &gap
		p.gapEnd = first
		p.held = polled
		p.received = &last
	case p.gapFrom != nil && !commit:
		r.acceptGapLocked(p, polled, first)
	default:
		// The position is still the one that the read sent. A read from Next
		// starts after the stored offset, so it shows a jump that a lost read or
		// write made, and its store fences out an older read that is still
		// parked on the server. A read from an offset shows no such jump.
		if commit && p.position.Kind == iggcon.POLLING_NEXT {
			p.lostRead = false
		}
		// A read alone goes over messages that an earlier read returned.
		if p.received == nil || last > *p.received {
			p.received = &last
		}
		r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, batch: polled})
	}
	if p.aloneUntil != nil && (p.gapFrom == nil && last+1 >= *p.aloneUntil || p.gapFrom != nil && *p.gapFrom >= *p.aloneUntil) {
		p.aloneUntil = nil
	}
	switch continuation, _ := polled.Continuation(); {
	case p.aloneUntil != nil && p.gapFrom == nil:
		p.position = continuation
	case r.onRead:
		p.position = iggcon.NextPollingStrategy()
	default:
		p.position = continuation
	}
	p.fromRead = p.position.Context != nil
}

// refuseUndecodedLocked reads a section that does not decode again one
// message at a time, so each good message reaches the application. A message
// that does not decode stops the partition with the continuation after it.
func (r *PartitionReader) refuseUndecodedLocked(p *partition, section iggcon.FetchMessagesSection, commit bool, err error) {
	start := firstOffset(section)
	if commit && section.NextOffset > 0 {
		// The server stored the offset of the section, so Close moves it back over what was not delivered.
		last := section.NextOffset - 1
		if p.received == nil || last > *p.received {
			p.received = &last
		}
	}
	if section.MessageCount > 1 {
		until := section.NextOffset
		p.aloneUntil = &until
		if p.gapFrom == nil {
			p.position = iggcon.OffsetPollingStrategy(start).WithContext(section.Context)
			p.fromRead = true
		}
		return
	}
	skip := iggcon.OffsetPollingStrategy(start + 1).WithContext(section.Context)
	failure := &ReadError{PartitionID: &p.id, Err: err, Skip: &skip}
	p.stopped = failure
	r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, err: failure})
}

// receiveStoredLocked queues the messages of a stored read of a partition that left the assignment.
func (r *PartitionReader) receiveStoredLocked(section iggcon.FetchMessagesSection, generation uint64) {
	if section.Err != nil {
		return
	}
	polled, err := binaryserialization.DeserializeSectionMessages(section)
	if err == nil && len(polled.Messages) > 0 {
		r.queue = append(r.queue, entry{partition: int64(section.PartitionId), generation: generation, batch: polled})
	}
}

// settleOnCloseLocked records what a committing read that ended during Close
// stored, so that the rewind moves the offset back over it.
func (r *PartitionReader) settleOnCloseLocked(generations map[uint32]uint64, sections []iggcon.FetchMessagesSection, err error) {
	if errors.Is(err, ierror.FetchProgressUncertain{}) {
		for id, generation := range generations {
			if p := r.partitions[id]; p != nil && p.generation == generation {
				p.lostRead = true
			}
		}
		return
	}
	for _, section := range sections {
		p := r.partitions[section.PartitionId]
		generation, read := generations[section.PartitionId]
		if p == nil || !read || p.generation != generation {
			continue
		}
		switch {
		case errors.Is(section.Err, ierror.FetchProgressUncertain{}):
			p.lostRead = true
		case section.Err == nil && section.MessageCount > 0 && section.NextOffset > 0:
			if p.baseline == nil && p.received == nil {
				// The first batch of the generation ended unseen, so the rewind moves the offset before it.
				first := firstOffset(section)
				p.baseline = &first
			}
			last := section.NextOffset - 1
			if p.received == nil || last > *p.received {
				p.received = &last
			}
		}
	}
}

func (r *PartitionReader) readGapLocked(p *partition) bool {
	missing := p.gapEnd - *p.gapFrom
	count := uint32(min(uint64(r.config.options.Count), missing))
	if p.aloneUntil != nil && *p.gapFrom < *p.aloneUntil {
		count = 1
	}
	strategy := iggcon.OffsetPollingStrategy(*p.gapFrom).WithContext(p.context)
	return r.readLocked([]iggcon.FetchMessagesTarget{{PartitionId: p.id, Strategy: strategy}}, count, false, false, true)
}

// acceptGapLocked takes a read of the gap. start is the offset of its first
// message, or the next offset of an empty read. The history no longer holds
// the part of the gap below start, as after retention.
func (r *PartitionReader) acceptGapLocked(p *partition, read *iggcon.PolledMessage, start uint64) {
	if start > *p.gapFrom {
		r.loseGapLocked(p, min(start, p.gapEnd))
	}
	heldFirst := p.gapEnd
	var missing []iggcon.IggyMessage
	for _, message := range read.Messages {
		if message.Header.Offset < heldFirst {
			missing = append(missing, message)
		}
	}
	if len(missing) > 0 {
		batch := *read
		batch.Messages = missing
		batch.MessageCount = uint32(len(missing))
		r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, batch: &batch})
		next := missing[len(missing)-1].Header.Offset + 1
		p.gapFrom = &next
	}
	if *p.gapFrom >= heldFirst {
		if len(p.held.Messages) > 0 {
			r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, batch: p.held})
		}
		p.gapFrom = nil
		p.held = nil
	}
}

func (r *PartitionReader) refuseSectionLocked(p *partition, err error, now time.Time) {
	p.backlogged = false
	r.handleRefusalLocked(p, err, now)
}

// giveUpGapLocked reports what a lost read stored as lost, since another member
// owns the partition now, and queues the batch read after it, as an empty read
// that ends the gap does.
func (r *PartitionReader) giveUpGapLocked(p *partition) {
	r.acceptGapLocked(p, &iggcon.PolledMessage{}, p.gapEnd)
}

// loseGapLocked reports the gap below until as lost and moves its start there,
// so the report comes once and before the messages after it.
func (r *PartitionReader) loseGapLocked(p *partition, until uint64) {
	gone := OffsetRange{First: *p.gapFrom, Last: until - 1}
	lost := &ReadError{PartitionID: &p.id, Lost: &gone, Err: fmt.Errorf("%w: offsets %d to %d were stored by a read whose reply was lost",
		ierror.FetchProgressUncertain{}, gone.First, gone.Last)}
	r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, err: lost})
	p.gapFrom = &until
}

func (r *PartitionReader) refuseLocked(generations map[uint32]uint64, err error) {
	now := time.Now()
	if errors.Is(err, context.Canceled) && r.run.Err() != nil {
		return
	}
	// A seek or a new assignment during the read started a newer generation,
	// which an error of the read says nothing about. So a read without a
	// current partition reports nothing either.
	refused := make([]*partition, 0, len(generations))
	for id, generation := range generations {
		if p := r.partitions[id]; p != nil && p.generation == generation {
			refused = append(refused, p)
		}
	}
	if len(refused) == 0 {
		return
	}
	if errors.Is(err, ierror.FetchProgressUncertain{}) {
		// The next read of the server starts after what the lost read stored, and the gap check reads it again.
		for _, p := range refused {
			p.lostRead = true
			p.retryAt = now.Add(r.config.retryInterval)
		}
		return
	}
	var coded interface{ Code() ierror.Code }
	if errors.As(err, &coded) {
		for _, p := range refused {
			r.handleRefusalLocked(p, err, now)
		}
		return
	}
	for _, p := range refused {
		p.retryAt = now.Add(r.config.retryInterval)
	}
	r.config.logger.Warn("A read of the reader failed, the reader retries it", "error", err)
	if len(r.queue) == 0 || r.queue[len(r.queue)-1].partition != wholeReader {
		r.queue = append(r.queue, entry{partition: wholeReader, err: &ReadError{Err: err}})
	}
}

func (r *PartitionReader) handleRefusalLocked(p *partition, err error, now time.Time) {
	switch {
	case errors.Is(err, ierror.ErrNotPrimary):
		r.regroup = true
		p.misrouted = true
		return
	case errors.Is(err, ierror.ErrConsumerGroupPartitionNotOwned):
		// A new session and a move of the partition away and back look the
		// same here. Only a leave that a refresh shows drops what the close
		// needs, so p keeps it and its position, at the cost of duplicates
		// after a move.
		p.loseOwner()
		r.refreshAt = now
		p.lastingRefusals++
		p.retryAt = now.Add(backoff(r.config.retryInterval, p.lastingRefusals))
		if p.lastingRefusals >= lastingRefusalsBeforeStop {
			p.lastingRefusals = 0
			r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, err: &ReadError{PartitionID: &p.id, Err: err}})
		}
		return
	case transient(err):
		p.retryAt = now.Add(r.config.retryInterval)
		return
	case errors.Is(err, ierror.ErrHistoryUnavailable) && !p.fromRead:
		r.config.logger.Warn("A partition has a new history, the reader reads it again", "partition", p.id)
		r.regroup = true
		p.retryAt = now.Add(r.config.retryInterval)
		return
	}
	p.lastingRefusals++
	if errors.Is(err, ierror.ErrHistoryUnavailable) || errors.Is(err, ierror.ErrInvalidSizeBytes) ||
		p.lastingRefusals >= lastingRefusalsBeforeStop {
		p.stopped = &ReadError{PartitionID: &p.id, Err: err}
		r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, err: p.stopped})
		return
	}
	p.retryAt = now.Add(backoff(r.config.retryInterval, p.lastingRefusals))
	r.config.logger.Warn("A partition refused a read, the reader retries it", "partition", p.id, "error", err)
}

// backoff is the wait after the refusals-th lasting refusal of a partition in
// a row. It doubles with each refusal, up to 2^maxBackoffShift times interval.
func backoff(interval time.Duration, refusals int) time.Duration {
	return interval << min(refusals-1, maxBackoffShift)
}

// moveLocked starts a new generation of p at the start of a seek, without the
// queued batches of p.
func (r *PartitionReader) moveLocked(p *partition, start iggcon.PollingStrategy) {
	r.lastGen++
	r.seekGenerations[p.id] = r.lastGen
	r.dropQueuedLocked(p.id)
	*p = partition{id: p.id, generation: r.lastGen, position: start, fromRead: start.Context != nil, context: p.context,
		seenOwner: p.seenOwner, lostOwner: p.lostOwner, misrouted: p.misrouted}
	if !r.onRead || start.Kind != iggcon.POLLING_OFFSET {
		return
	}
	if start.Context != nil {
		p.takeContext(*start.Context)
	}
	if start.Value > 0 {
		// The seek stored the offset before start, and the server answers a Next read after it.
		first := start.Value
		p.baseline = &first
		p.position = iggcon.NextPollingStrategy()
	}
}

// moveBefore moves the stored offset of a partition before first, or deletes
// it when first is 0, but only if the stored offset is at or past first. The
// server refuses a store past the end of the partition and the delete of a
// missing offset, and first can lie past the end when no read stored there.
// retry bounds the read of a write refused as not the owner, see writePosition.
func (r *PartitionReader) moveBefore(ctx, retry context.Context, p *partition, first iggcon.ConsumerPosition) error {
	stored, err := r.client.GetConsumerOffset(ctx, r.consumer, r.stream, r.topic, &first.PartitionId)
	if err != nil || stored == nil || stored.StoredOffset < first.Offset {
		return err
	}
	if first.Offset == 0 {
		return r.writePosition(ctx, retry, r.client.DeleteConsumerPosition, p, first)
	}
	before := first
	before.Offset--
	return r.writePosition(ctx, retry, r.client.StoreConsumerPosition, p, before)
}

// writePosition sends write, a store or delete of the offset of p. A refusal
// as not the owner is no leave either. The session can have changed after the
// write took its context, or a rebalance by another member gave p to the
// member at an op that no reply of the client named. So the reader takes the
// owner of p as lost, as after such a refusal of a read, and reads p for the
// owner that the server installed. If that read serves the incarnation of the
// write, or the write names none, the write goes once more under the context
// of that read, whose owner belongs to the history it names. Without p, as for
// a partition that left, or after a leave meanwhile, the refusal stands. retry
// is the context of the caller of the write, or r.run for a write in the
// background, whose caller is the reader. Its end ends that read, also after
// its reply, and the refusal stands. So Close ends the read of a write in the
// background.
func (r *PartitionReader) writePosition(ctx, retry context.Context,
	write func(context.Context, iggcon.Consumer, iggcon.Identifier, iggcon.Identifier, iggcon.ConsumerPosition) error,
	p *partition, position iggcon.ConsumerPosition,
) error {
	err := write(ctx, r.consumer, r.stream, r.topic, position)
	if p == nil || !errors.Is(err, ierror.ErrConsumerGroupPartitionNotOwned) {
		return err
	}
	r.mu.Lock()
	// A seek under Manual or Auto rewrites *p under the lock.
	id := p.id
	stayed := r.partitions[id] == p
	if stayed {
		p.loseOwner()
		r.refreshAt = time.Now()
	}
	r.mu.Unlock()
	if !stayed {
		return err
	}
	r.signal()
	installed, served := r.installedContext(retry, id)
	if !served || retry.Err() != nil || position.Context.Incarnation != 0 && installed.Incarnation != position.Context.Incarnation {
		return err
	}
	position.Context = installed
	return write(ctx, r.consumer, r.stream, r.topic, position)
}

// installedContext reads a partition once without a store and with owner 0,
// which the server serves for the owner that it installed for the member, and
// returns the context of the read. After a transient refusal it reads again
// after the retry interval, until ctx ends. The server refuses a read of one
// partition over its poll quota as a whole request, so a transient error of
// the request counts as one of the partition. It reports false for any other
// failure.
func (r *PartitionReader) installedContext(ctx context.Context, partitionID uint32) (iggcon.PartitionContext, bool) {
	options := r.fetchOptions(1)
	// Only the context of the reply counts, so the read waits for no message.
	options.MaxWait = 0
	target := iggcon.FetchMessagesTarget{PartitionId: partitionID, Strategy: iggcon.NextPollingStrategy()}
	for {
		probed, refusal, err := r.probe(ctx, target, options)
		if err == nil && probed != nil {
			return probed.Context, true
		}
		if err == nil {
			err = refusal
		}
		if !transient(err) {
			return iggcon.PartitionContext{}, false
		}
		select {
		case <-ctx.Done():
			return iggcon.PartitionContext{}, false
		case <-time.After(r.config.retryInterval):
		}
	}
}

// transient reports whether err refuses a read only for now. A primary that
// moved refuses with NotPrimary, and the client looks the route up again for
// the next read.
func transient(err error) bool {
	return errors.Is(err, ierror.ErrTransientNotAccepted) || errors.Is(err, ierror.ErrTransientNotCommitted) ||
		errors.Is(err, ierror.ErrDeferredPollCapacityExceeded) || errors.Is(err, ierror.ErrNotPrimary)
}

// firstQueuedLocked returns the first offset of the queued batches of the
// current generation of p. A batch of an earlier assignment names an owner
// that the server replaced, and another member can have moved the offset
// since, so it decides nothing.
func (r *PartitionReader) firstQueuedLocked(p *partition) (uint64, bool) {
	for _, queued := range r.queue {
		if queued.partition == int64(p.id) && queued.generation == p.generation && queued.batch != nil && len(queued.batch.Messages) > 0 {
			return queued.batch.Messages[0].Header.Offset, true
		}
	}
	return 0, false
}

func (r *PartitionReader) dropQueuedLocked(partitionID uint32) {
	kept := r.queue[:0]
	for _, queued := range r.queue {
		if queued.partition != int64(partitionID) {
			kept = append(kept, queued)
		}
	}
	r.queue = kept
}

// currentLocked reports whether a queued entry still counts. A seek or a new
// assignment of its partition drops it. A partition that left the assignment
// keeps the reads that the server stored under OnRead.
func (r *PartitionReader) currentLocked(queued entry) bool {
	if queued.partition == wholeReader {
		return true
	}
	return r.keepsLocked(uint32(queued.partition), queued.generation)
}

// keeps reports whether a batch that the generation of its partition read still counts.
func (r *PartitionReader) keeps(partitionID uint32, generation uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.keepsLocked(partitionID, generation)
}

func (r *PartitionReader) keepsLocked(partitionID uint32, generation uint64) bool {
	p := r.partitions[partitionID]
	return (p != nil && p.generation == generation) ||
		(r.onRead && generation >= r.seekGenerations[partitionID])
}

func (r *PartitionReader) deliveredLocked(p *partition, batch *iggcon.PolledMessage) {
	last, _ := batch.Checkpoint()
	if p.firstDelivered == nil {
		first := batch.Messages[0].Header.Offset
		p.firstDelivered = &first
	}
	p.delivered = &last
	if r.auto == nil {
		return
	}
	switch r.auto.After {
	case iggcon.CommitAfterDelivery:
		r.countLocked(p, last, uint64(len(batch.Messages)), true)
	case iggcon.CommitAfterProcessing:
		r.uncounted = &uncounted{partition: p.id, generation: p.generation, checkpoint: last, messages: uint64(len(batch.Messages))}
	}
}

// deliverMessage records one message that an IggyConsumer handed over from a
// batch that generation read. As in next, a batch of an earlier assignment is
// no close point.
func (r *PartitionReader) deliverMessage(checkpoint iggcon.Checkpoint, generation uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p := r.partitions[checkpoint.PartitionID()]; p != nil && p.generation == generation {
		if p.firstDelivered == nil {
			offset := checkpoint.Offset()
			p.firstDelivered = &offset
		}
		p.delivered = &checkpoint
	}
}

// countDelivered counts messages up to a delivered checkpoint, for an IggyConsumer.
func (r *PartitionReader) countDelivered(checkpoint iggcon.Checkpoint, messages uint64, endsBatch bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p := r.partitions[checkpoint.PartitionID()]; p != nil && p.deliveredHere(checkpoint) {
		r.countLocked(p, checkpoint, messages, endsBatch)
	}
}

func (r *PartitionReader) countUncountedLocked() {
	if r.uncounted == nil {
		return
	}
	batch := r.uncounted
	r.uncounted = nil
	if p := r.partitions[batch.partition]; p != nil && p.generation == batch.generation {
		r.countLocked(p, batch.checkpoint, batch.messages, true)
	}
}

func (r *PartitionReader) countLocked(p *partition, checkpoint iggcon.Checkpoint, messages uint64, endsBatch bool) {
	if r.auto == nil {
		return
	}
	if p.counted == nil || checkpoint.Offset() > p.counted.Offset() {
		p.counted = &checkpoint
	}
	p.sinceStore += messages
	due := false
	switch r.auto.Every.Kind {
	case iggcon.CommitEveryMessage:
		due = true
	case iggcon.CommitEveryMessages:
		due = p.sinceStore >= uint64(r.auto.Every.Messages)
	case iggcon.CommitEveryBatch:
		due = endsBatch
	}
	if due {
		select {
		case r.storeDue <- struct{}{}:
		default:
		}
	}
}

// flushInBackground starts a flush and returns a channel that closes when it
// ends.
func (r *PartitionReader) flushInBackground() <-chan struct{} {
	flushed := make(chan struct{})
	r.inFlight.Add(1)
	go func() {
		defer r.inFlight.Done()
		defer close(flushed)
		// The reader is the caller of these stores, so Close ends the read of a
		// store refused as not the owner, but no store itself.
		if err := r.flush(context.WithoutCancel(r.run), r.run); err != nil {
			r.config.logger.Error("Failed to store the counted offsets of a reader", "error", err)
		}
	}()
	return flushed
}

// flush stores the retired positions, then each counted position ahead of its
// last store, one at a time, and returns the first failure. retry bounds the
// read of a store refused as not the owner, see writePosition. The loop and
// Close never run two flushes at once, so an older position never overwrites
// a newer one.
func (r *PartitionReader) flush(ctx, retry context.Context) error {
	type pendingStore struct {
		p          *partition
		checkpoint iggcon.Checkpoint
		position   iggcon.ConsumerPosition
	}
	r.mu.Lock()
	stores := make([]pendingStore, 0, len(r.retiring)+len(r.partitions))
	for _, checkpoint := range r.retiring {
		stores = append(stores, pendingStore{nil, checkpoint, checkpointPosition(checkpoint)})
	}
	r.retiring = nil
	for _, p := range r.partitions {
		if p.counted != nil && (p.stored == nil || p.counted.Offset() > p.stored.Offset()) {
			position := checkpointPosition(*p.counted)
			position.Context = r.writeContext(p, position.Context)
			stores = append(stores, pendingStore{p, *p.counted, position})
		}
	}
	r.mu.Unlock()
	var failure error
	for _, pending := range stores {
		checkpoint := pending.checkpoint
		if err := r.writePosition(ctx, retry, r.client.StoreConsumerPosition, pending.p, pending.position); err != nil {
			// The next store of the partition, also of a later reader on the same client, can then go out.
			r.abandonUncertain(err)
			if failure == nil {
				failure = err
			}
			continue
		}
		r.mu.Lock()
		if p := r.partitions[checkpoint.PartitionID()]; p != nil && p.deliveredHere(checkpoint) {
			stored := checkpoint
			p.stored = &stored
			p.sinceStore = 0
		}
		r.mu.Unlock()
	}
	return failure
}

// rewind moves the offset of each partition with undelivered messages back to
// its last delivered message. A partition that delivered none moves before its
// first undelivered message. retry bounds the read of a write refused as not
// the owner, see writePosition.
func (r *PartitionReader) rewind(ctx, retry context.Context) error {
	type pendingWrite struct {
		p        *partition
		position iggcon.ConsumerPosition
	}
	r.mu.Lock()
	var positions, moves []pendingWrite
	for _, p := range r.partitions {
		current := r.writeContext(p, p.context)
		first, queued := r.firstQueuedLocked(p)
		undelivered := queued || p.lostRead || (p.readStarted && p.delivered == nil) ||
			(p.received != nil && (p.delivered == nil || *p.received > p.delivered.Offset()))
		started := p.baseline != nil && p.readStarted
		if started {
			// The first committing read of the generation started at the baseline.
			first = *p.baseline
		}
		known := queued || p.received != nil || p.probed
		switch {
		case !undelivered:
		case p.delivered != nil:
			delivered := checkpointPosition(*p.delivered)
			delivered.Context = r.writeContext(p, delivered.Context)
			positions = append(positions, pendingWrite{p, delivered})
		case (started || queued) && known && first > 0:
			positions = append(positions, pendingWrite{p, iggcon.ConsumerPosition{PartitionId: p.id, Offset: first - 1, Context: current}})
		case p.restore != nil && *p.restore > 0:
			// A seek write with an unknown outcome can have moved the stored
			// offset back or deleted it, so a lookup now cannot tell what reads
			// stored.
			positions = append(positions, pendingWrite{p, iggcon.ConsumerPosition{PartitionId: p.id, Offset: *p.restore - 1, Context: current}})
		case p.restore != nil:
			// No offset was stored before that write, so the offset that it or a
			// read stored goes, and a restart applies the start again.
			moves = append(moves, pendingWrite{p, iggcon.ConsumerPosition{PartitionId: p.id, Context: current}})
		case started || queued:
			// No offset comes before 0, and a named offset without a known batch
			// can lie past the end. So the offset moves only over what a read
			// stored.
			moves = append(moves, pendingWrite{p, iggcon.ConsumerPosition{PartitionId: p.id, Offset: first, Context: current}})
		default:
			r.config.logger.Warn("Cannot move the offset of a partition back, no message of it was delivered", "partition", p.id)
		}
	}
	r.queue = nil
	r.mu.Unlock()
	var failure error
	fail := func(err error) {
		// A later reader on the same client can then write the offset of the partition.
		r.abandonUncertain(err)
		if failure == nil {
			failure = err
		}
	}
	for _, pending := range moves {
		fail(r.moveBefore(ctx, retry, pending.p, pending.position))
	}
	for _, pending := range positions {
		fail(r.writePosition(ctx, retry, r.client.StoreConsumerPosition, pending.p, pending.position))
	}
	return failure
}

func (r *PartitionReader) regroupPartitions(ids []uint32) {
	defer r.inFlight.Done()
	groups, err := r.transport.ReadinessGroups(r.run, r.stream, r.topic, ids)
	r.mu.Lock()
	r.grouping = false
	if err != nil {
		// The partitions read on with the last good groups.
		r.regroup = true
		r.regroupAt = time.Now().Add(r.config.retryInterval)
		r.config.logger.Warn("Failed to group the partitions of a reader, it tries again", "error", err)
	} else {
		r.groups = groups
	}
	for _, p := range r.partitions {
		p.misrouted = false
	}
	r.mu.Unlock()
	r.signal()
}

func (r *PartitionReader) refreshAssignment() {
	defer r.inFlight.Done()
	rejoined := false
	var assignment *iggcon.ConsumerGroupAssignment
	err := r.resolveJoin()
	if err == nil {
		assignment, err = r.client.SyncConsumerGroup(r.run, r.stream, r.topic, r.consumer.Id)
	}
	lost := err != nil && errors.Is(err, ierror.ErrConsumerGroupMemberNotFound)
	if lost {
		err = r.client.JoinConsumerGroup(r.run, r.stream, r.topic, r.consumer.Id)
		var unknown ierror.OperationUncertain
		switch {
		case err == nil:
			rejoined = true
			assignment, err = r.client.SyncConsumerGroup(r.run, r.stream, r.topic, r.consumer.Id)
		case errors.As(err, &unknown):
			r.uncertainJoin = &unknown.OperationID
		}
	}
	r.mu.Lock()
	r.refreshing = false
	if lost {
		r.loseOwnersLocked()
	}
	r.joined = r.joined || rejoined
	pause := r.config.retryInterval
	if err != nil {
		r.config.logger.Warn("Failed to read the group assignment of a reader, it tries again", "error", err)
	} else if assignment != nil {
		pause = r.nextRefreshPauseLocked(r.assignLocked(assignment.Partitions))
	}
	r.refreshAt = time.Now().Add(pause)
	r.broadcastLocked()
	r.mu.Unlock()
	r.signal()
}

// loseOwnersLocked follows a lost membership, as after a lost session.
func (r *PartitionReader) loseOwnersLocked() {
	// The owner of a counted position of a partition that left is gone too.
	r.retiring = nil
	for _, p := range r.partitions {
		p.loseOwner()
	}
}

// loseOwner follows a lost owner of p. The server installs the owner of a
// partition that the member gets again under a higher generation, so the
// reads and writes of p name owner generation 0 until a read names the new
// owner.
func (p *partition) loseOwner() {
	p.lostOwner = max(p.lostOwner, p.seenOwner)
	if fence := p.position.Context; fence != nil {
		p.lostOwner = max(p.lostOwner, fence.OwnerGeneration)
		unowned := *fence
		unowned.OwnerGeneration = 0
		p.position = p.position.WithContext(unowned)
	}
	p.takeContext(p.context)
}

// resolveJoin resolves a rejoin with an unknown outcome before the next sync,
// because a sync that finds the member does not join again. The sync after it
// tells whether the member is in the group.
func (r *PartitionReader) resolveJoin() error {
	if r.uncertainJoin == nil {
		return nil
	}
	err := r.client.ResolveOperation(r.run, *r.uncertainJoin)
	if errors.Is(err, ierror.OperationUncertain{}) {
		return err
	}
	r.uncertainJoin = nil
	if errors.Is(err, ierror.ErrOperationNotRetained) {
		return nil
	}
	return err
}

// nextRefreshPauseLocked returns the pause after a sync that read the
// assignment. A join hands the partitions of the member over one at a time, so
// after a join or a change the member syncs again soon. Each sync without a
// change doubles the pause, up to the retry interval.
func (r *PartitionReader) nextRefreshPauseLocked(changed bool) time.Duration {
	switch {
	case changed || r.joined:
		r.refreshPause = min(assignmentSettlePause, r.config.retryInterval)
	// Doubling only below half of the interval cannot overflow.
	case r.refreshPause < r.config.retryInterval/2:
		r.refreshPause *= 2
	default:
		r.refreshPause = r.config.retryInterval
	}
	r.joined = false
	return r.refreshPause
}

// assignLocked follows an assignment and reports whether it changed the partitions of the reader.
func (r *PartitionReader) assignLocked(assigned []uint32) bool {
	next := make(map[uint32]bool, len(assigned))
	for _, id := range assigned {
		next[id] = true
	}
	changed := false
	for id, p := range r.partitions {
		if next[id] {
			continue
		}
		delete(r.partitions, id)
		if !r.onRead {
			r.dropQueuedLocked(id)
		} else if p.gapFrom != nil {
			r.giveUpGapLocked(p)
		}
		if p.counted != nil && (p.stored == nil || p.counted.Offset() > p.stored.Offset()) && !p.ownerLost(p.counted.Context()) {
			// The partition goes to another member, so its counted position is stored first.
			r.retiring = append(r.retiring, *p.counted)
		}
		changed = true
	}
	for _, id := range assigned {
		if _, ok := r.partitions[id]; !ok {
			// Another member can have moved the offset since the partition left,
			// so a counted position of an earlier assignment is never stored.
			r.retiring = slices.DeleteFunc(r.retiring, func(checkpoint iggcon.Checkpoint) bool { return checkpoint.PartitionID() == id })
			r.addPartition(id)
			changed = true
		}
	}
	if changed {
		r.regroup = true
		if len(r.retiring) > 0 {
			select {
			case r.storeDue <- struct{}{}:
			default:
			}
		}
	}
	return changed
}

// abandonLost abandons a committing read whose reply was lost. The client
// blocks the offset writes of its partitions until then, also after a seek or
// a new assignment. The read ends only after the abandon, so a seek that waits
// for it can store, and so can the rewind of Close.
func (r *PartitionReader) abandonLost(sections []iggcon.FetchMessagesSection, err error) {
	var unknown ierror.FetchProgressUncertain
	if errors.As(err, &unknown) {
		r.abandon(unknown.OperationID)
		return
	}
	for _, section := range sections {
		// The lost sections of a fetch share its one operation.
		if errors.As(section.Err, &unknown) {
			r.abandon(unknown.OperationID)
			return
		}
	}
}

// abandonUncertain abandons an offset write whose outcome is unknown, which
// blocks the offset writes of its partition on the client until then, and
// reports whether err was such a write.
func (r *PartitionReader) abandonUncertain(err error) bool {
	var unknown ierror.OperationUncertain
	if !errors.As(err, &unknown) {
		return false
	}
	r.abandon(unknown.OperationID)
	return true
}

func (r *PartitionReader) abandon(operationID uint64) {
	if err := r.client.AbandonOperation(context.WithoutCancel(r.run), operationID); err != nil {
		r.config.logger.Warn("Failed to abandon an operation with an unknown outcome", "operation", operationID, "error", err)
	}
}

// fetchable reports whether a fetch can read from position, since the server
// reads a fetch only from an offset or Next.
func fetchable(position iggcon.PollingStrategy) bool {
	return position.Kind == iggcon.POLLING_OFFSET || position.Kind == iggcon.POLLING_NEXT
}

// dealTargets deals targets round robin into count reads.
func dealTargets(targets []iggcon.FetchMessagesTarget, count int) [][]iggcon.FetchMessagesTarget {
	reads := make([][]iggcon.FetchMessagesTarget, count)
	for index, target := range targets {
		reads[index%count] = append(reads[index%count], target)
	}
	return reads
}

// sameIncarnationAndOwner reports whether offsets of two contexts compare. The
// metadata op is left out, because the server raises it per read.
func sameIncarnationAndOwner(left, right iggcon.PartitionContext) bool {
	return left.Incarnation == right.Incarnation && left.OwnerGeneration == right.OwnerGeneration
}

func checkpointPosition(checkpoint iggcon.Checkpoint) iggcon.ConsumerPosition {
	return iggcon.ConsumerPosition{PartitionId: checkpoint.PartitionID(), Offset: checkpoint.Offset(), Context: checkpoint.Context()}
}
