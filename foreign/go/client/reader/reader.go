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
// groups them by primary and reads each group with one request, and hands
// over one message at a time with an IggyConsumer.
package reader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	// DefaultPrefetchMessages bounds the messages of the reads in flight and the queued batches.
	DefaultPrefetchMessages = 16_000
	// DefaultRetryInterval is the pause before a refused or failed read is sent again.
	DefaultRetryInterval      = time.Second
	lastingRefusalsBeforeStop = 6
	maxBackoffShift           = 5
	wholeReader               = -1
)

// ReadError is an error of a read. The reader stays usable after it and retries by itself.
type ReadError struct {
	// PartitionID is the partition of the error, or nil for an error of the whole reader.
	PartitionID *uint32
	Err         error
	// Skip is, for a section that does not decode, the continuation after it.
	// The partition stops until a Seek, to Skip to step over the section.
	Skip *iggcon.PollingStrategy
}

func (e *ReadError) Error() string {
	if e.PartitionID != nil {
		return fmt.Sprintf("partition %d: %v", *e.PartitionID, e.Err)
	}
	return e.Err.Error()
}

func (e *ReadError) Unwrap() error { return e.Err }

// transport is what the reader needs of the client: a read of the partitions
// of one primary in one request, and the grouping of partitions by primary.
type transport interface {
	FetchMessages(ctx context.Context, streamId, topicId iggcon.Identifier, consumer iggcon.Consumer,
		targets []iggcon.FetchMessagesTarget, count uint32, autoCommit bool, options iggcon.DeferredPollOptions,
	) ([]iggcon.FetchMessagesSection, error)
	ReadinessGroups(ctx context.Context, streamId, topicId iggcon.Identifier, partitions []uint32) ([][]uint32, error)
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
func WithStart(start iggcon.PollingStrategy) Option {
	return func(c *config) { c.start = start }
}

// WithPollOptions sets the options of each read and the commit policy,
// WaitPollOptions(DefaultMaxWait) by default, which stores nothing.
func WithPollOptions(options iggcon.PollOptions) Option {
	return func(c *config) { c.options = options }
}

// WithPrefetchMessages bounds the messages of the reads in flight and the
// queued batches, the larger of DefaultPrefetchMessages and the count by default.
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
	retryAt  time.Time
	// servedAt is the read turn that last returned messages of the partition.
	servedAt        uint64
	lastingRefusals int
	stopped         *ReadError
	context         iggcon.PartitionContext
	// received is the last offset that a read of this generation returned.
	received       *uint64
	baseline       *uint64
	readStarted    bool
	firstDelivered *uint64
	delivered      *iggcon.Checkpoint
	committed      *uint64
	// gapFrom and held: under OnRead, the start of the messages that a lost
	// read stored, and the batch that was read after them.
	gapFrom     *uint64
	gapEnd      uint64
	held        *iggcon.PolledMessage
	pendingSeek *iggcon.PollingStrategy
	counted     *iggcon.Checkpoint
	stored      *iggcon.Checkpoint
	sinceStore  uint64
}

func (p *partition) deliveredHere(checkpoint iggcon.Checkpoint) bool {
	return p.delivered != nil && p.firstDelivered != nil && checkpoint.Context() == p.delivered.Context() &&
		checkpoint.Offset() >= *p.firstDelivered && checkpoint.Offset() <= p.delivered.Offset()
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
// undelivered batches back to its last delivered message. With CommitAuto the
// reader counts each batch as the policy says, or each Mark, and stores the
// latest counted position at its interval or event, before a rebalance takes a
// partition away, and in Close.
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

	storeMu  sync.Mutex
	storeDue chan struct{}
	wake     chan struct{}
	run      context.Context
	stop     context.CancelFunc
	inFlight sync.WaitGroup
	loopDone chan struct{}
}

// NewPartitionReader starts a reader. A consumer group reader joins its group first.
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
	if cfg.start.Kind == iggcon.POLLING_FIRST {
		cfg.start = iggcon.OffsetPollingStrategy(0)
	}
	prefetch := uint64(cfg.prefetchMessages)
	if prefetch == 0 {
		prefetch = max(DefaultPrefetchMessages, uint64(cfg.options.Count))
	}
	run, stop := context.WithCancel(context.WithoutCancel(ctx))
	r := &PartitionReader{
		client: client, transport: fetcher, stream: streamId, topic: topicId, consumer: consumer, config: cfg,
		slots: int(max(1, prefetch/uint64(cfg.options.Count))), group: group,
		onRead:     cfg.options.Commit.Kind == iggcon.CommitOnRead,
		partitions: make(map[uint32]*partition), changed: make(chan struct{}), regroup: true,
		storeDue: make(chan struct{}, 1), wake: make(chan struct{}, 1), run: run, stop: stop,
		loopDone: make(chan struct{}),
	}
	if cfg.options.Commit.Kind == iggcon.CommitAuto {
		auto := cfg.options.Commit.Auto
		r.auto = &auto
	}
	if group {
		if err := client.JoinConsumerGroup(ctx, streamId, topicId, consumer.Id); err != nil {
			stop()
			return nil, err
		}
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
	return r.next(ctx, true)
}

func (r *PartitionReader) next(ctx context.Context, delivers bool) (*iggcon.PolledMessage, error) {
	r.mu.Lock()
	if delivers {
		r.countUncountedLocked()
	}
	for {
		if r.closed {
			r.mu.Unlock()
			return nil, ierror.ErrClientShutdown
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
				return nil, head.err
			}
			if p := r.partitions[uint32(head.partition)]; delivers && p != nil {
				r.deliveredLocked(p, head.batch)
			}
			r.mu.Unlock()
			r.signal()
			return head.batch, nil
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
			return nil, ctx.Err()
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
// CommitOnRead and for a partition that the reader does not read.
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
	offset := checkpoint.Offset()
	p.committed = &offset
	r.mu.Unlock()
	return r.client.StoreConsumerPosition(ctx, r.consumer, r.stream, r.topic, checkpointPosition(checkpoint))
}

// Seek sets the next read position of a partition and starts a new generation
// of it. It drops the queued batches of the partition and resumes a stopped
// one. Under CommitOnRead, a seek to an offset stores the offset before it
// once the read in flight ended, so the next read of the server starts there.
func (r *PartitionReader) Seek(ctx context.Context, partitionID uint32, start iggcon.PollingStrategy) error {
	if err := start.RequirePartition(partitionID); err != nil {
		return err
	}
	if start.Kind == iggcon.POLLING_FIRST {
		start = iggcon.OffsetPollingStrategy(0)
	}
	r.mu.Lock()
	p := r.partitions[partitionID]
	if p == nil || r.closed {
		r.mu.Unlock()
		return ierror.ErrInvalidCommand
	}
	r.lastGen++
	if r.seekGenerations == nil {
		r.seekGenerations = make(map[uint32]uint64)
	}
	r.seekGenerations[partitionID] = r.lastGen
	r.dropQueuedLocked(partitionID)
	if r.onRead && p.inFlight {
		p.pendingSeek = &start
		r.mu.Unlock()
		return nil
	}
	store := r.applySeekLocked(p, start)
	r.mu.Unlock()
	r.signal()
	if store != nil {
		return store(ctx)
	}
	return nil
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

// Close stops the reader and waits for its reads, which it lets end. With
// CommitAuto it counts the last batch under CommitAfterProcessing and stores
// the latest counted position of each partition. With CommitOnRead it moves
// the offset of each partition with undelivered batches back to its last
// delivered message. A group reader then leaves its group. It returns the
// first failed store or leave.
func (r *PartitionReader) Close(ctx context.Context) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.countUncountedLocked()
	r.broadcastLocked()
	r.mu.Unlock()
	r.stop()
	<-r.loopDone
	r.inFlight.Wait()
	var failure error
	if r.onRead {
		failure = r.rewind(ctx)
	}
	if r.auto != nil {
		if err := r.flush(ctx); err != nil && failure == nil {
			failure = err
		}
	}
	if r.group {
		if err := r.client.LeaveConsumerGroup(ctx, r.stream, r.topic, r.consumer.Id); err != nil && failure == nil {
			failure = err
		}
	}
	return failure
}

func (r *PartitionReader) addPartition(id uint32) {
	r.lastGen++
	r.partitions[id] = &partition{id: id, generation: r.lastGen, position: r.config.start, fromRead: r.config.start.Context != nil}
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
	for {
		select {
		case <-r.run.Done():
			return
		case <-r.wake:
		case <-timer.C:
		case <-ticks:
			r.flushInBackground()
		case <-r.storeDue:
			r.flushInBackground()
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
	if !r.grouping && len(r.groups) > 0 && (!r.onRead || (r.waiting > 0 && len(r.queue) == 0)) {
		budget := r.slots - len(r.queue) - r.reads
		if r.onRead {
			budget = len(r.groups)
		}
		first := r.groupCursor
		for turn := 0; turn < len(r.groups) && budget > 0; turn++ {
			index := (first + turn) % len(r.groups)
			targets := r.readyTargetsLocked(r.groups[index], now)
			if len(targets) > 0 {
				r.readLocked(targets, r.config.options.Count, r.onRead)
				budget--
				r.groupCursor = index + 1
			}
		}
	}
	wait := r.config.retryInterval
	for _, p := range r.partitions {
		if !p.inFlight && p.stopped == nil && p.retryAt.After(now) {
			wait = min(wait, p.retryAt.Sub(now))
		}
	}
	return max(wait, time.Millisecond)
}

// readyTargetsLocked puts the partitions served last at the end, since a fetch
// that reaches its count serves its first targets.
func (r *PartitionReader) readyTargetsLocked(group []uint32, now time.Time) []iggcon.FetchMessagesTarget {
	var ready []*partition
	for _, id := range group {
		p := r.partitions[id]
		if p != nil && !p.inFlight && p.stopped == nil && p.gapFrom == nil && !p.retryAt.After(now) {
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

func (r *PartitionReader) readLocked(targets []iggcon.FetchMessagesTarget, count uint32, commit bool) {
	generations := make(map[uint32]uint64, len(targets))
	for _, target := range targets {
		p := r.partitions[target.PartitionId]
		p.inFlight = true
		generations[target.PartitionId] = p.generation
	}
	r.reads++
	r.inFlight.Add(1)
	options := r.config.options.Deferred()
	if count == 1 {
		options.MinCount = 1
	}
	// A fetch needs the reply margin also without a wait.
	options.RequestTimeout = max(options.RequestTimeout, options.MaxWait+iggcon.MinDeferredPollReplyMargin+time.Second)
	go func() {
		defer r.inFlight.Done()
		prepared := targets
		var err error
		if commit {
			prepared, err = r.prepareRead(targets, generations, options)
		}
		var sections []iggcon.FetchMessagesSection
		if err == nil && len(prepared) > 0 {
			sections, err = r.transport.FetchMessages(r.run, r.stream, r.topic, r.consumer, prepared, count, commit, options)
		}
		r.mu.Lock()
		r.reads--
		for id := range generations {
			if p := r.partitions[id]; p != nil {
				p.inFlight = false
			}
		}
		if !r.closed {
			if err == nil {
				r.acceptLocked(generations, commit, sections)
			} else {
				r.refuseLocked(generations, err)
			}
			r.applyPendingSeeksLocked(generations)
		}
		r.broadcastLocked()
		r.mu.Unlock()
		r.signal()
	}()
}

func (r *PartitionReader) prepareRead(targets []iggcon.FetchMessagesTarget, generations map[uint32]uint64, options iggcon.DeferredPollOptions) ([]iggcon.FetchMessagesTarget, error) {
	type baselineResult struct {
		target iggcon.FetchMessagesTarget
		batch  *iggcon.PolledMessage
		err    error
	}
	results := make(chan baselineResult, len(targets))
	probes := 0
	r.mu.Lock()
	for _, target := range targets {
		p := r.partitions[target.PartitionId]
		if p == nil || p.generation != generations[target.PartitionId] || p.received != nil || p.baseline != nil || target.Strategy.Kind != iggcon.POLLING_NEXT {
			continue
		}
		probes++
		go func() {
			probe := options
			probe.MinCount = 1
			sections, err := r.transport.FetchMessages(r.run, r.stream, r.topic, r.consumer, []iggcon.FetchMessagesTarget{target}, 1, false, probe)
			var polled *iggcon.PolledMessage
			for _, section := range sections {
				if section.PartitionId != target.PartitionId {
					continue
				}
				if section.Err != nil {
					err = section.Err
					break
				}
				polled, err = binaryserialization.DeserializeSectionMessages(section)
				break
			}
			results <- baselineResult{target: target, batch: polled, err: err}
		}()
	}
	r.mu.Unlock()
	var failure error
	for range probes {
		result := <-results
		r.mu.Lock()
		p := r.partitions[result.target.PartitionId]
		if result.err != nil && failure == nil {
			failure = result.err
		}
		if p != nil && p.generation == generations[result.target.PartitionId] && result.err == nil {
			if result.batch != nil && len(result.batch.Messages) > 0 {
				first := result.batch.Messages[0].Header.Offset
				p.baseline = &first
				p.context = result.batch.Context
				p.position = iggcon.NextPollingStrategy().WithContext(p.context)
			} else {
				p.retryAt = time.Now().Add(r.config.retryInterval)
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
		if p == nil || p.generation != generations[target.PartitionId] || p.pendingSeek != nil {
			continue
		}
		if target.Strategy.Kind == iggcon.POLLING_NEXT && p.received == nil && p.baseline == nil {
			continue
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
		if p == nil || p.generation != generations[section.PartitionId] || p.pendingSeek != nil {
			continue
		}
		var unknown ierror.FetchProgressUncertain
		if errors.As(section.Err, &unknown) {
			// The next read of the server shows what the lost read stored, and the gap check reads it again.
			r.abandon(unknown.OperationID)
			continue
		}
		if section.Err != nil {
			r.refuseSectionLocked(p, section.Err, now)
			continue
		}
		p.lastingRefusals = 0
		p.context = section.Context
		r.receiveLocked(p, section, commit, now)
	}
	for id := range generations {
		if p := r.partitions[id]; p != nil && !answered[id] && r.config.options.MaxWait == 0 {
			p.retryAt = now.Add(r.config.retryInterval)
		}
	}
}

func (r *PartitionReader) receiveLocked(p *partition, section iggcon.FetchMessagesSection, commit bool, now time.Time) {
	polled, err := binaryserialization.DeserializeSectionMessages(section)
	if err != nil {
		skip := iggcon.OffsetPollingStrategy(section.NextOffset).WithContext(section.Context)
		failure := &ReadError{PartitionID: &p.id, Err: err, Skip: &skip}
		p.stopped = failure
		r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, err: failure})
		return
	}
	expected := p.baseline
	if p.received != nil {
		next := *p.received + 1
		expected = &next
	}
	if len(polled.Messages) == 0 {
		if commit && p.gapFrom == nil && expected != nil && section.NextOffset > *expected {
			gap := *expected
			p.gapFrom = &gap
			p.gapEnd = section.NextOffset
			p.held = polled
			last := section.NextOffset - 1
			p.received = &last
			r.readGapLocked(p)
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
	case commit && p.gapFrom == nil && expected != nil && first > *expected:
		// A read whose reply was lost stored up to here, so the messages in between are read again first.
		gap := *expected
		p.gapFrom = &gap
		p.gapEnd = first
		p.held = polled
		p.received = &last
		r.readGapLocked(p)
	case p.gapFrom != nil && !commit:
		r.acceptGapLocked(p, polled)
	default:
		p.received = &last
		r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, batch: polled})
	}
	if r.onRead {
		p.position = iggcon.NextPollingStrategy()
	} else if continuation, ok := polled.Continuation(); ok {
		p.position = continuation
	}
	p.fromRead = !r.onRead
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

func (r *PartitionReader) readGapLocked(p *partition) {
	missing := p.gapEnd - *p.gapFrom
	count := uint32(min(uint64(r.config.options.Count), missing))
	strategy := iggcon.OffsetPollingStrategy(*p.gapFrom).WithContext(p.context)
	r.readLocked([]iggcon.FetchMessagesTarget{{PartitionId: p.id, Strategy: strategy}}, count, false)
}

func (r *PartitionReader) acceptGapLocked(p *partition, read *iggcon.PolledMessage) {
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
	if len(missing) == 0 || *p.gapFrom >= heldFirst {
		if len(p.held.Messages) > 0 {
			r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, batch: p.held})
		}
		p.gapFrom = nil
		p.held = nil
		return
	}
	r.readGapLocked(p)
}

func (r *PartitionReader) refuseSectionLocked(p *partition, err error, now time.Time) {
	if p.gapFrom != nil && errors.Is(err, ierror.ErrConsumerGroupPartitionNotOwned) {
		r.giveUpGapLocked(p)
	}
	r.handleRefusalLocked(p, err, now)
}

// giveUpGapLocked reports what a lost read stored as lost, since another member
// owns the partition now, and queues the batch read after it.
func (r *PartitionReader) giveUpGapLocked(p *partition) {
	lost := &ReadError{PartitionID: &p.id, Err: fmt.Errorf("%w: offsets %d to %d were stored by a read whose reply was lost",
		ierror.FetchProgressUncertain{}, *p.gapFrom, p.gapEnd-1)}
	r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, err: lost})
	if len(p.held.Messages) > 0 {
		r.queue = append(r.queue, entry{partition: int64(p.id), generation: p.generation, batch: p.held})
	}
	p.gapFrom = nil
	p.held = nil
}

func (r *PartitionReader) refuseLocked(generations map[uint32]uint64, err error) {
	now := time.Now()
	var unknown ierror.FetchProgressUncertain
	switch {
	case errors.Is(err, context.Canceled) && r.run.Err() != nil:
		return
	case errors.As(err, &unknown):
		// The next read of the server starts after what the lost read stored, and the gap check reads it again.
		r.abandon(unknown.OperationID)
		return
	case errors.Is(err, ierror.ErrNotPrimary):
		r.regroup = true
		return
	}
	var coded interface{ Code() ierror.Code }
	if errors.As(err, &coded) {
		for id := range generations {
			if p := r.partitions[id]; p != nil {
				r.handleRefusalLocked(p, err, now)
			}
		}
		return
	}
	for id := range generations {
		if p := r.partitions[id]; p != nil {
			p.retryAt = now.Add(r.config.retryInterval)
		}
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
		return
	case errors.Is(err, ierror.ErrConsumerGroupPartitionNotOwned):
		r.refreshAt = now
		p.retryAt = now.Add(r.config.retryInterval)
		return
	case errors.Is(err, ierror.ErrTransientNotAccepted), errors.Is(err, ierror.ErrTransientNotCommitted),
		errors.Is(err, ierror.ErrDeferredPollCapacityExceeded):
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
	shift := min(p.lastingRefusals-1, maxBackoffShift)
	p.retryAt = now.Add(r.config.retryInterval << shift)
	r.config.logger.Warn("A partition refused a read, the reader retries it", "partition", p.id, "error", err)
}

func (r *PartitionReader) applyPendingSeeksLocked(generations map[uint32]uint64) {
	for id := range generations {
		p := r.partitions[id]
		if p == nil || p.pendingSeek == nil {
			continue
		}
		start := *p.pendingSeek
		p.pendingSeek = nil
		r.dropQueuedLocked(id)
		if store := r.applySeekLocked(p, start); store != nil {
			r.inFlight.Add(1)
			go func() {
				defer r.inFlight.Done()
				if err := store(r.run); err != nil {
					r.config.logger.Error("Failed to store the offset before a seek", "partition", id, "error", err)
				}
			}()
		}
	}
}

// applySeekLocked starts a new generation of p at start, and returns the store
// that must go before its next read under OnRead.
func (r *PartitionReader) applySeekLocked(p *partition, start iggcon.PollingStrategy) func(context.Context) error {
	r.lastGen++
	*p = partition{id: p.id, generation: r.lastGen, position: start, fromRead: start.Context != nil, context: p.context}
	if !r.onRead || start.Kind != iggcon.POLLING_OFFSET || start.Value == 0 {
		return nil
	}
	// The server stores forward only on a read, so a store moves the offset that the next read starts after.
	before := start.Value - 1
	p.received = &before
	p.position = iggcon.NextPollingStrategy()
	history := p.context
	if start.Context != nil {
		history = *start.Context
	}
	id := p.id
	return func(ctx context.Context) error {
		return r.client.StoreConsumerPosition(ctx, r.consumer, r.stream, r.topic, iggcon.ConsumerPosition{
			PartitionId: id, Offset: before, Context: history})
	}
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

// deliverMessage records one message that an IggyConsumer handed over.
func (r *PartitionReader) deliverMessage(checkpoint iggcon.Checkpoint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p := r.partitions[checkpoint.PartitionID()]; p != nil {
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

func (r *PartitionReader) flushInBackground() {
	r.inFlight.Add(1)
	go func() {
		defer r.inFlight.Done()
		if err := r.flush(context.WithoutCancel(r.run)); err != nil {
			r.config.logger.Error("Failed to store the counted offsets of a reader", "error", err)
		}
	}()
}

// flush stores the retired positions, then each counted position ahead of its
// last store, one at a time, and returns the first failure.
func (r *PartitionReader) flush(ctx context.Context) error {
	r.storeMu.Lock()
	defer r.storeMu.Unlock()
	r.mu.Lock()
	stores := append([]iggcon.Checkpoint(nil), r.retiring...)
	r.retiring = nil
	for _, p := range r.partitions {
		if p.counted != nil && (p.stored == nil || p.counted.Offset() > p.stored.Offset()) {
			stores = append(stores, *p.counted)
		}
	}
	r.mu.Unlock()
	var failure error
	for _, checkpoint := range stores {
		if err := r.client.StoreConsumerPosition(ctx, r.consumer, r.stream, r.topic, checkpointPosition(checkpoint)); err != nil {
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
// its last delivered message.
func (r *PartitionReader) rewind(ctx context.Context) error {
	r.mu.Lock()
	firstQueued := make(map[uint32]uint64)
	for _, queued := range r.queue {
		if queued.batch != nil && len(queued.batch.Messages) > 0 {
			if _, ok := firstQueued[uint32(queued.partition)]; !ok {
				firstQueued[uint32(queued.partition)] = queued.batch.Messages[0].Header.Offset
			}
		}
	}
	r.queue = nil
	var positions []iggcon.ConsumerPosition
	var deletes []iggcon.ConsumerPosition
	for _, p := range r.partitions {
		first, queued := firstQueued[p.id]
		undelivered := queued || (p.readStarted && p.delivered == nil) || (p.received != nil && (p.delivered == nil || *p.received > p.delivered.Offset()))
		switch {
		case !undelivered:
		case p.delivered != nil:
			positions = append(positions, checkpointPosition(*p.delivered))
		case p.baseline != nil && p.readStarted:
			position := iggcon.ConsumerPosition{PartitionId: p.id, Context: p.context}
			if *p.baseline == 0 {
				deletes = append(deletes, position)
			} else {
				position.Offset = *p.baseline - 1
				positions = append(positions, position)
			}
		case r.config.start.Kind == iggcon.POLLING_NEXT && queued && first > 0:
			positions = append(positions, iggcon.ConsumerPosition{PartitionId: p.id, Offset: first - 1, Context: p.context})
		default:
			r.config.logger.Warn("Cannot move the offset of a partition back, no message of it was delivered", "partition", p.id)
		}
	}
	r.mu.Unlock()
	var failure error
	for _, position := range deletes {
		if err := r.client.DeleteConsumerPosition(ctx, r.consumer, r.stream, r.topic, position); err != nil && failure == nil {
			failure = err
		}
	}
	for _, position := range positions {
		if err := r.client.StoreConsumerPosition(ctx, r.consumer, r.stream, r.topic, position); err != nil && failure == nil {
			failure = err
		}
	}
	return failure
}

func (r *PartitionReader) regroupPartitions(ids []uint32) {
	defer r.inFlight.Done()
	groups, err := r.transport.ReadinessGroups(r.run, r.stream, r.topic, ids)
	r.mu.Lock()
	r.grouping = false
	if err != nil {
		r.regroup = true
		r.regroupAt = time.Now().Add(r.config.retryInterval)
		r.config.logger.Warn("Failed to group the partitions of a reader, it tries again", "error", err)
		for _, p := range r.partitions {
			p.retryAt = r.regroupAt
		}
	} else {
		r.groups = groups
	}
	r.mu.Unlock()
	r.signal()
}

func (r *PartitionReader) refreshAssignment() {
	defer r.inFlight.Done()
	assignment, err := r.client.SyncConsumerGroup(r.run, r.stream, r.topic, r.consumer.Id)
	if err != nil && errors.Is(err, ierror.ErrConsumerGroupMemberNotFound) {
		if err = r.client.JoinConsumerGroup(r.run, r.stream, r.topic, r.consumer.Id); err == nil {
			assignment, err = r.client.SyncConsumerGroup(r.run, r.stream, r.topic, r.consumer.Id)
		}
	}
	r.mu.Lock()
	r.refreshing = false
	r.refreshAt = time.Now().Add(r.config.retryInterval)
	if err != nil {
		r.config.logger.Warn("Failed to read the group assignment of a reader, it tries again", "error", err)
	} else if assignment != nil {
		r.assignLocked(assignment.Partitions)
	}
	r.broadcastLocked()
	r.mu.Unlock()
	r.signal()
}

func (r *PartitionReader) assignLocked(assigned []uint32) {
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
		if p.counted != nil && (p.stored == nil || p.counted.Offset() > p.stored.Offset()) {
			// The partition goes to another member, so its counted position is stored first.
			r.retiring = append(r.retiring, *p.counted)
		}
		changed = true
	}
	for _, id := range assigned {
		if _, ok := r.partitions[id]; !ok {
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
}

func (r *PartitionReader) abandon(operationID uint64) {
	r.inFlight.Add(1)
	go func() {
		defer r.inFlight.Done()
		if err := r.client.AbandonOperation(r.run, operationID); err != nil {
			r.config.logger.Warn("Failed to abandon a lost read", "operation", operationID, "error", err)
		}
	}()
}

func checkpointPosition(checkpoint iggcon.Checkpoint) iggcon.ConsumerPosition {
	return iggcon.ConsumerPosition{PartitionId: checkpoint.PartitionID(), Offset: checkpoint.Offset(), Context: checkpoint.Context()}
}
