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
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"maps"
	"math"
	"slices"
	"sync"
	"time"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
)

// Layouts of the deferred requests. A deferred poll is a poll body followed by
// [wait_us u64][min_count u32][max_bytes u32][request_timeout_us u64]. A fetch is
// [consumer][stream][topic][wait_us u64][request_timeout_us u64]
// [min_count u32][count u32][max_bytes u32][auto_commit u8][targets u32],
// then that many targets of [partition_id u32][strategy kind u8]
// [strategy value u64][max_bytes u32][incarnation u64][owner_generation u64].
const (
	wireDurationSize         = 8
	deferredParametersSize   = wireDurationSize + 4 + 4 + wireDurationSize
	fetchAutoCommitAt        = 2*wireDurationSize + 3*4
	fetchParametersSize      = fetchAutoCommitAt + 1 + 4
	fetchTargetMaxBytesAt    = partitionIdSize + 1 + 8
	fetchTargetIncarnationAt = fetchTargetMaxBytesAt + 4
	fetchTargetSize          = fetchTargetIncarnationAt + 2*8
)

// readinessLookups bounds the route lookups that one call runs at once, as in
// the Rust SDK.
const readinessLookups = 16

// FetchMessages reads the partitions of one primary in one request, as a
// reader reads one readiness group. Read partitions with reader.PartitionReader,
// which groups them, keeps their positions and recovers lost replies.
func (c *IggyTcpClient) FetchMessages(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	consumer iggcon.Consumer,
	targets []iggcon.FetchMessagesTarget,
	count uint32,
	autoCommit bool,
	options iggcon.DeferredPollOptions,
) ([]iggcon.FetchMessagesSection, error) {
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	// The encoding cuts the limits to whole microseconds, so they are checked
	// before it.
	if err := options.ValidateFetch(count); err != nil {
		return nil, err
	}
	request := &command.FetchMessages{
		StreamId: streamId, TopicId: topicId, Consumer: consumer, Targets: make([]command.FetchTarget, 0, len(targets)),
		Count: count, AutoCommit: autoCommit, Options: options,
	}
	// Each target may take the whole budget, so that the server serves a
	// ready target in full instead of slicing its batches.
	for _, target := range targets {
		request.Targets = append(request.Targets, command.FetchTarget{FetchMessagesTarget: target, MaxBytes: options.MaxBytes})
	}
	body, err := request.MarshalBinary()
	if err != nil {
		return nil, err
	}
	fetch, err := parseFetch(body)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.deferredContext(ctx, options.RequestTimeout)
	defer cancel()
	_, sections, err := c.fetchMessages(ctx, fetch)
	return sections, err
}

// ReadinessGroups groups partitions by their primary for a reader, in
// ascending primary id order, with at most MaxPartitionsPerRequest in a group.
func (c *IggyTcpClient) ReadinessGroups(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	partitions []uint32,
) ([][]uint32, error) {
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	topic, err := iggcon.MarshalIdentifiers(streamId, topicId)
	if err != nil {
		return nil, err
	}
	routes, err := c.partitionRoutes(ctx, topic, partitions)
	if err != nil {
		return nil, err
	}
	primaries := make(map[uint8][]uint32)
	for index, route := range routes {
		primaries[route.primary] = append(primaries[route.primary], partitions[index])
	}
	groups := make([][]uint32, 0, len(primaries))
	for _, primary := range slices.Sorted(maps.Keys(primaries)) {
		groups = slices.AppendSeq(groups, slices.Chunk(primaries[primary], iggcon.MaxPartitionsPerRequest))
	}
	return groups, nil
}

// fetchRequest is a fetch body that parseFetch accepted.
type fetchRequest struct {
	body []byte
	// topic is the encoded stream and topic, the start of each route query.
	topic   []byte
	options iggcon.DeferredPollOptions
	// targets is where the targets start in body.
	targets int
}

// parseFetch refuses a fetch that the server refuses, before anything is
// sent.
func parseFetch(body []byte) (fetchRequest, error) {
	topic, position, err := pollTarget(body)
	if err != nil {
		return fetchRequest{}, err
	}
	targets := position + fetchParametersSize
	if len(body) < targets {
		return fetchRequest{}, ierror.ErrInvalidCommand
	}
	count := binary.LittleEndian.Uint32(body[targets-4:])
	if count == 0 || count > iggcon.MaxPartitionsPerRequest || uint64(len(body)-targets) != uint64(count)*fetchTargetSize {
		return fetchRequest{}, ierror.ErrInvalidCommand
	}
	// The limits are [wait_us u64][request_timeout_us u64][min_count u32]
	// [count u32][max_bytes u32].
	limits := body[position:targets]
	fetch := fetchRequest{body: body, topic: topic, targets: targets, options: iggcon.DeferredPollOptions{
		MaxWait:        wireDuration(limits),
		RequestTimeout: wireDuration(limits[wireDurationSize:]),
		MinCount:       binary.LittleEndian.Uint32(limits[2*wireDurationSize:]),
		MaxBytes:       binary.LittleEndian.Uint32(limits[2*wireDurationSize+8:]),
	}}
	if err := fetch.options.ValidateFetch(binary.LittleEndian.Uint32(limits[2*wireDurationSize+4:])); err != nil {
		return fetchRequest{}, err
	}
	seen := make(map[uint32]struct{}, count)
	for offset := targets; offset < len(body); offset += fetchTargetSize {
		target := body[offset : offset+fetchTargetSize]
		partition := binary.LittleEndian.Uint32(target)
		if _, duplicate := seen[partition]; duplicate {
			return fetchRequest{}, ierror.ErrInvalidCommand
		}
		seen[partition] = struct{}{}
		// The server splits count over the targets, and another start point
		// would loop or misread under a split count.
		if kind := iggcon.MessagePolling(target[partitionIdSize]); kind != iggcon.POLLING_NEXT && kind != iggcon.POLLING_OFFSET {
			return fetchRequest{}, ierror.ErrInvalidCommand
		}
		if limit := binary.LittleEndian.Uint32(target[fetchTargetMaxBytesAt:]); limit < iggcon.MinDeferredPollMaxBytes || limit > fetch.options.MaxBytes {
			return fetchRequest{}, ierror.ErrInvalidSizeBytes
		}
	}
	return fetch, nil
}

// fetchMessages sends fetch to the primary of its first target, and returns
// the reply with its sections. The caller groups the targets by primary, so
// a target that moved gets a NotPrimary section. A refused section marks the
// route of its partition, and a refusal of the whole fetch, which names no
// target, marks the routes of every target. So the next ReadinessGroups and
// the next fetch look them up again.
func (c *IggyTcpClient) fetchMessages(ctx context.Context, fetch fetchRequest) ([]byte, []iggcon.FetchMessagesSection, error) {
	if err := c.fenceTargets(ctx, &fetch); err != nil {
		return nil, nil, err
	}
	completion := &fetchCompletion{}
	ctx = context.WithValue(ctx, fetchCompletionKey{}, completion)
	reply, err := c.do(ctx, rawRequest{code: command.FetchMessagesCode, payload: fetch.body})
	if err != nil {
		for offset := fetch.targets; offset < len(fetch.body); offset += fetchTargetSize {
			c.polls.refusedAt(routeQuery(fetch.topic, binary.LittleEndian.Uint32(fetch.body[offset:])), err)
		}
		return nil, nil, err
	}
	sections, err := binaryserialization.DeserializeFetchSections(reply, c.MessageCompression)
	if err != nil {
		return nil, nil, err
	}
	for index := range sections {
		section := &sections[index]
		if section.Err != nil {
			c.polls.refusedAt(routeQuery(fetch.topic, section.PartitionId), section.Err)
			if slices.Contains(completion.uncertain, section.PartitionId) {
				section.Err = ierror.FetchProgressUncertain{OperationID: completion.operationID}
			}
		}
	}
	return reply, sections, nil
}

type fetchCompletionKey struct{}

type fetchCompletion struct {
	operationID uint64
	uncertain   []uint32
}

// classifyFetchReply validates only the section table while all progress lanes
// are still held. Record decoding remains the caller's responsibility.
func classifyFetchReply(ctx context.Context, operation *pendingOperation, ticket *laneTicket, body []byte) error {
	sections, err := binaryserialization.DeserializeFetchSections(body, "")
	if err != nil {
		return err
	}
	requested := make(map[uint32]laneKey, len(ticket.fetch.keys))
	for _, key := range ticket.fetch.keys {
		requested[key.partition] = key
	}
	unknown := make([]laneKey, 0, len(sections))
	for _, section := range sections {
		key, found := requested[section.PartitionId]
		if !found {
			return ierror.ErrInvalidCommand
		}
		delete(requested, section.PartitionId)
		if section.Err != nil && !fetchRefusedBeforeAdmission(section.Err, true) {
			unknown = append(unknown, key)
		}
	}
	if len(unknown) > 0 {
		_ = ticket.retainFetch(unknown)
		if completion, ok := ctx.Value(fetchCompletionKey{}).(*fetchCompletion); ok {
			completion.operationID = operation.id
			for _, key := range unknown {
				completion.uncertain = append(completion.uncertain, key.partition)
			}
		}
	}
	return nil
}

// fetchRefusedBeforeAdmission reports whether a committing poll or fetch that
// failed with err, as its whole reply or as one fetch section, stored nothing.
func fetchRefusedBeforeAdmission(err error, section bool) bool {
	var coded ierror.IggyError
	if !errors.As(err, &coded) {
		return false
	}
	switch coded.Code() {
	case ierror.TransientNotAcceptedCode, ierror.InvalidSizeBytesCode, ierror.HistoryUnavailableCode, ierror.NotPrimaryCode,
		ierror.PartitionNotFoundCode, ierror.TooManyConsumerOffsetsCode, ierror.ConsumerGroupPartitionNotOwnedCode:
		return true
	case ierror.FeatureUnavailableCode, ierror.InvalidIdentifierCode, ierror.DeferredPollCapacityExceededCode,
		ierror.StreamIdNotFoundCode, ierror.StreamNameNotFoundCode, ierror.TopicIdNotFoundCode, ierror.TopicNameNotFoundCode,
		ierror.InvalidOffsetCode:
		return !section
	default:
		return false
	}
}

// Zero wire incarnations select the current history. Resolve them before
// lane admission so raw and typed calls share the same progress identity.
func (c *IggyTcpClient) fenceTargets(ctx context.Context, fetch *fetchRequest) error {
	var unfenced []int
	var partitions []uint32
	for offset := fetch.targets; offset < len(fetch.body); offset += fetchTargetSize {
		target := fetch.body[offset:]
		if binary.LittleEndian.Uint64(target[fetchTargetIncarnationAt:]) == 0 {
			unfenced = append(unfenced, offset)
			partitions = append(partitions, binary.LittleEndian.Uint32(target))
		}
	}
	if len(unfenced) == 0 {
		return nil
	}
	routes, err := c.partitionRoutes(ctx, fetch.topic, partitions)
	if err != nil {
		return err
	}
	fetch.body = append([]byte(nil), fetch.body...)
	for lookup, offset := range unfenced {
		if routes[lookup].context.Incarnation == 0 {
			return ierror.ErrTransientNotAccepted
		}
		incarnation := fetch.body[offset+fetchTargetIncarnationAt:]
		binary.LittleEndian.PutUint64(incarnation, routes[lookup].context.Incarnation)
	}
	return nil
}

// fetchParameters returns where the parameters of a fetch body start, after
// its consumer, stream and topic, or false for a body too short for them.
func fetchParameters(body []byte) (int, bool) {
	_, position, err := pollTarget(body)
	return position, err == nil && len(body)-position >= fetchParametersSize
}

// isProgressFetch reports whether a fetch body commits or has a Next target:
// either moves or reads the stored offset of its partitions.
func isProgressFetch(body []byte) bool {
	position, ok := fetchParameters(body)
	if !ok {
		return false
	}
	if body[position+fetchAutoCommitAt] != 0 {
		return true
	}
	for target := position + fetchParametersSize; len(body)-target >= fetchTargetSize; target += fetchTargetSize {
		if body[target+partitionIdSize] == byte(iggcon.POLLING_NEXT) {
			return true
		}
	}
	return false
}

// fetchLanes are the progress lanes of the partitions of a fetch body, in
// ascending partition order, which enterLanes needs.
func fetchLanes(body []byte) []laneKey {
	position, ok := fetchParameters(body)
	if !ok {
		return nil
	}
	targets := body[position+fetchParametersSize:]
	keys := make([]laneKey, 0, len(targets)/fetchTargetSize)
	for ; len(targets) >= fetchTargetSize; targets = targets[fetchTargetSize:] {
		keys = append(keys, laneKey{
			incarnation: binary.LittleEndian.Uint64(targets[fetchTargetIncarnationAt:]),
			partition:   binary.LittleEndian.Uint32(targets),
		}.progressLane())
	}
	slices.SortFunc(keys, func(a, b laneKey) int { return cmp.Compare(a.partition, b.partition) })
	return keys
}

// partitionRoutes looks up the routes of partitions of topic, readinessLookups
// at once. The first failure in the order of partitions fails it.
func (c *IggyTcpClient) partitionRoutes(ctx context.Context, topic []byte, partitions []uint32) ([]pollRoute, error) {
	routes := make([]pollRoute, len(partitions))
	failures := make([]error, len(partitions))
	slots := make(chan struct{}, readinessLookups)
	var lookups sync.WaitGroup
	for index, partition := range partitions {
		slots <- struct{}{}
		lookups.Go(func() {
			defer func() { <-slots }()
			routes[index], failures[index] = c.readinessRoute(ctx, routeQuery(topic, partition))
		})
	}
	lookups.Wait()
	for _, failure := range failures {
		if failure != nil {
			return nil, failure
		}
	}
	return routes, nil
}

// readinessRoute is the route of the partition of query. A lookup that fails
// before admission is retried until the call ends.
func (c *IggyTcpClient) readinessRoute(ctx context.Context, query []byte) (pollRoute, error) {
	delay := replayInterval
	for {
		if err := c.waitForRecovery(ctx); err != nil {
			return pollRoute{}, err
		}
		route, err := c.consumerRoute(ctx, query)
		if err == nil {
			return route, nil
		}
		if c.sessionRecovers(ctx, err) {
			continue
		}
		if !retryBeforeAdmission(err) {
			return pollRoute{}, err
		}
		if c.pause(ctx, nextLookupBackoff(&delay)) != nil {
			return pollRoute{}, ierror.ErrTransientNotAccepted
		}
	}
}

// rawDeferredPollOptions reads the limits of a raw deferred poll and refuses
// a poll that the server refuses, before anything is sent.
func rawDeferredPollOptions(payload []byte) (iggcon.DeferredPollOptions, error) {
	if _, err := partitionQuery(command.PollMessagesDeferredCode, payload); err != nil {
		return iggcon.DeferredPollOptions{}, err
	}
	limits := payload[len(payload)-deferredParametersSize:]
	// The poll parameters are [kind u8][value u64][count u32][auto_commit u8].
	parameters := payload[len(payload)-deferredParametersSize-pollParametersSize:]
	count := binary.LittleEndian.Uint32(parameters[1+8:])
	options := iggcon.DeferredPollOptions{
		MaxWait:        wireDuration(limits),
		MinCount:       binary.LittleEndian.Uint32(limits[wireDurationSize:]),
		MaxBytes:       binary.LittleEndian.Uint32(limits[wireDurationSize+4:]),
		RequestTimeout: wireDuration(limits[deferredParametersSize-wireDurationSize:]),
	}
	return options, options.Validate(count)
}

// deferredBudget is the budget of a deferred poll or fetch when its call
// reached the driver, and where its frame carries the budget.
type deferredBudget struct {
	started   time.Time
	options   iggcon.DeferredPollOptions
	waitAt    int
	timeoutAt int
	// minTimeout is the shortest request timeout that the server accepts.
	minTimeout time.Duration
	// storesOnRead marks a poll or fetch with auto-commit. A cancel of one
	// waits for the single reply of the server, which tells whether it stored.
	storesOnRead bool
}

// newDeferredBudget reads the budget of a deferred poll or fetch frame that
// passed partitionQuery. It returns nil for any other request.
func newDeferredBudget(code command.Code, frame []byte) *deferredBudget {
	var waitAt int
	switch code {
	case command.PollMessagesDeferredCode:
		waitAt = len(frame) - deferredParametersSize
	case command.FetchMessagesCode:
		_, position, err := pollTarget(frame[vsr.HeaderSize:])
		if err != nil {
			return nil
		}
		waitAt = vsr.HeaderSize + position
	default:
		return nil
	}
	timeoutAt := waitAt + wireDurationSize
	if code == command.PollMessagesDeferredCode {
		timeoutAt = len(frame) - wireDurationSize
	}
	budget := &deferredBudget{
		started:    time.Now(),
		options:    iggcon.DeferredPollOptions{MaxWait: wireDuration(frame[waitAt:]), RequestTimeout: wireDuration(frame[timeoutAt:])},
		waitAt:     waitAt,
		timeoutAt:  timeoutAt,
		minTimeout: time.Microsecond,
	}
	if code == command.FetchMessagesCode {
		budget.minTimeout = iggcon.MinDeferredPollReplyMargin
	}
	budget.storesOnRead = storesOnRead(code, frame[vsr.HeaderSize:])
	return budget
}

// attempt is the frame of the next attempt: the request with what is left of
// its budget, cut to the deadline of the call so that the server answers
// before the caller stops waiting. It is a copy, because the writer of an
// earlier attempt can still hold the frame.
func (b *deferredBudget) attempt(ctx context.Context, frame []byte) (*operationFrame, error) {
	options, err := b.options.Remaining(time.Since(b.started))
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		options.RequestTimeout = min(options.RequestTimeout, time.Until(deadline))
	}
	if options.RequestTimeout < b.minTimeout {
		return nil, ierror.ErrTransientNotAccepted
	}
	options.MaxWait = min(options.MaxWait, max(options.RequestTimeout-iggcon.MinDeferredPollReplyMargin, 0))
	attempt := append([]byte(nil), frame...)
	binary.LittleEndian.PutUint64(attempt[b.waitAt:], command.Micros(options.MaxWait))
	binary.LittleEndian.PutUint64(attempt[b.timeoutAt:], command.Micros(options.RequestTimeout))
	operation := newOperationFrame(attempt, nil)
	operation.uncancellable = b.storesOnRead
	return operation, nil
}

// storesOnRead reports whether the body of a poll or fetch asks the server to
// store the offsets of what it reads.
func storesOnRead(code command.Code, body []byte) bool {
	switch code {
	case command.PollMessagesCode:
		return len(body) > 0 && body[len(body)-1] != 0
	case command.PollMessagesDeferredCode:
		return len(body) > deferredParametersSize && body[len(body)-deferredParametersSize-1] != 0
	case command.FetchMessagesCode:
		_, position, err := pollTarget(body)
		return err == nil && len(body) > position+fetchAutoCommitAt && body[position+fetchAutoCommitAt] != 0
	default:
		return false
	}
}

// wireDuration reads microseconds as the wire carries them, saturated at the
// longest Duration.
func wireDuration(micros []byte) time.Duration {
	value := binary.LittleEndian.Uint64(micros)
	if value > math.MaxInt64/uint64(time.Microsecond) {
		return math.MaxInt64
	}
	return time.Duration(value) * time.Microsecond
}
