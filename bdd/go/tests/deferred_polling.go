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

package tests

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/cucumber/godog"
)

const (
	deferredPollingCount = 10
	// A message that reaches the server before the call answers it at once,
	// so a start step waits until its call is parked.
	deferredPollingParkTime           = 100 * time.Millisecond
	deferredPollingAssignmentBudget   = 20 * time.Second
	deferredPollingAssignmentInterval = 50 * time.Millisecond
)

type deferredPollingCtxKey struct{}

type deferredPollingCtx struct {
	partitions   uint32
	group        iggcon.Consumer
	groupTargets []iggcon.FetchMessagesTarget
	poll         *deferredCall[*iggcon.PolledMessage]
	fetch        *deferredCall[[]iggcon.FetchMessagesSection]
}

func getDeferredPollingCtx(ctx context.Context) *deferredPollingCtx {
	return ctx.Value(deferredPollingCtxKey{}).(*deferredPollingCtx)
}

type deferredPollingSteps struct{}

func (deferredPollingSteps) givenStreamWithTopic(ctx context.Context, stream, topic string, partitions uint32) error {
	client := getResourceOptionsCtx(ctx).client
	name, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return fmt.Errorf("stream id %q: %w", stream, err)
	}
	// A run that ended before its cleanup leaves the stream behind.
	_, err = client.GetStream(ctx, name)
	switch {
	case err == nil:
		if err := client.DeleteStream(ctx, name); err != nil {
			return fmt.Errorf("delete stream %s: %w", stream, err)
		}
	case !errors.Is(err, ierror.ErrStreamIdNotFound):
		return fmt.Errorf("get stream %s: %w", stream, err)
	}
	options := resourceOptionsSteps{}
	if err := options.createStream(ctx, stream, nil); err != nil {
		return err
	}
	if err := options.whenCreateTopic(ctx, topic, partitions); err != nil {
		return err
	}
	getDeferredPollingCtx(ctx).partitions = partitions
	return nil
}

func (s deferredPollingSteps) whenStartDeferredPoll(ctx context.Context, partition uint32, offset uint64, maxWait int) error {
	return s.startPoll(ctx, partition, iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(offset), false,
		deferredOptions(time.Duration(maxWait)*time.Second))
}

func (s deferredPollingSteps) whenStartCommittingDeferredPoll(
	ctx context.Context,
	partition uint32,
	consumerName string,
	minCount uint32,
	maxWait int,
) error {
	consumer, err := namedConsumer(consumerName)
	if err != nil {
		return err
	}
	options := deferredOptions(time.Duration(maxWait) * time.Second)
	options.MinCount = minCount
	return s.startPoll(ctx, partition, consumer, iggcon.NextPollingStrategy(), true, options)
}

func (deferredPollingSteps) startPoll(
	ctx context.Context,
	partition uint32,
	consumer iggcon.Consumer,
	strategy iggcon.PollingStrategy,
	autoCommit bool,
	options iggcon.DeferredPollOptions,
) error {
	state := getResourceOptionsCtx(ctx)
	streamId, topicId := streamIdentifier(state), topicIdentifier(state)
	poll := startDeferredCall(ctx, func(callCtx context.Context) (*iggcon.PolledMessage, error) {
		return state.client.PollMessagesDeferred(callCtx, streamId, topicId, consumer, strategy,
			deferredPollingCount, autoCommit, &partition, options)
	})
	getDeferredPollingCtx(ctx).poll = poll
	return poll.parked()
}

func (deferredPollingSteps) whenSendToPartition(ctx context.Context, payload string, partition uint32) error {
	state := getResourceOptionsCtx(ctx)
	message, err := iggcon.NewIggyMessage([]byte(payload))
	if err != nil {
		return fmt.Errorf("build message: %w", err)
	}
	if _, err := state.client.SendMessages(ctx, streamIdentifier(state), topicIdentifier(state),
		iggcon.PartitionId(partition), []iggcon.IggyMessage{message}); err != nil {
		return fmt.Errorf("send message: %w", err)
	}
	return nil
}

func (deferredPollingSteps) thenDeferredPollReturns(ctx context.Context, payload string, within int) error {
	polled, err := getDeferredPollingCtx(ctx).poll.result(time.Duration(within) * time.Second)
	if err != nil {
		return fmt.Errorf("deferred poll: %w", err)
	}
	return singlePayload(polled, payload)
}

func (deferredPollingSteps) thenDeferredPollReturnsNothing(ctx context.Context, from, to int) error {
	poll := getDeferredPollingCtx(ctx).poll
	polled, err := poll.result(time.Until(poll.started.Add(time.Duration(to) * time.Second)))
	if err != nil {
		return fmt.Errorf("deferred poll: %w", err)
	}
	if len(polled.Messages) != 0 {
		return fmt.Errorf("the deferred poll returned %d messages, want none", len(polled.Messages))
	}
	if took := poll.ended.Sub(poll.started); took < time.Duration(from)*time.Second {
		return fmt.Errorf("the deferred poll returned after %v, before %d s", took, from)
	}
	return nil
}

func (deferredPollingSteps) whenStartFetch(
	ctx context.Context,
	first, second, third uint32,
	offset uint64,
	maxWait int,
) error {
	state := getResourceOptionsCtx(ctx)
	streamId, topicId := streamIdentifier(state), topicIdentifier(state)
	groups, err := state.client.ReadinessGroups(ctx, streamId, topicId, []uint32{first, second, third})
	if err != nil {
		return fmt.Errorf("readiness groups: %w", err)
	}
	if len(groups) != 1 {
		return fmt.Errorf("the test server has %d readiness groups, want one", len(groups))
	}
	var targets []iggcon.FetchMessagesTarget
	for _, partition := range groups[0] {
		targets = append(targets, iggcon.FetchMessagesTarget{
			PartitionId: partition,
			Strategy:    iggcon.OffsetPollingStrategy(offset),
		})
	}
	options := deferredOptions(time.Duration(maxWait) * time.Second)
	fetch := startDeferredCall(ctx, func(callCtx context.Context) ([]iggcon.FetchMessagesSection, error) {
		return state.client.FetchMessages(callCtx, streamId, topicId, iggcon.DefaultConsumer(), targets,
			deferredPollingCount, false, options)
	})
	getDeferredPollingCtx(ctx).fetch = fetch
	return fetch.parked()
}

func (deferredPollingSteps) thenFetchReturns(ctx context.Context, partition uint32, within int) error {
	sections, err := getDeferredPollingCtx(ctx).fetch.result(time.Duration(within) * time.Second)
	if err != nil {
		return fmt.Errorf("fetch: %w", err)
	}
	if len(sections) != 1 || sections[0].PartitionId != partition {
		return fmt.Errorf("the fetch returned sections %+v, want only partition %d", sections, partition)
	}
	polled, err := binaryserialization.DeserializeSectionMessages(sections[0])
	if err != nil {
		return fmt.Errorf("decode partition %d: %w", partition, err)
	}
	return singlePayload(polled, "ready")
}

func (deferredPollingSteps) givenSoleGroupMember(ctx context.Context, name string) error {
	state := getResourceOptionsCtx(ctx)
	streamId, topicId := streamIdentifier(state), topicIdentifier(state)
	group, err := state.client.CreateConsumerGroup(ctx, streamId, topicId, name)
	if err != nil {
		return fmt.Errorf("create consumer group %s: %w", name, err)
	}
	groupId, err := iggcon.NewIdentifier(group.Id)
	if err != nil {
		return fmt.Errorf("consumer group id %d: %w", group.Id, err)
	}
	if err := state.client.JoinConsumerGroup(ctx, streamId, topicId, groupId); err != nil {
		return fmt.Errorf("join consumer group %s: %w", name, err)
	}
	// A member owns a partition only once that partition installs it.
	partitions := getDeferredPollingCtx(ctx).partitions
	deadline := time.Now().Add(deferredPollingAssignmentBudget)
	for {
		assignment, err := state.client.SyncConsumerGroup(ctx, streamId, topicId, groupId)
		if err != nil {
			return fmt.Errorf("sync consumer group %s: %w", name, err)
		}
		if len(assignment.Partitions) == int(partitions) {
			groups, err := state.client.ReadinessGroups(ctx, streamId, topicId, assignment.Partitions)
			if err != nil {
				return fmt.Errorf("readiness groups: %w", err)
			}
			if len(groups) != 1 {
				return fmt.Errorf("the test server has %d readiness groups, want one", len(groups))
			}
			for _, partition := range groups[0] {
				getDeferredPollingCtx(ctx).groupTargets = append(getDeferredPollingCtx(ctx).groupTargets,
					iggcon.FetchMessagesTarget{PartitionId: partition, Strategy: iggcon.NextPollingStrategy()})
			}
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("after %v the only member owns the partitions %v of %d",
				deferredPollingAssignmentBudget, assignment.Partitions, partitions)
		}
		time.Sleep(deferredPollingAssignmentInterval)
	}
	getDeferredPollingCtx(ctx).group = iggcon.NewGroupConsumer(groupId)
	return nil
}

func (deferredPollingSteps) thenGroupFetchReturns(ctx context.Context, maxWait int, payload string, within int) error {
	state := getResourceOptionsCtx(ctx)
	streamId, topicId := streamIdentifier(state), topicIdentifier(state)
	deferred := getDeferredPollingCtx(ctx)
	options := deferredOptions(time.Duration(maxWait) * time.Second)
	// A deadline would shorten the wait to fit it, so the call is cancelled
	// when it overruns instead.
	fetch := startDeferredCall(ctx, func(callCtx context.Context) ([]iggcon.FetchMessagesSection, error) {
		return state.client.FetchMessages(callCtx, streamId, topicId, deferred.group, deferred.groupTargets,
			deferredPollingCount, true, options)
	})
	sections, err := fetch.result(time.Duration(within) * time.Second)
	if err != nil {
		return fmt.Errorf("group fetch: %w", err)
	}
	var messages []iggcon.IggyMessage
	for _, section := range sections {
		polled, err := binaryserialization.DeserializeSectionMessages(section)
		if err != nil {
			return fmt.Errorf("decode partition %d: %w", section.PartitionId, err)
		}
		index := slices.IndexFunc(deferred.groupTargets, func(target iggcon.FetchMessagesTarget) bool {
			return target.PartitionId == section.PartitionId
		})
		if index < 0 {
			return fmt.Errorf("the group fetch returned unassigned partition %d", section.PartitionId)
		}
		deferred.groupTargets[index].Strategy = iggcon.OffsetPollingStrategy(section.NextOffset).WithContext(section.Context)
		messages = append(messages, polled.Messages...)
	}
	return singlePayload(&iggcon.PolledMessage{Messages: messages}, payload)
}

func (deferredPollingSteps) whenCancelDeferredPoll(ctx context.Context, after int) error {
	poll := getDeferredPollingCtx(ctx).poll
	time.Sleep(time.Duration(after) * time.Millisecond)
	if !poll.running() {
		return fmt.Errorf("the deferred poll ended before its cancellation, with %+v and %v", poll.value, poll.err)
	}
	poll.stop()
	// The client reports a sent call whose context ended as not committed.
	// Only the cancellation keeps the server from completing it.
	if poll.value != nil || !errors.Is(poll.err, ierror.ErrTransientNotCommitted) {
		return fmt.Errorf("the cancelled deferred poll ended with %+v and %v, want %v",
			poll.value, poll.err, ierror.ErrTransientNotCommitted)
	}
	return nil
}

func (deferredPollingSteps) whenWait(seconds int) error {
	time.Sleep(time.Duration(seconds) * time.Second)
	return nil
}

func (deferredPollingSteps) thenNextPollReturns(ctx context.Context, partition uint32, consumerName, payload string) error {
	state := getResourceOptionsCtx(ctx)
	consumer, err := namedConsumer(consumerName)
	if err != nil {
		return err
	}
	polled, err := state.client.PollMessages(ctx, streamIdentifier(state), topicIdentifier(state), consumer,
		iggcon.NextPollingStrategy(), deferredPollingCount, false, &partition)
	if err != nil {
		return fmt.Errorf("poll messages: %w", err)
	}
	if len(polled.Messages) == 0 {
		return fmt.Errorf("the poll returned no message, want the payload %q", payload)
	}
	if first := string(polled.Messages[0].Payload); first != payload {
		return fmt.Errorf("the first polled message has the payload %q, want %q", first, payload)
	}
	return nil
}

func initDeferredPollingScenario(sc *godog.ScenarioContext) {
	sc.Before(func(context.Context, *godog.Scenario) (context.Context, error) {
		ctx := context.WithValue(context.Background(), resourceOptionsCtxKey{}, &resourceOptionsCtx{})
		return context.WithValue(ctx, deferredPollingCtxKey{}, &deferredPollingCtx{}), nil
	})
	options := resourceOptionsSteps{}
	s := deferredPollingSteps{}
	sc.Step(`^I have a running Iggy server$`, options.givenRunningServer)
	sc.Step(`^I am authenticated as the root user$`, options.givenAuthenticationAsRoot)
	sc.Step(`^a stream named "([^"]+)" with a topic named "([^"]+)" with (\d+) partitions exists$`, s.givenStreamWithTopic)
	sc.Step(`^I start a deferred poll of partition (\d+) from offset (\d+) with a max wait of (\d+) seconds?$`,
		s.whenStartDeferredPoll)
	sc.Step(`^I start a deferred poll of partition (\d+) for consumer "([^"]+)" with auto commit, a min count of (\d+) and a max wait of (\d+) seconds?$`,
		s.whenStartCommittingDeferredPoll)
	sc.Step(`^I send a message with payload "([^"]+)" to partition (\d+)$`, s.whenSendToPartition)
	sc.Step(`^the deferred poll should return the payload "([^"]+)" within (\d+) seconds?$`, s.thenDeferredPollReturns)
	sc.Step(`^the deferred poll should return no messages between (\d+) and (\d+) seconds after it started$`,
		s.thenDeferredPollReturnsNothing)
	sc.Step(`^I start a fetch on partitions (\d+), (\d+) and (\d+) from offset (\d+) with a max wait of (\d+) seconds?$`,
		s.whenStartFetch)
	sc.Step(`^the fetch should return only partition (\d+) within (\d+) seconds?$`, s.thenFetchReturns)
	sc.Step(`^I am the only member of consumer group "([^"]+)" on the topic$`, s.givenSoleGroupMember)
	sc.Step(`^a group fetch with a max wait of (\d+) seconds? should return the payload "([^"]+)" within (\d+) seconds?$`,
		s.thenGroupFetchReturns)
	sc.Step(`^I cancel the deferred poll after (\d+) milliseconds$`, s.whenCancelDeferredPoll)
	sc.Step(`^I wait (\d+) seconds?$`, s.whenWait)
	sc.Step(`^a poll of partition (\d+) for consumer "([^"]+)" with the next strategy should return the payload "([^"]+)"$`,
		s.thenNextPollReturns)
	sc.After(func(ctx context.Context, scenario *godog.Scenario, scenarioError error) (context.Context, error) {
		// A call that a failed step left parked must end before the client closes.
		state := getDeferredPollingCtx(ctx)
		if state.poll != nil {
			state.poll.stop()
		}
		if state.fetch != nil {
			state.fetch.stop()
		}
		return resourceOptionsCleanup(ctx, scenario, scenarioError)
	})
}

// deferredCall is a deferred poll or fetch that runs while the
// scenario goes on.
type deferredCall[T any] struct {
	started time.Time
	cancel  context.CancelFunc
	done    chan struct{}
	// value, err and ended are set before done closes.
	value T
	err   error
	ended time.Time
}

func startDeferredCall[T any](ctx context.Context, call func(context.Context) (T, error)) *deferredCall[T] {
	callCtx, cancel := context.WithCancel(ctx)
	running := &deferredCall[T]{started: time.Now(), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(running.done)
		running.value, running.err = call(callCtx)
		running.ended = time.Now()
	}()
	return running
}

func (c *deferredCall[T]) parked() error {
	time.Sleep(deferredPollingParkTime)
	if !c.running() {
		return fmt.Errorf("the call ended before it parked, with %+v and %v", c.value, c.err)
	}
	return nil
}

func (c *deferredCall[T]) running() bool {
	select {
	case <-c.done:
		return false
	default:
		return true
	}
}

// result waits up to within for the call and returns what it returned. A
// call that still runs then is cancelled, so that it never outlives its step.
func (c *deferredCall[T]) result(within time.Duration) (T, error) {
	timer := time.NewTimer(within)
	defer timer.Stop()
	select {
	case <-c.done:
	case <-timer.C:
		if c.running() {
			c.stop()
			var none T
			return none, fmt.Errorf("the call did not end within %v", within)
		}
	}
	return c.value, c.err
}

// stop cancels the call and waits until it ends.
func (c *deferredCall[T]) stop() {
	c.cancel()
	<-c.done
}

// deferredOptions are the SDK defaults with the max wait of the step. The
// request timeout grows when the default does not cover the wait and its
// reply margin.
func deferredOptions(maxWait time.Duration) iggcon.DeferredPollOptions {
	options := iggcon.DefaultDeferredPollOptions()
	options.MaxWait = maxWait
	options.RequestTimeout = max(options.RequestTimeout, maxWait+iggcon.MinDeferredPollReplyMargin)
	return options
}

func namedConsumer(name string) (iggcon.Consumer, error) {
	id, err := iggcon.NewIdentifier(name)
	if err != nil {
		return iggcon.Consumer{}, fmt.Errorf("consumer id %q: %w", name, err)
	}
	return iggcon.NewSingleConsumer(id), nil
}

func singlePayload(polled *iggcon.PolledMessage, want string) error {
	payloads := make([]string, len(polled.Messages))
	for index, message := range polled.Messages {
		payloads[index] = string(message.Payload)
	}
	if !slices.Equal(payloads, []string{want}) {
		return fmt.Errorf("the poll returned the payloads %q, want only %q", payloads, want)
	}
	return nil
}
