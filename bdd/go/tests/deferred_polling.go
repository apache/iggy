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

	"github.com/apache/iggy/bdd/go/tests/env"
	"github.com/apache/iggy/foreign/go/client/reader"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/cucumber/godog"
)

const (
	// A message that reaches the server before the call answers it at once,
	// so a start step waits until its call is parked.
	deferredPollingParkTime = 100 * time.Millisecond
)

type deferredPollingCtxKey struct{}

type deferredPollingCtx struct {
	partitions uint32
	poll       *deferredCall[*iggcon.PolledMessage]
	reader     *reader.PartitionReader
	consumer   *reader.IggyConsumer
	// lostOperation is the operation of the deferred poll that failed as uncertain.
	lostOperation uint64
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
	return s.startPoll(ctx, partition, iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(offset),
		waitOptions(maxWait))
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
	options := waitOptions(maxWait)
	options.MinCount = minCount
	options.Commit = iggcon.OnReadCommit()
	return s.startPoll(ctx, partition, consumer, iggcon.NextPollingStrategy(), options)
}

func (s deferredPollingSteps) whenStartOnReadDeferredPoll(ctx context.Context, partition uint32, consumerName string, maxWait int) error {
	// A read that stores nothing looks up the route first, so the committing
	// poll reaches the server before a later step drops the connection.
	state := getResourceOptionsCtx(ctx)
	if _, err := state.client.PollMessages(ctx, streamIdentifier(state), topicIdentifier(state), partition,
		iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(0), iggcon.DefaultPollOptions()); err != nil {
		return fmt.Errorf("look up the route of partition %d: %w", partition, err)
	}
	return s.whenStartCommittingDeferredPoll(ctx, partition, consumerName, 1, maxWait)
}

func (deferredPollingSteps) startPoll(
	ctx context.Context,
	partition uint32,
	consumer iggcon.Consumer,
	strategy iggcon.PollingStrategy,
	options iggcon.PollOptions,
) error {
	state := getResourceOptionsCtx(ctx)
	streamId, topicId := streamIdentifier(state), topicIdentifier(state)
	poll := startDeferredCall(ctx, func(callCtx context.Context) (*iggcon.PolledMessage, error) {
		return state.client.PollMessages(callCtx, streamId, topicId, partition, consumer, strategy, options)
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

func (deferredPollingSteps) whenOpenReader(ctx context.Context, first, second, third uint32, offset uint64, maxWait int) error {
	state := getResourceOptionsCtx(ctx)
	partitionReader, err := reader.NewPartitionReader(ctx, state.client, streamIdentifier(state), topicIdentifier(state),
		iggcon.DefaultConsumer(), reader.WithPartitions(first, second, third),
		reader.WithStart(iggcon.OffsetPollingStrategy(offset)), reader.WithPollOptions(waitOptions(maxWait)))
	if err != nil {
		return fmt.Errorf("open partition reader: %w", err)
	}
	getDeferredPollingCtx(ctx).reader = partitionReader
	return nil
}

func (deferredPollingSteps) thenReaderReturns(ctx context.Context, payload string, partition uint32, within int) error {
	readCtx, cancel := context.WithTimeout(ctx, time.Duration(within)*time.Second)
	defer cancel()
	polled, err := getDeferredPollingCtx(ctx).reader.Next(readCtx)
	if err != nil {
		return fmt.Errorf("partition reader: %w", err)
	}
	if polled.PartitionId != partition {
		return fmt.Errorf("the reader returned partition %d, want %d", polled.PartitionId, partition)
	}
	return singlePayload(polled, payload)
}

func (s deferredPollingSteps) whenOpenGroupConsumer(ctx context.Context, group string, maxWait int) error {
	return s.openConsumer(ctx, group, waitOptions(maxWait))
}

func (s deferredPollingSteps) whenOpenProcessingConsumer(ctx context.Context, group string) error {
	options := iggcon.WaitPollOptions(reader.DefaultMaxWait)
	options.Commit = iggcon.AutoCommitPolicy(iggcon.DefaultAutoCommit())
	return s.openConsumer(ctx, group, options)
}

func (deferredPollingSteps) openConsumer(ctx context.Context, group string, options iggcon.PollOptions) error {
	state := getResourceOptionsCtx(ctx)
	streamId, topicId := streamIdentifier(state), topicIdentifier(state)
	groupId, err := iggcon.NewIdentifier(group)
	if err != nil {
		return fmt.Errorf("group id %q: %w", group, err)
	}
	if _, err := state.client.GetConsumerGroup(ctx, streamId, topicId, groupId); err != nil {
		if _, err := state.client.CreateConsumerGroup(ctx, streamId, topicId, group); err != nil {
			return fmt.Errorf("create consumer group %s: %w", group, err)
		}
	}
	consumer, err := reader.NewIggyConsumer(ctx, state.client, streamId, topicId, iggcon.NewGroupConsumer(groupId),
		reader.WithPollOptions(options))
	if err != nil {
		return fmt.Errorf("open consumer: %w", err)
	}
	getDeferredPollingCtx(ctx).consumer = consumer
	return nil
}

func (deferredPollingSteps) thenConsumerReturns(ctx context.Context, payload string, within int) error {
	readCtx, cancel := context.WithTimeout(ctx, time.Duration(within)*time.Second)
	defer cancel()
	message, err := getDeferredPollingCtx(ctx).consumer.Next(readCtx)
	if err != nil {
		return fmt.Errorf("consumer: %w", err)
	}
	if got := string(message.Message.Payload); got != payload {
		return fmt.Errorf("the consumer returned the payload %q, want %q", got, payload)
	}
	return nil
}

func (deferredPollingSteps) whenCloseConsumer(ctx context.Context) error {
	state := getDeferredPollingCtx(ctx)
	consumer := state.consumer
	state.consumer = nil
	return consumer.Shutdown(ctx)
}

func (deferredPollingSteps) thenOnReadPollReturns(ctx context.Context, partition uint32, consumerName, payload string) error {
	options := iggcon.DefaultPollOptions()
	options.Count = 1
	options.Commit = iggcon.OnReadCommit()
	return pollNext(ctx, partition, consumerName, payload, options)
}

func (deferredPollingSteps) whenCancelDeferredPoll(ctx context.Context, after int) error {
	poll := getDeferredPollingCtx(ctx).poll
	time.Sleep(time.Duration(after) * time.Millisecond)
	if !poll.running() {
		return fmt.Errorf("the deferred poll ended before its cancellation, with %+v and %v", poll.value, poll.err)
	}
	poll.stop()
	// The server answers a cancelled poll that stores its offset once, refused
	// when the cancel came before the read, which stored nothing.
	if poll.value != nil || !errors.Is(poll.err, ierror.ErrTransientNotAccepted) {
		return fmt.Errorf("the cancelled deferred poll ended with %+v and %v, want %v",
			poll.value, poll.err, ierror.ErrTransientNotAccepted)
	}
	return nil
}

// whenClientDisconnects drops the connection of the client that owns the poll, without a sign-out.
func (deferredPollingSteps) whenClientDisconnects(ctx context.Context) error {
	poll := getDeferredPollingCtx(ctx).poll
	if !poll.running() {
		return fmt.Errorf("the deferred poll ended before the disconnect, with %+v and %v", poll.value, poll.err)
	}
	client, err := disconnectable(getResourceOptionsCtx(ctx).client)
	if err != nil {
		return err
	}
	return client.Disconnect()
}

func (deferredPollingSteps) thenDeferredPollUncertain(ctx context.Context) error {
	state := getDeferredPollingCtx(ctx)
	polled, err := state.poll.result(5 * time.Second)
	var unknown ierror.FetchProgressUncertain
	if !errors.As(err, &unknown) {
		return fmt.Errorf("the deferred poll ended with %+v and %v, want %v", polled, err, ierror.ErrFetchProgressUncertain)
	}
	state.lostOperation = unknown.OperationID
	return nil
}

func (deferredPollingSteps) whenClientConnectsAgain(ctx context.Context) error {
	client := getResourceOptionsCtx(ctx).client
	if err := client.Connect(ctx); err != nil {
		return fmt.Errorf("connect client: %w", err)
	}
	username, password := env.RootCredentials()
	if _, err := client.LoginUser(ctx, username, password); err != nil {
		return fmt.Errorf("authenticate client: %w", err)
	}
	return nil
}

func (deferredPollingSteps) thenOnReadPollBlocked(ctx context.Context, partition uint32, consumerName string) error {
	state := getResourceOptionsCtx(ctx)
	consumer, err := namedConsumer(consumerName)
	if err != nil {
		return err
	}
	options := iggcon.DefaultPollOptions()
	options.Count = 1
	options.Commit = iggcon.OnReadCommit()
	_, err = state.client.PollMessages(ctx, streamIdentifier(state), topicIdentifier(state), partition, consumer,
		iggcon.NextPollingStrategy(), options)
	lost := getDeferredPollingCtx(ctx).lostOperation
	var blocked ierror.PartitionLaneBlocked
	if !errors.As(err, &blocked) || blocked.PartitionID != partition || blocked.OperationID != lost {
		return fmt.Errorf("the poll ended with %v, want %v", err, ierror.PartitionLaneBlocked{PartitionID: partition, OperationID: lost})
	}
	return nil
}

func (deferredPollingSteps) whenAbandonDeferredPoll(ctx context.Context) error {
	operation := getDeferredPollingCtx(ctx).lostOperation
	if err := getResourceOptionsCtx(ctx).client.AbandonOperation(ctx, operation); err != nil {
		return fmt.Errorf("abandon operation %d: %w", operation, err)
	}
	return nil
}

func (deferredPollingSteps) whenWait(seconds int) error {
	time.Sleep(time.Duration(seconds) * time.Second)
	return nil
}

func (deferredPollingSteps) thenNextPollReturns(ctx context.Context, partition uint32, consumerName, payload string) error {
	return pollNext(ctx, partition, consumerName, payload, iggcon.DefaultPollOptions())
}

// pollNext polls partition from the next offset of consumerName and expects payload first.
func pollNext(ctx context.Context, partition uint32, consumerName, payload string, options iggcon.PollOptions) error {
	state := getResourceOptionsCtx(ctx)
	consumer, err := namedConsumer(consumerName)
	if err != nil {
		return err
	}
	polled, err := state.client.PollMessages(ctx, streamIdentifier(state), topicIdentifier(state), partition, consumer,
		iggcon.NextPollingStrategy(), options)
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
	sc.Step(`^I start a deferred poll of partition (\d+) for consumer "([^"]+)" with the on read commit policy, a min count of (\d+) and a max wait of (\d+) seconds?$`,
		s.whenStartCommittingDeferredPoll)
	sc.Step(`^I start a deferred poll of partition (\d+) for consumer "([^"]+)" with the on read commit policy and a max wait of (\d+) seconds?$`,
		s.whenStartOnReadDeferredPoll)
	sc.Step(`^the client disconnects before the deferred poll answers$`, s.whenClientDisconnects)
	sc.Step(`^the deferred poll should fail as uncertain$`, s.thenDeferredPollUncertain)
	sc.Step(`^the client connects again$`, s.whenClientConnectsAgain)
	sc.Step(`^a poll of partition (\d+) for consumer "([^"]+)" with the next strategy and the on read commit policy should fail as blocked by the deferred poll$`,
		s.thenOnReadPollBlocked)
	sc.Step(`^I abandon the operation of the deferred poll$`, s.whenAbandonDeferredPoll)
	sc.Step(`^I send a message with payload "([^"]+)" to partition (\d+)$`, s.whenSendToPartition)
	sc.Step(`^the deferred poll should return the payload "([^"]+)" within (\d+) seconds?$`, s.thenDeferredPollReturns)
	sc.Step(`^the deferred poll should return no messages between (\d+) and (\d+) seconds after it started$`,
		s.thenDeferredPollReturnsNothing)
	sc.Step(`^a poll of partition (\d+) for consumer "([^"]+)" with the next strategy and the on read commit policy should return the payload "([^"]+)"$`,
		s.thenOnReadPollReturns)
	sc.Step(`^I cancel the deferred poll after (\d+) milliseconds$`, s.whenCancelDeferredPoll)
	sc.Step(`^I wait (\d+) seconds?$`, s.whenWait)
	sc.Step(`^a poll of partition (\d+) for consumer "([^"]+)" with the next strategy should return the payload "([^"]+)"$`,
		s.thenNextPollReturns)
	sc.Step(`^I open a partition reader on partitions (\d+), (\d+) and (\d+) from offset (\d+) with a max wait of (\d+) seconds?$`,
		s.whenOpenReader)
	sc.Step(`^the partition reader should return the payload "([^"]+)" from partition (\d+) within (\d+) seconds?$`,
		s.thenReaderReturns)
	sc.Step(`^I open a consumer in group "([^"]+)" on the topic with a max wait of (\d+) seconds?$`, s.whenOpenGroupConsumer)
	sc.Step(`^I open a consumer in group "([^"]+)" on the topic that commits after processing$`, s.whenOpenProcessingConsumer)
	sc.Step(`^the consumer should return the payload "([^"]+)" within (\d+) seconds?$`, s.thenConsumerReturns)
	sc.Step(`^I close the consumer$`, s.whenCloseConsumer)
	sc.After(func(ctx context.Context, scenario *godog.Scenario, scenarioError error) (context.Context, error) {
		// A poll, a reader or a consumer that still runs must end before the client closes.
		state := getDeferredPollingCtx(ctx)
		if state.poll != nil {
			state.poll.stop()
		}
		if state.reader != nil {
			_ = state.reader.Close(ctx)
		}
		if state.consumer != nil {
			_ = state.consumer.Shutdown(ctx)
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
	return parked([]*deferredCall[T]{c})
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
	c.stopping()()
}

// stopping cancels the call and returns the wait for its end. A committing
// fetch ignores the cancel and ends with its reply.
func (c *deferredCall[T]) stopping() func() {
	c.cancel()
	return func() { <-c.done }
}

// parked waits until the calls had time to park and fails when one of them
// ended before.
func parked[T any](calls []*deferredCall[T]) error {
	time.Sleep(deferredPollingParkTime)
	for _, call := range calls {
		if !call.running() {
			return fmt.Errorf("the call ended before it parked, with %+v and %v", call.value, call.err)
		}
	}
	return nil
}

// waitOptions are the poll defaults with the max wait of the step.
func waitOptions(maxWait int) iggcon.PollOptions {
	return iggcon.WaitPollOptions(time.Duration(maxWait) * time.Second)
}

// disconnecter is a client that drops its connection without a sign-out.
type disconnecter interface{ Disconnect() error }

// disconnectable is client, or the client that it wraps, as a disconnecter.
func disconnectable(client iggcon.Client) (disconnecter, error) {
	for {
		if dropping, ok := client.(disconnecter); ok {
			return dropping, nil
		}
		wrapper, ok := client.(interface{ Unwrap() iggcon.Client })
		if !ok {
			return nil, fmt.Errorf("the client %T cannot disconnect", client)
		}
		client = wrapper.Unwrap()
	}
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
