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
	"errors"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func autoConsumer(t *testing.T, client *fakeClient, after iggcon.CommitAfter) *IggyConsumer {
	t.Helper()
	auto := iggcon.AutoCommit{Every: iggcon.CommitEvery{Kind: iggcon.CommitEveryMessage}, After: after}
	consumer, err := NewIggyConsumer(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(1),
		WithPollOptions(withCommit(waitOneSecond, iggcon.AutoCommitPolicy(auto))), WithRetryInterval(100*time.Millisecond))
	require.NoError(t, err)
	return consumer
}

func TestIggyConsumer_CountsAMessageWhenTheNextIsAskedForAndTheLastAtShutdown(t *testing.T) {
	client := newFakeClient()
	consumer := autoConsumer(t, client, iggcon.CommitAfterProcessing)
	go func() { client.nextRead(t).answer(section(1, 0, 1)) }()

	first, err := consumer.Next(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(0), first.Checkpoint.Offset())
	assert.Empty(t, client.recordedStores())
	_, err = consumer.Next(context.Background())
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(client.recordedStores()) == 1 }, testTimeout, 5*time.Millisecond)
	assert.Equal(t, []store{{1, 0}}, client.recordedStores())

	require.NoError(t, consumer.Shutdown(context.Background()))

	assert.Equal(t, []store{{1, 0}, {1, 1}}, client.recordedStores())
}

// A handler that fails leaves its message uncounted and ends the loop with its error.
func TestIggyConsumer_CountsAfterASuccessfulHandlerAndEndsOnAFailedOne(t *testing.T) {
	client := newFakeClient()
	consumer := autoConsumer(t, client, iggcon.CommitAfterProcessing)
	go func() { client.nextRead(t).answer(section(1, 0, 1, 2)) }()
	failure := errors.New("handler failed")
	var handled []uint64

	err := consumer.Consume(context.Background(), func(_ context.Context, message ReceivedMessage) error {
		handled = append(handled, message.Checkpoint.Offset())
		if message.Checkpoint.Offset() == 2 {
			return failure
		}
		return nil
	})

	require.ErrorIs(t, err, failure)
	assert.Equal(t, []uint64{0, 1, 2}, handled)
	require.NoError(t, consumer.Shutdown(context.Background()))
	assert.Equal(t, uint64(1), client.recordedStores()[len(client.recordedStores())-1].offset,
		"the failed message stays uncounted")
}

// Under OnRead the server stored the whole batch, so Shutdown moves the offset back to the last message handed over.
func TestIggyConsumer_MovesTheOffsetBackToTheLastMessageHandedOverAtShutdownUnderOnRead(t *testing.T) {
	client := newFakeClient()
	consumer, err := NewIggyConsumer(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(1),
		WithPollOptions(withCommit(waitOneSecond, iggcon.OnReadCommit())))
	require.NoError(t, err)
	go func() { client.nextRead(t).answer(section(1, 0, 1, 2, 3)) }()
	_, err = consumer.Next(context.Background())
	require.NoError(t, err)
	_, err = consumer.Next(context.Background())
	require.NoError(t, err)

	require.NoError(t, consumer.Shutdown(context.Background()))

	assert.Equal(t, []store{{1, 1}}, client.recordedStores())
}

func TestIggyConsumer_ShutdownWaitsForAnAutomaticStoreWithoutCancellingIt(t *testing.T) {
	client := newFakeClient()
	client.storeStarted = make(chan context.Context, 4)
	client.storeRelease = make(chan struct{})
	consumer := autoConsumer(t, client, iggcon.CommitAfterDelivery)
	client.nextRead(t).answer(section(1, 0))
	_, err := consumer.Next(context.Background())
	require.NoError(t, err)
	var storeContext context.Context
	select {
	case storeContext = <-client.storeStarted:
	case <-time.After(testTimeout):
		t.Fatal("the automatic store did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- consumer.Shutdown(context.Background()) }()
	<-consumer.reader.run.Done()
	assert.NoError(t, storeContext.Err(), "shutdown must let an admitted automatic store settle")
	close(client.storeRelease)
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(testTimeout):
		t.Fatal("shutdown did not finish after the store settled")
	}
}

// A seek of another partition leaves the batch that the consumer took from the reader meanwhile.
func TestIggyConsumer_HandsOverABatchTakenWhileAnotherPartitionIsSought(t *testing.T) {
	client := newFakeClient()
	consumer := manualConsumer(t, client, 1, 2)
	read := client.nextRead(t)
	received := receiveInBackground(consumer)
	require.Eventually(t, func() bool { return waiting(consumer.reader) == 1 }, testTimeout, time.Millisecond)

	require.NoError(t, consumer.Seek(context.Background(), 2, iggcon.OffsetPollingStrategy(0)))
	read.answer(section(1, 0))

	result := <-received
	require.NoError(t, result.err)
	assert.Equal(t, uint32(1), result.message.PartitionID)
	assert.Equal(t, uint64(0), result.message.Checkpoint.Offset())
}

// The first batch that the reader reads after a seek reaches the consumer that waited during the seek.
func TestIggyConsumer_HandsOverTheBatchReadAfterASeekOfItsPartition(t *testing.T) {
	client := newFakeClient()
	consumer := manualConsumer(t, client, 1)
	stale := client.nextRead(t)
	received := receiveInBackground(consumer)
	require.Eventually(t, func() bool { return waiting(consumer.reader) == 1 }, testTimeout, time.Millisecond)

	require.NoError(t, consumer.Seek(context.Background(), 1, iggcon.OffsetPollingStrategy(5)))
	sought := client.nextRead(t)
	require.Equal(t, uint64(5), sought.targets[0].Strategy.Value)
	sought.answer(section(1, 5))
	stale.answer(section(1, 0))

	result := <-received
	require.NoError(t, result.err)
	assert.Equal(t, uint64(5), result.message.Checkpoint.Offset())
}

// A handler that ends the context of Consume gets no further message of the batch.
func TestIggyConsumer_ConsumeEndsWhenItsContextEndsBetweenMessages(t *testing.T) {
	client := newFakeClient()
	consumer := manualConsumer(t, client, 1)
	client.nextRead(t).answer(section(1, 0, 1, 2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var handled []uint64

	err := consumer.Consume(ctx, func(_ context.Context, message ReceivedMessage) error {
		handled = append(handled, message.Checkpoint.Offset())
		cancel()
		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, []uint64{0}, handled)
}

func manualConsumer(t *testing.T, client *fakeClient, partitions ...uint32) *IggyConsumer {
	t.Helper()
	consumer, err := NewIggyConsumer(context.Background(), client, identifier(t, 1), identifier(t, 2),
		iggcon.NewSingleConsumer(identifier(t, 3)), WithPartitions(partitions...), WithPollOptions(waitOneSecond),
		WithRetryInterval(testRetryInterval))
	require.NoError(t, err)
	t.Cleanup(func() { _ = consumer.Shutdown(context.Background()) })
	return consumer
}

type receivedOutcome struct {
	message ReceivedMessage
	err     error
}

// receiveInBackground starts a Next of consumer whose outcome arrives on the returned channel.
func receiveInBackground(consumer *IggyConsumer) <-chan receivedOutcome {
	received := make(chan receivedOutcome, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()
		message, err := consumer.Next(ctx)
		received <- receivedOutcome{message, err}
	}()
	return received
}
