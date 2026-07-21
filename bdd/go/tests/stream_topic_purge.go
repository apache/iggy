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
	"math"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/cucumber/godog"
)

type purgeCtxKey struct{}

type purgeCtx struct {
	client           iggcon.Client
	streamIDs        map[string]uint32
	topicIDs         map[string]uint32
	lastErr          error
	lastPollMessages *iggcon.PolledMessage
}

func getPurgeCtx(ctx context.Context) *purgeCtx {
	return ctx.Value(purgeCtxKey{}).(*purgeCtx)
}

type purgeSteps struct{}

func (s purgeSteps) givenRunningServer(ctx context.Context) error {
	c := getPurgeCtx(ctx)
	cli, err := connectToServer(ctx, defaultServerAddress())
	if err != nil {
		return err
	}
	c.client = cli
	return nil
}

func (s purgeSteps) givenAuthenticatedAsRoot(ctx context.Context) error {
	return loginAsRoot(ctx, getPurgeCtx(ctx).client)
}

func (s purgeSteps) givenHaveStream(ctx context.Context, name string) error {
	c := getPurgeCtx(ctx)
	stream, err := c.client.CreateStream(ctx, name)
	if err != nil {
		return fmt.Errorf("error creating stream: %w", err)
	}
	c.streamIDs[name] = stream.Id
	return nil
}

func (s purgeSteps) givenHaveTopic(ctx context.Context, topicName, streamName string, partitionsCount uint32) error {
	c := getPurgeCtx(ctx)
	streamID, ok := c.streamIDs[streamName]
	if !ok {
		return fmt.Errorf("stream %q not found", streamName)
	}
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	topic, err := c.client.CreateTopic(
		ctx,
		streamIdentifier,
		topicName,
		partitionsCount,
		iggcon.CompressionAlgorithmNone,
		iggcon.IggyExpiryNeverExpire,
		0,
		nil,
	)
	if err != nil {
		return fmt.Errorf("error creating topic: %w", err)
	}
	c.topicIDs[topicName] = topic.Id
	return nil
}

func (s purgeSteps) givenHaveSentMessages(ctx context.Context, messagesCount uint32, streamName, topicName string, partitionID uint32) error {
	c := getPurgeCtx(ctx)
	streamID, ok := c.streamIDs[streamName]
	if !ok {
		return fmt.Errorf("stream %q not found", streamName)
	}
	topicID, ok := c.topicIDs[topicName]
	if !ok {
		return fmt.Errorf("topic %q not found", topicName)
	}
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	topicIdentifier, _ := iggcon.NewIdentifier(topicID)
	if _, err := sendTestMessages(ctx, c.client, streamIdentifier, topicIdentifier, partitionID, messagesCount); err != nil {
		return err
	}
	return nil
}

func (s purgeSteps) whenPurgeStream(ctx context.Context, streamName string) error {
	c := getPurgeCtx(ctx)
	streamID, ok := c.streamIDs[streamName]
	if !ok {
		return fmt.Errorf("stream %q not found", streamName)
	}
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	if err := c.client.PurgeStream(ctx, streamIdentifier); err != nil {
		return fmt.Errorf("error purging stream: %w", err)
	}
	return nil
}

func (s purgeSteps) whenPurgeTopic(ctx context.Context, topicName, streamName string) error {
	c := getPurgeCtx(ctx)
	streamID, ok := c.streamIDs[streamName]
	if !ok {
		return fmt.Errorf("stream %q not found", streamName)
	}
	topicID, ok := c.topicIDs[topicName]
	if !ok {
		return fmt.Errorf("topic %q not found", topicName)
	}
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	topicIdentifier, _ := iggcon.NewIdentifier(topicID)
	if err := c.client.PurgeTopic(ctx, streamIdentifier, topicIdentifier); err != nil {
		return fmt.Errorf("error purging topic: %w", err)
	}
	return nil
}

func (s purgeSteps) whenTryPurgeNonExistingStream(ctx context.Context) error {
	c := getPurgeCtx(ctx)
	streamIdentifier, _ := iggcon.NewIdentifier[uint32](math.MaxUint32)
	c.lastErr = c.client.PurgeStream(ctx, streamIdentifier)
	return nil
}

func (s purgeSteps) whenTryPurgeNonExistingTopic(ctx context.Context, streamName string) error {
	c := getPurgeCtx(ctx)
	streamID, ok := c.streamIDs[streamName]
	if !ok {
		return fmt.Errorf("stream %q not found", streamName)
	}
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	topicIdentifier, _ := iggcon.NewIdentifier[uint32](math.MaxUint32)
	c.lastErr = c.client.PurgeTopic(ctx, streamIdentifier, topicIdentifier)
	return nil
}

func (s purgeSteps) whenPollMessages(ctx context.Context, streamName, topicName string, partitionID uint32, startOffset uint64) error {
	c := getPurgeCtx(ctx)
	streamID, ok := c.streamIDs[streamName]
	if !ok {
		return fmt.Errorf("stream %q not found", streamName)
	}
	topicID, ok := c.topicIDs[topicName]
	if !ok {
		return fmt.Errorf("topic %q not found", topicName)
	}
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	topicIdentifier, _ := iggcon.NewIdentifier(topicID)
	polled, err := pollTestMessages(ctx, c.client, streamIdentifier, topicIdentifier, partitionID, startOffset, 100)
	if err != nil {
		return err
	}
	c.lastPollMessages = polled
	return nil
}

func (s purgeSteps) thenShouldReceiveMessages(ctx context.Context, expectedCount uint32) error {
	return assertMessageCount(getPurgeCtx(ctx).lastPollMessages, expectedCount)
}

func (s purgeSteps) thenPurgeFailsWithStreamNotFound(ctx context.Context) error {
	c := getPurgeCtx(ctx)
	if c.lastErr == nil {
		return errors.New("expected an error when purging non-existing stream, got nil")
	}
	if !errors.Is(c.lastErr, ierror.ErrStreamIdNotFound) {
		return fmt.Errorf("expected ErrStreamIdNotFound, got: %v", c.lastErr)
	}
	return nil
}

func (s purgeSteps) thenPurgeTopicFailsWithTopicNotFound(ctx context.Context) error {
	c := getPurgeCtx(ctx)
	if c.lastErr == nil {
		return errors.New("expected an error when purging non-existing topic, got nil")
	}
	if !errors.Is(c.lastErr, ierror.ErrTopicIdNotFound) {
		return fmt.Errorf("expected ErrTopicIdNotFound, got: %v", c.lastErr)
	}
	return nil
}

func initPurgeScenario(sc *godog.ScenarioContext) {
	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		return context.WithValue(context.Background(), purgeCtxKey{}, &purgeCtx{
			streamIDs: make(map[string]uint32),
			topicIDs:  make(map[string]uint32),
		}), nil
	})
	s := &purgeSteps{}
	sc.Step(`^I have a running Iggy server$`, s.givenRunningServer)
	sc.Step(`^I am authenticated as the root user$`, s.givenAuthenticatedAsRoot)
	sc.Step(`^I have a stream with name "([^"]*)"$`, s.givenHaveStream)
	sc.Step(`^I have a topic with name "([^"]*)" in stream "([^"]*)" with (\d+) partition[s]?$`, s.givenHaveTopic)
	sc.Step(`^I have sent (\d+) messages to stream "([^"]*)", topic "([^"]*)", partition (\d+)$`, s.givenHaveSentMessages)
	sc.Step(`^I purge the stream "([^"]*)"$`, s.whenPurgeStream)
	sc.Step(`^I purge topic "([^"]*)" in stream "([^"]*)"$`, s.whenPurgeTopic)
	sc.Step(`^I try to purge a non-existing stream$`, s.whenTryPurgeNonExistingStream)
	sc.Step(`^I try to purge a non-existing topic in stream "([^"]*)"$`, s.whenTryPurgeNonExistingTopic)
	sc.Step(`^I poll messages from stream "([^"]*)", topic "([^"]*)", partition (\d+) starting from offset (\d+)$`, s.whenPollMessages)
	sc.Step(`^I should receive (\d+) messages$`, s.thenShouldReceiveMessages)
	sc.Step(`^the purge operation should fail with a stream not found error$`, s.thenPurgeFailsWithStreamNotFound)
	sc.Step(`^the purge topic operation should fail with a topic not found error$`, s.thenPurgeTopicFailsWithTopicNotFound)
	sc.After(func(ctx context.Context, _ *godog.Scenario, scErr error) (context.Context, error) {
		c := getPurgeCtx(ctx)
		if c.client != nil {
			if err := c.client.Close(); err != nil {
				scErr = errors.Join(scErr, fmt.Errorf("error closing client: %w", err))
			}
		}
		return ctx, scErr
	})
}
