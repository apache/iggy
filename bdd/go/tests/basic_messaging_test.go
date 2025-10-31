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
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/apache/iggy/foreign/go/tcp"
	"github.com/cucumber/godog"
	"github.com/google/uuid"
	"os"
	"testing"
)

type basicMessagingCtxKey struct{}

type basicMessagingCtx struct {
	serverAddr          *string
	client              iggycli.Client
	lastSentMessage     *iggcon.IggyMessage
	lastPollMessages    *iggcon.PolledMessage
	lastStreamID        *uint32
	lastStreamName      *string
	lastTopicID         *uint32
	lastTopicName       *string
	lastTopicPartitions *uint32
}

func getBasicMessagingCtx(ctx context.Context) *basicMessagingCtx {
	return ctx.Value(basicMessagingCtxKey{}).(*basicMessagingCtx)
}

func givenRunningServer(ctx context.Context) error {
	c := getBasicMessagingCtx(ctx)
	addr := os.Getenv("IGGY_TCP_ADDRESS")
	if addr == "" {
		addr = "127.0.0.1:8090"
	}
	c.serverAddr = &addr
	return nil
}
func givenAuthenticationAsRoot(ctx context.Context) error {
	c := getBasicMessagingCtx(ctx)
	serverAddr := *c.serverAddr

	client, err := iggycli.NewIggyClient(
		iggycli.WithTcp(
			tcp.WithServerAddress(serverAddr),
		),
	)
	if err != nil {
		return fmt.Errorf("error creating client: %w", err)
	}

	if err = client.Ping(); err != nil {
		return fmt.Errorf("error pinging client: %w", err)
	}

	if _, err = client.LoginUser("iggy", "iggy"); err != nil {
		return fmt.Errorf("error logging in: %v", err)
	}

	c.client = client
	return nil
}

func whenSendMessages(
	ctx context.Context,
	messagesCount uint32,
	streamID uint32,
	topicID uint32,
	partitionID uint32) error {
	c := getBasicMessagingCtx(ctx)
	messages, err := createTestMessages(messagesCount)
	if err != nil {
		return fmt.Errorf("error creating test messages: %w", err)
	}

	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	topicIdentifier, _ := iggcon.NewIdentifier(topicID)
	partitioning := iggcon.PartitionId(partitionID)
	if err = c.client.SendMessages(streamIdentifier, topicIdentifier, partitioning, messages); err != nil {
		return fmt.Errorf("failed to sending messages: %w", err)
	}

	c.lastSentMessage = &messages[len(messages)-1]
	return nil
}

func createTestMessages(count uint32) ([]iggcon.IggyMessage, error) {
	messages := make([]iggcon.IggyMessage, 0, count)
	for i := 0; uint32(i) < count; i++ {
		id := uuid.New()
		payload := []byte(fmt.Sprintf("test message %d", i))
		message, err := iggcon.NewIggyMessage(payload, iggcon.WithID(id))
		if err != nil {
			return nil, fmt.Errorf("failed to create message: %w", err)
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func whenPollMessages(
	ctx context.Context,
	streamID uint32,
	topicID uint32,
	partitionID uint32,
	startOffset uint64) error {
	c := getBasicMessagingCtx(ctx)
	consumer := iggcon.DefaultConsumer()
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	topicIdentifier, _ := iggcon.NewIdentifier(topicID)
	uint32PartitionID := partitionID
	polledMessages, err := c.client.PollMessages(
		streamIdentifier,
		topicIdentifier,
		consumer,
		iggcon.OffsetPollingStrategy(startOffset),
		100,
		false,
		&uint32PartitionID,
	)
	if err != nil {
		return fmt.Errorf("failed to poll messages: %w", err)
	}
	c.lastPollMessages = polledMessages
	return nil
}

func thenMessageSentSuccessfully(_ context.Context) error {
	return nil
}

func thenShouldReceiveMessages(ctx context.Context, expectedCount uint32) error {
	polledMessages := getBasicMessagingCtx(ctx).lastPollMessages
	if uint32(len(polledMessages.Messages)) != expectedCount {
		return fmt.Errorf("expected %d messages, but there is %d", expectedCount, len(polledMessages.Messages))
	}
	return nil
}

func thenMessagesHaveSequentialOffsets(
	ctx context.Context,
	startOffset uint64,
	endOffset uint64) error {
	polledMessages := getBasicMessagingCtx(ctx).lastPollMessages
	for i, m := range polledMessages.Messages {
		expectedOffset := startOffset + uint64(i)
		if expectedOffset != m.Header.Offset {
			return fmt.Errorf("message at index %d should have offset %d", i, expectedOffset)
		}
	}
	lastMessage := polledMessages.Messages[len(polledMessages.Messages)-1]
	if lastMessage.Header.Offset != endOffset {
		return fmt.Errorf("last message should have offset %d", endOffset)
	}
	return nil
}

func thenMessagesHaveExpectedPayload(ctx context.Context) error {
	polledMessages := getBasicMessagingCtx(ctx).lastPollMessages
	for i, m := range polledMessages.Messages {
		expectedPayload := fmt.Sprintf("test message %d", i)
		if expectedPayload != string(m.Payload) {
			return fmt.Errorf("message at offset %d should have payload '%s'", i, expectedPayload)
		}
	}
	return nil
}

func thenLastPolledMessageMatchesSent(ctx context.Context) error {
	c := getBasicMessagingCtx(ctx)
	polledMessages := c.lastPollMessages
	sentMessage := c.lastSentMessage
	if len(polledMessages.Messages) == 0 {
		return errors.New("should have at least one polled message")
	}

	lastPolled := polledMessages.Messages[len(polledMessages.Messages)-1]

	if lastPolled.Header.Id != sentMessage.Header.Id {
		return fmt.Errorf("message IDs should match: sent %d, polled %d", sentMessage.Header.Id, lastPolled.Header.Id)
	}

	if string(lastPolled.Payload) != string(sentMessage.Payload) {
		return fmt.Errorf("message payload should match: sent %s, polled %s", sentMessage.Payload, lastPolled.Header.Id)
	}
	return nil
}

func givenNoStreams(ctx context.Context) error {
	client := getBasicMessagingCtx(ctx).client
	streams, err := client.GetStreams()
	if err != nil {
		return fmt.Errorf("failed to get streams: %w", err)
	}

	if len(streams) != 0 {
		return errors.New("system should have no stream initially")
	}

	return err
}

func whenCreateStream(ctx context.Context, streamName string) error {
	c := getBasicMessagingCtx(ctx)
    stream, err := c.client.CreateStream(streamName)
	if err != nil {
		return fmt.Errorf("failed to create stream: %w", err)
	}
	c.lastStreamID = &stream.Id
	c.lastStreamName = &stream.Name
	return nil
}

func thenStreamCreatedSuccessfully(ctx context.Context) error {
	if getBasicMessagingCtx(ctx).lastStreamID == nil {
		return errors.New("stream should have been created")
	}
	return nil
}

func thenStreamHasName(
    ctx context.Context,
    expectedName string) error {
	c := getBasicMessagingCtx(ctx)
	streamName := *c.lastStreamName
	if streamName != expectedName {
		return fmt.Errorf("expected stream name %s, got %s", expectedName, streamName)
	}
	return nil
}

func whenCreateTopic(
    ctx context.Context,
    topicName string,
    streamID uint32,
    partitionsCount uint32) error {
	c := getBasicMessagingCtx(ctx)
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
    topic, err := c.client.CreateTopic(
		streamIdentifier,
		topicName,
		partitionsCount,
		iggcon.CompressionAlgorithmNone,
		iggcon.IggyExpiryNeverExpire,
        0,
        nil,
	)
	if err != nil {
		return fmt.Errorf("failed to create topic: %w", err)
	}

	c.lastTopicID = &topic.Id
	c.lastTopicName = &topic.Name
	c.lastTopicPartitions = &topic.PartitionsCount

	return nil
}

func thenTopicCreatedSuccessfully(ctx context.Context) error {
	if getBasicMessagingCtx(ctx).lastTopicID == nil {
		return errors.New("topic should have been created")
	}
	return nil
}
func thenTopicHasName(
    ctx context.Context,
    expectedName string) error {
	c := getBasicMessagingCtx(ctx)
	topicName := *c.lastTopicName
	if topicName != expectedName {
		return fmt.Errorf("expected topic name %s, got %s", expectedName, topicName)
	}
	return nil
}

func thenTopicsHasPartitions(ctx context.Context, expectedTopicPartitions uint32) error {
	topicPartitions := *getBasicMessagingCtx(ctx).lastTopicPartitions
	if topicPartitions != expectedTopicPartitions {
		return errors.New("topic should have expected number of partitions")
	}
	return nil
}

func initScenarios(sc *godog.ScenarioContext) {
	sc.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		return context.WithValue(context.Background(), basicMessagingCtxKey{}, &basicMessagingCtx{}), nil
	})
	sc.Step(`I have a running Iggy server`, givenRunningServer)
	sc.Step(`I am authenticated as the root user`, givenAuthenticationAsRoot)
	sc.Step(`^I send (\d+) messages to stream (\d+), topic (\d+), partition (\d+)$`, whenSendMessages)
	sc.Step(`^I poll messages from stream (\d+), topic (\d+), partition (\d+) starting from offset (\d+)$`, whenPollMessages)
	sc.Step(`all messages should be sent successfully`, thenMessageSentSuccessfully)
	sc.Step(`^I should receive (\d+) messages$`, thenShouldReceiveMessages)
	sc.Step(`^the messages should have sequential offsets from (\d+) to (\d+)$`, thenMessagesHaveSequentialOffsets)
	sc.Step(`each message should have the expected payload content`, thenMessagesHaveExpectedPayload)
	sc.Step(`the last polled message should match the last sent message`, thenLastPolledMessageMatchesSent)
    sc.Step(`^the stream should have name "([^"]*)"$`, thenStreamHasName)
	sc.Step(`the stream should be created successfully`, thenStreamCreatedSuccessfully)
    sc.Step(`^I create a stream with name "([^"]*)"$`, whenCreateStream)
	sc.Step(`I have no streams in the system`, givenNoStreams)
    sc.Step(`^I create a topic with name "([^"]*)" in stream (\d+) with (\d+) partitions$`, whenCreateTopic)
	sc.Step(`the topic should be created successfully`, thenTopicCreatedSuccessfully)
    sc.Step(`^the topic should have name "([^"]*)"$`, thenTopicHasName)
	sc.Step(`^the topic should have (\d+) partitions$`, thenTopicsHasPartitions)
}

func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: initScenarios,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"../../scenarios/basic_messaging.feature"},
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("failing feature tests")
	}
}
