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

type (
	serverAddrKeyType          struct{}
	clientKeyType              struct{}
	lastSentMessageKeyType     struct{}
	lastPollMessagesKeyType    struct{}
	lastStreamIDKeyType        struct{}
	lastStreamNameKeyType      struct{}
	lastTopicIDKeyType         struct{}
	lastTopicNameKeyType       struct{}
	lastTopicPartitionsKeyType struct{}
)

var (
	ServerAddrKey          = serverAddrKeyType{}
	ClientKey              = clientKeyType{}
	LastSentMessageKey     = lastSentMessageKeyType{}
	LastPollMessagesKey    = lastPollMessagesKeyType{}
	LastStreamIDKey        = lastStreamIDKeyType{}
	LastStreamNameKey      = lastStreamNameKeyType{}
	LastTopicIDKey         = lastTopicIDKeyType{}
	LastTopicNameKey       = lastTopicNameKeyType{}
	LastTopicPartitionsKey = lastTopicPartitionsKeyType{}
)

func givenRunningServer(ctx context.Context) (context.Context, error) {
	addr := os.Getenv("IGGY_TCP_ADDRESS")
	if addr == "" {
		addr = "127.0.0.1:8090"
	}
	return context.WithValue(ctx, ServerAddrKey, addr), nil
}
func givenAuthenticationAsRoot(ctx context.Context) (context.Context, error) {
	serverAddr := ctx.Value(ServerAddrKey).(string)

	client, err := iggycli.NewIggyClient(
		iggycli.WithTcp(
			tcp.WithServerAddress(serverAddr),
		),
	)
	if err != nil {
		return ctx, fmt.Errorf("error creating client: %w", err)
	}

	if err = client.Ping(); err != nil {
		return ctx, fmt.Errorf("error pinging client: %w", err)
	}

	if _, err = client.LoginUser("iggy", "iggy"); err != nil {
		return ctx, fmt.Errorf("error logging in: %v", err)
	}

	return context.WithValue(ctx, ClientKey, client), nil
}

func whenSendMessages(
	ctx context.Context,
	messagesCount uint32,
	streamID uint32,
	topicID uint32,
	partitionID uint32) (context.Context, error) {
	clientVal := ctx.Value(ClientKey)
	if clientVal == nil {
		return ctx, errors.New("client should be available")
	}
	client := clientVal.(iggycli.Client)
	messages, err := createTestMessages(messagesCount)
	if err != nil {
		return ctx, fmt.Errorf("error creating test messages: %w", err)
	}

	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	topicIdentifier, _ := iggcon.NewIdentifier(topicID)
	partitioning := iggcon.PartitionId(partitionID)
	if err = client.SendMessages(streamIdentifier, topicIdentifier, partitioning, messages); err != nil {
		return ctx, fmt.Errorf("failed to sending messages: %w", err)
	}

	return context.WithValue(ctx, LastSentMessageKey, messages[len(messages)-1]), nil
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
	startOffset uint64) (context.Context, error) {
	client := ctx.Value(ClientKey).(iggycli.Client)
	consumer := iggcon.DefaultConsumer()
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	topicIdentifier, _ := iggcon.NewIdentifier(topicID)
	uint32PartitionID := partitionID
	polledMessages, err := client.PollMessages(
		streamIdentifier,
		topicIdentifier,
		consumer,
		iggcon.OffsetPollingStrategy(startOffset),
		100,
		false,
		&uint32PartitionID,
	)
	if err != nil {
		return ctx, fmt.Errorf("failed to poll messages: %w", err)
	}
	return context.WithValue(ctx, LastPollMessagesKey, polledMessages), nil
}

func thenMessageSentSuccessfully(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func thenShouldReceiveMessages(ctx context.Context, expectedCount uint32) (context.Context, error) {
	polledMessages := ctx.Value(LastPollMessagesKey).(*iggcon.PolledMessage)
	if uint32(len(polledMessages.Messages)) != expectedCount {
		return ctx, fmt.Errorf("expected %d messages, but there is %d", expectedCount, len(polledMessages.Messages))
	}
	return ctx, nil
}

func thenMessagesHaveSequentialOffsets(
	ctx context.Context,
	startOffset uint64,
	endOffset uint64) (context.Context, error) {
	polledMessages := ctx.Value(LastPollMessagesKey).(*iggcon.PolledMessage)
	for i, m := range polledMessages.Messages {
		expectedOffset := startOffset + uint64(i)
		if expectedOffset != m.Header.Offset {
			return ctx, fmt.Errorf("message at index %d should have offset %d", i, expectedOffset)
		}
	}
	lastMessage := polledMessages.Messages[len(polledMessages.Messages)-1]
	if lastMessage.Header.Offset != endOffset {
		return ctx, fmt.Errorf("last message should have offset %d", endOffset)
	}
	return ctx, nil
}

func thenMessagesHaveExpectedPayload(ctx context.Context) (context.Context, error) {
	polledMessages := ctx.Value(LastPollMessagesKey).(*iggcon.PolledMessage)
	for i, m := range polledMessages.Messages {
		expectedPayload := fmt.Sprintf("test message %d", i)
		if expectedPayload != string(m.Payload) {
			return ctx, fmt.Errorf("message at offset %d should have payload '%s'", i, expectedPayload)
		}
	}
	return ctx, nil
}

func thenLastPolledMessageMatchesSent(ctx context.Context) (context.Context, error) {
	polledMessages := ctx.Value(LastPollMessagesKey).(*iggcon.PolledMessage)
	sentMessage := ctx.Value(LastSentMessageKey).(iggcon.IggyMessage)
	if len(polledMessages.Messages) == 0 {
		return ctx, errors.New("should have at least one polled message")
	}

	lastPolled := polledMessages.Messages[len(polledMessages.Messages)-1]

	if lastPolled.Header.Id != sentMessage.Header.Id {
		return ctx, fmt.Errorf("message IDs should match: sent %d, polled %d", sentMessage.Header.Id, lastPolled.Header.Id)
	}

	if string(lastPolled.Payload) != string(sentMessage.Payload) {
		return ctx, fmt.Errorf("message payload should match: sent %s, polled %s", sentMessage.Payload, lastPolled.Header.Id)
	}
	return ctx, nil
}

func givenNoStreams(ctx context.Context) (context.Context, error) {
	client := ctx.Value(ClientKey).(iggycli.Client)
	streams, err := client.GetStreams()
	if err != nil {
		return ctx, fmt.Errorf("failed to get streams: %w", err)
	}

	if len(streams) != 0 {
		return ctx, errors.New("system should have no stream initially")
	}

	return ctx, err
}

func whenCreateStream(ctx context.Context, streamID uint32, streamName string) (context.Context, error) {
	client := ctx.Value(ClientKey).(iggycli.Client)
	stream, err := client.CreateStream(streamName, &streamID)
	if err != nil {
		return ctx, fmt.Errorf("failed to create stream: %w", err)
	}
	ctx = context.WithValue(ctx, LastStreamIDKey, stream.Id)
	ctx = context.WithValue(ctx, LastStreamNameKey, stream.Name)
	return ctx, nil
}

func thenStreamCreatedSuccessfully(ctx context.Context) (context.Context, error) {
	if ctx.Value(LastStreamIDKey) == nil {
		return ctx, errors.New("stream should have been created")
	}
	return ctx, nil
}

func thenStreamHasIDAndName(
	ctx context.Context,
	expectedID uint32,
	expectedName string) (context.Context, error) {
	streamID := ctx.Value(LastStreamIDKey).(uint32)
	streamName := ctx.Value(LastStreamNameKey).(string)
	if streamID != expectedID {
		return ctx, fmt.Errorf("expected stream ID %d, got %d", expectedID, streamID)
	}
	if streamName != expectedName {
		return ctx, fmt.Errorf("expected stream name %s, got %s", expectedName, streamName)
	}
	return ctx, nil
}

func whenCreateTopic(
	ctx context.Context,
	topicID uint32,
	topicName string,
	streamID uint32,
	partitionsCount uint32) (context.Context, error) {
	client := ctx.Value(ClientKey).(iggycli.Client)
	streamIdentifier, _ := iggcon.NewIdentifier(streamID)
	uint32TopicID := topicID
	topic, err := client.CreateTopic(
		streamIdentifier,
		topicName,
		partitionsCount,
		iggcon.CompressionAlgorithmNone,
		iggcon.IggyExpiryNeverExpire,
		0,
		nil,
		&uint32TopicID,
	)
	if err != nil {
		return ctx, fmt.Errorf("failed to create topic: %w", err)
	}

	ctx = context.WithValue(ctx, LastTopicIDKey, topicID)
	ctx = context.WithValue(ctx, LastTopicNameKey, topic.Name)
	ctx = context.WithValue(ctx, LastTopicPartitionsKey, topic.PartitionsCount)

	return ctx, nil
}

func thenTopicCreatedSuccessfully(ctx context.Context) (context.Context, error) {
	if ctx.Value(LastTopicIDKey) == nil {
		return ctx, errors.New("topic should have been created")
	}
	return ctx, nil
}
func thenTopicHasIDAndName(
	ctx context.Context,
	expectedID uint32,
	expectedName string) (context.Context, error) {
	topicID := ctx.Value(LastTopicIDKey).(uint32)
	topicName := ctx.Value(LastTopicNameKey).(string)
	if topicID != expectedID {
		return ctx, fmt.Errorf("expected topic ID %d, got %d", expectedID, topicID)
	}
	if topicName != expectedName {
		return ctx, fmt.Errorf("expected topic name %s, got %s", expectedName, topicName)
	}
	return ctx, nil
}

func thenTopicsHasPartitions(ctx context.Context, expectedTopicPartitions uint32) (context.Context, error) {
	topicPartitions := ctx.Value(LastTopicPartitionsKey).(uint32)
	if topicPartitions != expectedTopicPartitions {
		return ctx, errors.New("topic should have expected number of partitions")
	}
	return ctx, nil
}

func initScenarios(sc *godog.ScenarioContext) {
	sc.Step(`I have a running Iggy server`, givenRunningServer)
	sc.Step(`I am authenticated as the root user`, givenAuthenticationAsRoot)
	sc.Step(`^I send (\d+) messages to stream (\d+), topic (\d+), partition (\d+)$`, whenSendMessages)
	sc.Step(`^I poll messages from stream (\d+), topic (\d+), partition (\d+) starting from offset (\d+)$`, whenPollMessages)
	sc.Step(`all messages should be sent successfully`, thenMessageSentSuccessfully)
	sc.Step(`^I should receive (\d+) messages$`, thenShouldReceiveMessages)
	sc.Step(`^the messages should have sequential offsets from (\d+) to (\d+)$`, thenMessagesHaveSequentialOffsets)
	sc.Step(`each message should have the expected payload content`, thenMessagesHaveExpectedPayload)
	sc.Step(`the last polled message should match the last sent message`, thenLastPolledMessageMatchesSent)
	sc.Step(`^the stream should have ID (\d+) and name (.+)$`, thenStreamHasIDAndName)
	sc.Step(`the stream should be created successfully`, thenStreamCreatedSuccessfully)
	sc.Step(`^I create a stream with ID (\d+) and name (.+)$`, whenCreateStream)
	sc.Step(`I have no streams in the system`, givenNoStreams)
	sc.Step(`^I create a topic with ID (\d+) and name (.+) in stream (\d+) with (\d+) partitions$`, whenCreateTopic)
	sc.Step(`the topic should be created successfully`, thenTopicCreatedSuccessfully)
	sc.Step(`^the topic should have ID (\d+) and name (.+)$`, thenTopicHasIDAndName)
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
