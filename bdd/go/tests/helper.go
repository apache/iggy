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
	"fmt"
	"os"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/google/uuid"
)

func defaultServerAddress() string {
	if addr := os.Getenv("IGGY_TCP_ADDRESS"); addr != "" {
		return addr
	}
	return "127.0.0.1:8090"
}

func connectToServer(ctx context.Context, addr string) (iggcon.Client, error) {
	cli, err := client.NewIggyClient(
		client.WithTcp(
			tcp.WithServerAddress(addr),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating client: %w", err)
	}
	if err = cli.Connect(ctx); err != nil {
		return nil, fmt.Errorf("error connecting to server: %w", err)
	}
	if err = cli.Ping(ctx); err != nil {
		return nil, fmt.Errorf("error pinging server: %w", err)
	}
	return cli, nil
}

func loginAsRoot(ctx context.Context, cli iggcon.Client) error {
	if _, err := cli.LoginUser(ctx, "iggy", "iggy"); err != nil {
		return fmt.Errorf("error logging in: %w", err)
	}
	return nil
}

func createTestMessages(count uint32) ([]iggcon.IggyMessage, error) {
	messages := make([]iggcon.IggyMessage, 0, count)
	for i := range count {
		id := uuid.New()
		payload := fmt.Appendf(nil, "test message %d", i)
		message, err := iggcon.NewIggyMessage(payload, iggcon.WithID(id))
		if err != nil {
			return nil, fmt.Errorf("failed to create message: %w", err)
		}
		messages = append(messages, message)
	}
	return messages, nil
}

func sendTestMessages(
	ctx context.Context,
	cli iggcon.Client,
	streamID iggcon.Identifier,
	topicID iggcon.Identifier,
	partitionID uint32,
	count uint32,
) (*iggcon.IggyMessage, error) {
	messages, err := createTestMessages(count)
	if err != nil {
		return nil, err
	}
	partitioning := iggcon.PartitionId(partitionID)
	if err = cli.SendMessages(ctx, streamID, topicID, partitioning, messages); err != nil {
		return nil, fmt.Errorf("failed to send messages: %w", err)
	}
	last := messages[len(messages)-1]
	return &last, nil
}

func pollTestMessages(
	ctx context.Context,
	cli iggcon.Client,
	streamID iggcon.Identifier,
	topicID iggcon.Identifier,
	partitionID uint32,
	startOffset uint64,
	maxCount uint32,
) (*iggcon.PolledMessage, error) {
	consumer := iggcon.DefaultConsumer()
	polled, err := cli.PollMessages(
		ctx,
		streamID,
		topicID,
		consumer,
		iggcon.OffsetPollingStrategy(startOffset),
		maxCount,
		false,
		&partitionID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to poll messages: %w", err)
	}
	return polled, nil
}

func assertMessageCount(polled *iggcon.PolledMessage, expected uint32) error {
	actual := uint32(len(polled.Messages))
	if actual != expected {
		return fmt.Errorf("expected %d messages, got %d", expected, actual)
	}
	return nil
}
