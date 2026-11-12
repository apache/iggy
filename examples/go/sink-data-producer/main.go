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

// Package main fills an Iggy topic with JSON records so a sink connector has
// something to read.
//
// It creates the stream and topic when they are missing, sends 100 batches of
// 100 to 499 records each, logs the total and exits. Running it again against
// the same server reuses the stream and topic. The record fields match the
// Rust and Python sink data producers.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	"github.com/apache/iggy/examples/go/common"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	defaultAddress = "127.0.0.1:8090"
	defaultStream  = "qw"
	defaultTopic   = "records"
)

const (
	batchCount         = 100
	minRecordsPerBatch = 100
	maxRecordsPerBatch = 499
)

const alphanumeric = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var (
	sources = []string{"browser", "mobile", "desktop", "email", "network", "other"}
	states  = []string{"active", "inactive", "blocked", "deleted", "unknown"}
	domains = []string{"gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com"}
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("sink data producer failed: %v", err)
	}
}

func run() error {
	ctx := context.Background()
	address := envOrDefault("IGGY_ADDRESS", defaultAddress)
	username := envOrDefault("IGGY_USERNAME", common.DefaultRootUsername)
	password := envOrDefault("IGGY_PASSWORD", common.DefaultRootPassword)
	stream := envOrDefault("IGGY_STREAM", defaultStream)
	topic := envOrDefault("IGGY_TOPIC", defaultTopic)

	cli, err := common.Dial(ctx, address, []tcp.Option{tcp.WithServerAddress(address)}, username, password)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer common.Close(cli)

	streamID, err := iggcon.NewIdentifier(stream)
	if err != nil {
		return fmt.Errorf("build identifier for stream %q: %w", stream, err)
	}
	topicID, err := iggcon.NewIdentifier(topic)
	if err != nil {
		return fmt.Errorf("build identifier for topic %q: %w", topic, err)
	}

	if err := ensureStream(ctx, cli, streamID, stream); err != nil {
		return err
	}
	if err := ensureTopic(ctx, cli, streamID, topicID, stream, topic); err != nil {
		return err
	}

	// None() is the SDK's balanced partitioning, the same as the Rust and
	// Python producers use.
	partitioning := iggcon.None()
	totalRecords := 0

	for batch := 1; batch <= batchCount; batch++ {
		recordsCount := minRecordsPerBatch + rand.IntN(maxRecordsPerBatch-minRecordsPerBatch+1)

		messages := make([]iggcon.IggyMessage, 0, recordsCount)
		for range recordsCount {
			payload, err := json.Marshal(randomRecord())
			if err != nil {
				return fmt.Errorf("encode record for batch %d: %w", batch, err)
			}
			message, err := iggcon.NewIggyMessage(payload)
			if err != nil {
				return fmt.Errorf("build message for batch %d: %w", batch, err)
			}
			messages = append(messages, message)
		}

		if _, err := cli.SendMessages(ctx, streamID, topicID, partitioning, messages); err != nil {
			return fmt.Errorf("send batch %d to topic %q in stream %q: %w", batch, topic, stream, err)
		}
		totalRecords += recordsCount
		log.Printf("Sent %d messages.", recordsCount)
	}

	log.Printf("Produced %d messages.", totalRecords)
	return nil
}

// Record is the JSON payload of every message.
type Record struct {
	UserID    string    `json:"user_id"`
	UserType  int       `json:"user_type"`
	Email     string    `json:"email"`
	Source    string    `json:"source"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	Message   string    `json:"message"`
}

func ensureStream(ctx context.Context, cli iggcon.Client, streamID iggcon.Identifier, stream string) error {
	_, err := cli.GetStream(ctx, streamID)
	switch {
	case err == nil:
		log.Printf("Stream %q already exists, reusing it.", stream)
		return nil
	case !errors.Is(err, ierror.ErrStreamIdNotFound):
		return fmt.Errorf("get stream %q: %w", stream, err)
	}

	_, err = cli.CreateStream(ctx, stream, nil)
	switch {
	case err == nil:
		log.Printf("Stream %q was created.", stream)
		return nil
	case errors.Is(err, ierror.ErrStreamNameAlreadyExists):
		log.Printf("Stream %q already exists, reusing it.", stream)
		return nil
	default:
		return fmt.Errorf("create stream %q: %w", stream, err)
	}
}

func ensureTopic(ctx context.Context, cli iggcon.Client, streamID, topicID iggcon.Identifier, stream, topic string) error {
	_, err := cli.GetTopic(ctx, streamID, topicID)
	switch {
	case err == nil:
		log.Printf("Topic %q already exists in stream %q, reusing it.", topic, stream)
		return nil
	case !errors.Is(err, ierror.ErrTopicIdNotFound):
		return fmt.Errorf("get topic %q in stream %q: %w", topic, stream, err)
	}

	_, err = cli.CreateTopic(ctx, streamID, topic, nil)
	switch {
	case err == nil:
		log.Printf("Topic %q was created in stream %q.", topic, stream)
		return nil
	case errors.Is(err, ierror.ErrTopicNameAlreadyExists):
		log.Printf("Topic %q already exists in stream %q, reusing it.", topic, stream)
		return nil
	default:
		return fmt.Errorf("create topic %q in stream %q: %w", topic, stream, err)
	}
}

func randomRecord() Record {
	return Record{
		UserID:    fmt.Sprintf("user_%d", 1+rand.IntN(99)),
		UserType:  1 + rand.IntN(4),
		Email:     randomAlphanumeric(3+rand.IntN(17)) + "@" + randomChoice(domains),
		Source:    randomChoice(sources),
		State:     randomChoice(states),
		CreatedAt: time.Now().UTC().AddDate(0, 0, -rand.IntN(1000)),
		Message:   randomAlphanumeric(10 + rand.IntN(90)),
	}
}

func randomChoice(options []string) string {
	return options[rand.IntN(len(options))]
}

func randomAlphanumeric(length int) string {
	text := make([]byte, length)
	for index := range text {
		text[index] = alphanumeric[rand.IntN(len(alphanumeric))]
	}
	return string(text)
}

// envOrDefault treats an empty value like an unset one, so IGGY_STREAM="" falls
// back to the default instead of producing an invalid empty identifier.
func envOrDefault(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}
