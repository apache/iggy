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

package benchmarks

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/apache/iggy/foreign/go/tcp"
	"github.com/google/uuid"
)

const (
	messagesCount    = 1000
	messagesBatch    = 1000
	messageSize      = 1000
	producerCount    = 10
	startingStreamId = 100
	topicId          = 1
)

func BenchmarkSendMessage(b *testing.B) {
	rand.New(rand.NewSource(42)) // Seed the random number generator for consistent results
	clients := make([]iggycli.Client, producerCount)

	for i := 0; i < producerCount; i++ {
		cli, err := iggycli.NewIggyClient(
			iggycli.WithTcp(
				tcp.WithServerAddress("127.0.0.1:8090"),
			),
		)
		if err != nil {
			panic("COULD NOT CREATE MESSAGE STREAM")
		}
		_, err = cli.LoginUser("iggy", "iggy")
		if err != nil {
			panic("COULD NOT LOG IN")
		}
		clients[i] = cli
	}

	for index, value := range clients {
		err := ensureInfrastructureIsInitialized(value, startingStreamId+index)
		if err != nil {
			panic("COULD NOT INITIALIZE INFRASTRUCTURE")
		}
	}

	resultChannel := make(chan struct {
		avgLatency    float64
		avgThroughput float64
	}, producerCount)

	wg := sync.WaitGroup{}
	for i := 0; i < producerCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			avgLatency, avgThroughput := SendMessage(clients[i], i, messagesCount, messagesBatch, messageSize)

			resultChannel <- struct {
				avgLatency    float64
				avgThroughput float64
			}{avgLatency, avgThroughput}
		}(i)
	}

	wg.Wait()
	close(resultChannel)

	aggregateThroughput := 0.0
	for result := range resultChannel {
		aggregateThroughput += result.avgThroughput
	}
	// Print the final results
	fmt.Printf("Summarized Average Throughput: %.2f MB/s\n", aggregateThroughput)

	for index, value := range clients {
		err := cleanupInfrastructure(value, startingStreamId+index)
		if err != nil {
			panic("COULD NOT CLEANUP INFRASTRUCTURE")
		}
	}
}

func ensureInfrastructureIsInitialized(cli iggycli.Client, streamId int) error {
	if _, streamErr := cli.GetStream(iggcon.NewIdentifier(streamId)); streamErr != nil {
		uint32StreamId := uint32(streamId)
		_, streamErr = cli.CreateStream("benchmark"+fmt.Sprint(streamId), &uint32StreamId)
		if streamErr != nil {
			panic(streamErr)
		}
	}
	if _, topicErr := cli.GetTopic(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(1)); topicErr != nil {
		_, topicErr = cli.CreateTopic(
			iggcon.NewIdentifier(streamId),
			"benchmark",
			1,
			1,
			0,
			1,
			nil,
			nil,
		)

		if topicErr != nil {
			panic(topicErr)
		}
	}
	return nil
}

func cleanupInfrastructure(cli iggycli.Client, streamId int) error {
	return cli.DeleteStream(iggcon.NewIdentifier(streamId))
}

// CreateMessages creates messages with random payloads.
func CreateMessages(messagesCount, messageSize int) []iggcon.IggyMessage {
	messages := make([]iggcon.IggyMessage, messagesCount)
	for i := 0; i < messagesCount; i++ {
		payload := make([]byte, messageSize)
		for j := 0; j < messageSize; j++ {
			payload[j] = byte(rand.Intn(26) + 97)
		}
		id, _ := uuid.NewUUID()

		var err error
		messages[i], err = iggcon.NewIggyMessage(payload, iggcon.WithID(id))
		if err != nil {
			panic(err)
		}
	}
	return messages
}

// SendMessage performs the message sending operation.
func SendMessage(cli iggycli.Client, producerNumber, messagesCount, messagesBatch, messageSize int) (avgLatency float64, avgThroughput float64) {
	totalMessages := messagesBatch * messagesCount
	totalMessagesBytes := int64(totalMessages * messageSize)
	fmt.Printf("Executing Send Messages command for producer %d, messages count %d, with size %d bytes\n", producerNumber, totalMessages, totalMessagesBytes)

	streamId := iggcon.NewIdentifier(startingStreamId + producerNumber)
	messages := CreateMessages(messagesCount, messageSize)
	latencies := make([]time.Duration, 0)

	for i := 0; i < messagesBatch; i++ {
		startTime := time.Now()
		_ = cli.SendMessages(
			streamId,
			iggcon.NewIdentifier(topicId),
			iggcon.PartitionId(1),
			messages,
		)
		elapsedTime := time.Since(startTime)
		latencies = append(latencies, elapsedTime)
	}

	totalLatencies := time.Duration(0)
	for _, latency := range latencies {
		totalLatencies += latency
	}
	avgLatency = float64(totalLatencies.Nanoseconds()) / float64(time.Millisecond) / float64(len(latencies))
	duration := totalLatencies.Seconds()
	avgThroughput = float64(totalMessagesBytes) / duration / 1024 / 1024
	fmt.Printf("Total message bytes: %d, average latency: %.2f ms.\n", totalMessagesBytes, avgLatency)
	fmt.Printf("Producer number: %d send Messages: %d in %d batches, with average throughput %.2f MB/s\n", producerNumber, messagesCount, messagesBatch, avgThroughput)

	return avgLatency, avgThroughput
}
