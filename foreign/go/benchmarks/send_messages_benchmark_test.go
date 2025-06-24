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

	"github.com/google/uuid"
	"github.com/iggy-rs/iggy-go-client"
	iggcon "github.com/iggy-rs/iggy-go-client/contracts"
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
	streams := make([]iggy.MessageStream, producerCount)

	factory := &iggy.IggyClientFactory{}
	config := iggcon.IggyConfiguration{
		BaseAddress: "127.0.0.1:8090",
		Protocol:    iggcon.Tcp,
	}

	for i := 0; i < producerCount; i++ {
		ms, err := factory.CreateMessageStream(config)
		if err != nil {
			panic("COULD NOT CREATE MESSAGE STREAM")
		}
		_, err = ms.LogIn(iggcon.LogInRequest{
			Username: "iggy",
			Password: "iggy",
		})
		if err != nil {
			panic("COULD NOT LOG IN")
		}
		streams[i] = ms
	}

	for index, value := range streams {
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
			avgLatency, avgThroughput := SendMessage(streams[i], i, messagesCount, messagesBatch, messageSize)

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

	for index, value := range streams {
		err := cleanupInfrastructure(value, startingStreamId+index)
		if err != nil {
			panic("COULD NOT CLEANUP INFRASTRUCTURE")
		}
	}
}

func ensureInfrastructureIsInitialized(messageStream iggy.MessageStream, streamId int) error {
	if _, streamErr := messageStream.GetStreamById(iggcon.GetStreamRequest{
		StreamID: iggcon.NewIdentifier(streamId)}); streamErr != nil {
		streamErr = messageStream.CreateStream(iggcon.CreateStreamRequest{
			StreamId: streamId,
			Name:     "benchmark" + fmt.Sprint(streamId),
		})
		if streamErr != nil {
			panic(streamErr)
		}
	}
	if _, topicErr := messageStream.GetTopicById(iggcon.NewIdentifier(streamId), iggcon.NewIdentifier(1)); topicErr != nil {
		topicErr = messageStream.CreateTopic(iggcon.CreateTopicRequest{
			TopicId:              1,
			Name:                 "benchmark",
			PartitionsCount:      1,
			StreamId:             iggcon.NewIdentifier(streamId),
			CompressionAlgorithm: 1,
		})

		if topicErr != nil {
			panic(topicErr)
		}
	}
	return nil
}

func cleanupInfrastructure(messageStream iggy.MessageStream, streamId int) error {
	return messageStream.DeleteStream(iggcon.NewIdentifier(streamId))
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
func SendMessage(bus iggy.MessageStream, producerNumber, messagesCount, messagesBatch, messageSize int) (avgLatency float64, avgThroughput float64) {
	totalMessages := messagesBatch * messagesCount
	totalMessagesBytes := int64(totalMessages * messageSize)
	fmt.Printf("Executing Send Messages command for producer %d, messages count %d, with size %d bytes\n", producerNumber, totalMessages, totalMessagesBytes)

	busStreamId := iggcon.NewIdentifier(startingStreamId + producerNumber)
	busTopicId := iggcon.NewIdentifier(topicId)
	messages := CreateMessages(messagesCount, messageSize)
	latencies := make([]time.Duration, 0)

	for i := 0; i < messagesBatch; i++ {
		startTime := time.Now()
		_ = bus.SendMessages(iggcon.SendMessagesRequest{
			StreamId:     busStreamId,
			TopicId:      busTopicId,
			Partitioning: iggcon.PartitionId(1),
			Messages:     messages,
		})
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
