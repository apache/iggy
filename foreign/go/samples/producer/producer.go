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

package main

import (
	"fmt"
	"time"

	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/apache/iggy/foreign/go/tcp"
	"github.com/google/uuid"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	sharedDemoContracts "github.com/apache/iggy/foreign/go/samples/shared"
)

const (
    StreamId          = uint32(0)
    TopicId           = uint32(0)
    MessageBatchCount = 1
    Partition         = 0
    Interval          = 1000
)

func main() {
	cli, err := iggycli.NewIggyClient(
		iggycli.WithTcp(
			tcp.WithServerAddress("127.0.0.1:8090"),
		),
	)
	if err != nil {
		panic(err)
	}
	_, err = cli.LoginUser("iggy", "iggy")
	if err != nil {
		panic("COULD NOT LOG IN")
	}

	if err = EnsureInfrastructureIsInitialized(cli); err != nil {
		panic(err)
	}

	if err = PublishMessages(cli); err != nil {
		panic(err)
	}
}

func EnsureInfrastructureIsInitialized(cli iggycli.Client) error {
	streamIdentifier, _ := iggcon.NewIdentifier(StreamId)
    if _, streamErr := cli.GetStream(streamIdentifier); streamErr != nil {
        _, streamErr = cli.CreateStream("Test Producer Stream")

		fmt.Println(StreamId)

		if streamErr != nil {
			panic(streamErr)
		}

		fmt.Printf("Created stream with ID: %d.\n", StreamId)
	}

	fmt.Printf("Stream with ID: %d exists.\n", StreamId)

	topicIdentifier, _ := iggcon.NewIdentifier(TopicId)
    if _, topicErr := cli.GetTopic(streamIdentifier, topicIdentifier); topicErr != nil {
        _, topicErr = cli.CreateTopic(
			streamIdentifier,
			"Test Topic From Producer Sample",
			12,
			0,
			0,
			0,
            nil)

		if topicErr != nil {
			panic(topicErr)
		}

		fmt.Printf("Created topic with ID: %d.\n", TopicId)
	}

	fmt.Printf("Topic with ID: %d exists.\n", TopicId)

	return nil
}

func PublishMessages(messageStream iggycli.Client) error {
	fmt.Printf("Messages will be sent to stream '%d', topic '%d', partition '%d' with interval %d ms.\n", StreamId, TopicId, Partition, Interval)
	messageGenerator := NewMessageGenerator()

	for {
		var debugMessages []sharedDemoContracts.ISerializableMessage
		var messages []iggcon.IggyMessage

		for i := 0; i < MessageBatchCount; i++ {
			message := messageGenerator.GenerateMessage()
			json := message.ToBytes()

			debugMessages = append(debugMessages, message)
			messages = append(messages, iggcon.IggyMessage{
				Header: iggcon.MessageHeader{
					Id:               iggcon.MessageID(uuid.New()),
					OriginTimestamp:  uint64(time.Now().UnixMicro()),
					UserHeaderLength: 0,
					PayloadLength:    uint32(len(json)),
				},
				Payload: json,
			})
		}

		streamIdentifier, _ := iggcon.NewIdentifier(StreamId)
		topicIdentifier, _ := iggcon.NewIdentifier(TopicId)
		err := messageStream.SendMessages(
			streamIdentifier,
			topicIdentifier,
			iggcon.PartitionId(Partition),
			messages,
		)
		if err != nil {
			fmt.Printf("%s", err)
			return nil
		}

		for _, m := range debugMessages {
			fmt.Println("Sent messages:", m.ToJson())
		}

		time.Sleep(time.Millisecond * time.Duration(Interval))
	}
}
