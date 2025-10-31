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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/iggycli"
	sharedDemoContracts "github.com/apache/iggy/foreign/go/samples/shared"
	"github.com/apache/iggy/foreign/go/tcp"
)

// config
const (
    DefaultStreamId = uint32(0)
    TopicId         = uint32(0)
    Partition       = 0
    Interval        = 1000
    ConsumerId      = uint32(0)
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

	if err = ConsumeMessages(cli); err != nil {
		panic(err)
	}
}

func EnsureInfrastructureIsInitialized(cli iggycli.Client) error {
	streamIdentifier, _ := iggcon.NewIdentifier(DefaultStreamId)
    if _, streamErr := cli.GetStream(streamIdentifier); streamErr != nil {
        _, streamErr = cli.CreateStream("Test Producer Stream")

		if streamErr != nil {
			panic(streamErr)
		}

		fmt.Printf("Created stream with ID: %d.\n", DefaultStreamId)
	}

	fmt.Printf("Stream with ID: %d exists.\n", DefaultStreamId)

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

func ConsumeMessages(cli iggycli.Client) error {
	fmt.Printf("Messages will be polled from stream '%d', topic '%d', partition '%d' with interval %d ms.\n", DefaultStreamId, TopicId, Partition, Interval)

	for {
		streamIdentifier, _ := iggcon.NewIdentifier(DefaultStreamId)
		topicIdentifier, _ := iggcon.NewIdentifier(TopicId)
		consumerIdentifier, _ := iggcon.NewIdentifier(ConsumerId)
		partionId := uint32(Partition)
		messagesWrapper, err := cli.PollMessages(
			streamIdentifier,
			topicIdentifier,
			iggcon.NewSingleConsumer(consumerIdentifier),
			iggcon.NextPollingStrategy(),
			1,
			true,
			&partionId)
		if err != nil {
			return err
		}
		if messagesWrapper == nil {
			panic("Something went wrong - this edge case handling will be refactored")
		}
		messages := messagesWrapper.Messages
		if len(messages) != 0 {
			for _, message := range messages {
				if err := HandleMessage(message); err != nil {
					fmt.Printf("Error when consuming message: %s\n", err.Error())
				}
			}
		} else {
			fmt.Println("Received 0 messages.")
		}

		time.Sleep(time.Duration(Interval) * time.Millisecond)
	}
}

func HandleMessage(iggyMessage iggcon.IggyMessage) error {
	length := (len(iggyMessage.Payload) * 3) / 4
	bytes := make([]byte, length)

	str := string(iggyMessage.Payload)
	isBase64 := false

	if _, err := base64.StdEncoding.Decode(bytes, []byte(str)); err == nil {
		isBase64 = true
	}

	var envelope sharedDemoContracts.Envelope

	if isBase64 {
		bytes, err := base64.StdEncoding.DecodeString(str)
		if err != nil {
			return err
		}

		jsonStr := string(bytes)
		if err = json.Unmarshal([]byte(jsonStr), &envelope); err != nil {
			return err
		}
	} else {
		if err := json.Unmarshal(iggyMessage.Payload, &envelope); err != nil {
			return err
		}
	}

	fmt.Printf("Handling message type: %s at offset: %d with message Id: %s ", envelope.MessageType, iggyMessage.Header.Offset, iggyMessage.Header.Id)

	switch envelope.MessageType {
	case "order_created":
		var orderCreated sharedDemoContracts.OrderCreated
		if err := json.Unmarshal([]byte(envelope.Payload), &orderCreated); err != nil {
			return err
		}
		fmt.Printf("%+v\n", orderCreated)
	case "order_confirmed":
		var orderConfirmed sharedDemoContracts.OrderConfirmed
		if err := json.Unmarshal([]byte(envelope.Payload), &orderConfirmed); err != nil {
			return err
		}
		fmt.Printf("%+v\n", orderConfirmed)
	case "order_rejected":
		var orderRejected sharedDemoContracts.OrderRejected
		if err := json.Unmarshal([]byte(envelope.Payload), &orderRejected); err != nil {
			return err
		}
		fmt.Printf("%+v\n", orderRejected)
	}
	return nil
}
