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
	"flag"
	"github.com/apache/iggy/examples/go/common"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/apache/iggy/foreign/go/tcp"
	"log"
	"net"
	"time"
)

var (
    StreamID     = uint32(0)
    TopicID      = uint32(0)
    PartitionID  = uint32(0)
    BatchesLimit = uint32(5)
)

func main() {
	client, err := iggycli.NewIggyClient(
		iggycli.WithTcp(
			tcp.WithServerAddress(getTcpServerAddr()),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.LoginUser(common.DefaultRootUsername, common.DefaultRootPassword)
	if err != nil {
		log.Fatal(err)
	}

	err = consumeMessages(client)
	if err != nil {
		log.Fatal(err)
	}
}

func consumeMessages(client iggycli.Client) error {
	interval := 500 * time.Millisecond
	log.Printf(
		"Messages will be consumed from stream: %d, topic: %d, partition: %d with interval %s.",
		StreamID, TopicID, PartitionID, interval,
	)

	offset := uint64(0)
	const messagePerBatch = 10
	consumedBatches := uint32(0)
	consumer := iggcon.DefaultConsumer()
	for {
		if consumedBatches == BatchesLimit {
			log.Printf("Consumed %d batches of messages, exiting.", consumedBatches)
			return nil
		}

		streamIdentifier, _ := iggcon.NewIdentifier(StreamID)
		topicIdentifier, _ := iggcon.NewIdentifier(TopicID)
		pollMessages, err := client.
			PollMessages(
				streamIdentifier,
				topicIdentifier,
				consumer,
				iggcon.OffsetPollingStrategy(offset),
				messagePerBatch,
				false,
				&PartitionID,
			)
		if err != nil {
			return err
		}

		if len(pollMessages.Messages) == 0 {
			log.Println("No messages found.")
			time.Sleep(interval)
			continue
		}

		offset += uint64(len(pollMessages.Messages))
		for _, message := range pollMessages.Messages {
			err = handleMessage(message)
			if err != nil {
				return err
			}
		}
		consumedBatches++
		time.Sleep(interval)
	}
}

func handleMessage(message iggcon.IggyMessage) error {
	payload := string(message.Payload)
	log.Printf(
		"Handling message at offset: %d, payload: %s...",
		message.Header.Offset, payload)
	return nil
}

func getTcpServerAddr() string {
	tcpServerAddr := flag.String("tcp-server-address", "127.0.0.1:8090", "TCP server address")
	flag.Parse()

	if _, err := net.ResolveTCPAddr("tcp", *tcpServerAddr); err != nil {
		log.Fatalf("Invalid server address %s! Usage: --tcp-server-address <server-address>", *tcpServerAddr)
	}

	log.Printf("Using server address: %s\n", *tcpServerAddr)
	return *tcpServerAddr
}
