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
	"fmt"
	"github.com/apache/iggy/examples/go/common"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"log"
	"net"
	"time"

	"github.com/apache/iggy/foreign/go/iggycli"
	"github.com/apache/iggy/foreign/go/tcp"
)

var (
    StreamId     = uint32(0)
    TopicId      = uint32(0)
    PartitionId  = uint32(0)
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

	if _, err := client.LoginUser(common.DefaultRootUsername, common.DefaultRootPassword); err != nil {
		log.Fatalf("Login failed: %v", err)
	}
	initSystem(client)
	if err := produceMessages(client); err != nil {
		log.Fatalf("Producing messages failed: %v", err)
	}
}

func initSystem(client iggycli.Client) {
    if _, err := client.CreateStream("sample-stream"); err != nil {
		log.Printf("WARN: Stream already exists or error: %v", err)
	}
	log.Println("Stream was created.")

	streamIdentifier, _ := iggcon.NewIdentifier(StreamId)
    if _, err := client.CreateTopic(
		streamIdentifier,
		"sample-topic",
		1,
		iggcon.CompressionAlgorithmNone,
		iggcon.IggyExpiryNeverExpire,
		0,
        nil); err != nil {
		log.Printf("WARN: Topic already exists and will not be created again or error: %v", err)
	}
	log.Println("Topic was created.")
}

func produceMessages(client iggycli.Client) error {
	interval := 500 * time.Millisecond
	log.Printf(
		"Messages will be sent to stream: %d, topic: %d, partition: %d with interval %s.",
		StreamId, TopicId, PartitionId, interval)

	currentID := 0
	messagesPerBatch := 10
	sentBatches := uint32(0)
	partitioning := iggcon.PartitionId(PartitionId)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if sentBatches == BatchesLimit {
			log.Printf("Sent %d batches of messages, exiting.", sentBatches)
			return nil
		}
		<-ticker.C

		var messages = make([]iggcon.IggyMessage, 0, messagesPerBatch)
		for i := 0; i < messagesPerBatch; i++ {
			currentID++
			payload := fmt.Sprintf("message-%d", currentID)
			message, _ := iggcon.NewIggyMessage([]byte(payload))
			messages = append(messages, message)
		}

		streamIdentifier, _ := iggcon.NewIdentifier(StreamId)
		topicIdentifier, _ := iggcon.NewIdentifier(TopicId)
		if err := client.SendMessages(
			streamIdentifier,
			topicIdentifier,
			partitioning,
			messages); err != nil {
			return err
		}
		sentBatches++
		log.Printf("Sent %d message(s).", messagesPerBatch)
	}
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
