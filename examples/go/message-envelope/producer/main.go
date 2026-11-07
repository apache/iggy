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

// Message-envelope producer wraps OrderCreated, OrderConfirmed and
// OrderRejected events in a JSON envelope whose message_type tells the
// consumer how to decode the payload, so different types share one topic.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/apache/iggy/examples/go/common"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// A dedicated stream keeps these envelopes apart from the plain-text
// messages other examples write to the same server.
var (
	StreamID     = "message-envelope-stream"
	TopicID      = "message-envelope-topic"
	PartitionID  = uint32(0)
	BatchesLimit = uint32(5)
)

type SerializableMessage interface {
	messageType() string
}

type OrderCreated struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Amount     int    `json:"amount"`
}

func (*OrderCreated) messageType() string {
	return orderCreatedType
}

type OrderConfirmed struct {
	OrderID   string `json:"order_id"`
	Timestamp int64  `json:"timestamp"`
}

func (*OrderConfirmed) messageType() string {
	return orderConfirmedType
}

type OrderRejected struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func (*OrderRejected) messageType() string {
	return orderRejectedType
}

type MessageEnvelope struct {
	MessageType string `json:"message_type"`
	Payload     string `json:"payload"`
}

const (
	orderCreatedType   = "OrderCreated"
	orderConfirmedType = "OrderConfirmed"
	orderRejectedType  = "OrderRejected"
)

func encodeEnvelope(sm SerializableMessage) ([]byte, error) {
	payload, err := json.Marshal(sm)
	if err != nil {
		return nil, err
	}
	return json.Marshal(MessageEnvelope{
		MessageType: sm.messageType(),
		Payload:     string(payload),
	})
}

type MessagesGenerator struct {
	orderID int
}

func NewMessagesGenerator() *MessagesGenerator {
	return &MessagesGenerator{}
}

func (g *MessagesGenerator) generate() SerializableMessage {
	g.orderID++
	switch g.orderID % 3 {
	case 0:
		return &OrderCreated{
			OrderID:    fmt.Sprintf("order-%d", g.orderID),
			CustomerID: fmt.Sprintf("customer-%d", rand.Intn(100)),
			Amount:     rand.Intn(10000),
		}
	case 1:
		return &OrderConfirmed{
			OrderID:   fmt.Sprintf("order-%d", g.orderID),
			Timestamp: time.Now().UnixMilli(),
		}
	default:
		return &OrderRejected{
			OrderID: fmt.Sprintf("order-%d", g.orderID),
			Reason:  "Insufficient balance",
		}
	}
}

func main() {
	serverAddr, tcpOptions := common.GetTcpOptions()

	ctx := context.Background()
	log.Printf("Connecting to server at %s", serverAddr)

	cli, err := common.Dial(
		ctx,
		serverAddr,
		tcpOptions,
		common.DefaultRootUsername,
		common.DefaultRootPassword,
	)
	if err != nil {
		log.Fatalf("Iggy Client Dial Failed: %v", err)
	}
	defer common.Close(cli)
	initSystem(ctx, cli)
	if err := produceMessages(ctx, cli); err != nil {
		log.Fatalf("Producing messages failed: %v", err)
	}
}

func initSystem(ctx context.Context, client iggcon.Client) {
	if _, err := client.CreateStream(ctx, StreamID, nil); err != nil {
		log.Printf("WARN: Stream already exists or error: %v", err)
	} else {
		log.Println("Stream was created.")
	}

	streamIdentifier, _ := iggcon.NewIdentifier(StreamID)
	partitions := uint32(1)
	expiry := iggcon.IggyExpiryNeverExpire
	if _, err := client.CreateTopic(
		ctx,
		streamIdentifier,
		TopicID,
		&iggcon.TopicOptions{PartitionsCount: &partitions, MessageExpiry: &expiry}); err != nil {
		log.Printf("WARN: Topic already exists and will not be created again or error: %v", err)
	} else {
		log.Println("Topic was created.")
	}
}

func produceMessages(ctx context.Context, client iggcon.Client) error {
	interval := 500 * time.Millisecond
	log.Printf(
		"Messages will be sent to stream: %s, topic: %s, partition: %d with interval %s.",
		StreamID, TopicID, PartitionID, interval)

	streamIdentifier, err := iggcon.NewIdentifier(StreamID)
	if err != nil {
		return err
	}
	topicIdentifier, err := iggcon.NewIdentifier(TopicID)
	if err != nil {
		return err
	}

	messagesPerBatch := 10
	sentBatches := uint32(0)
	partitioning := iggcon.PartitionId(PartitionID)

	messagesGenerator := NewMessagesGenerator()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if sentBatches == BatchesLimit {
			log.Printf("Sent %d batches of messages, exiting.", sentBatches)
			return nil
		}
		<-ticker.C

		messages := make([]iggcon.IggyMessage, 0, messagesPerBatch)
		for i := 0; i < messagesPerBatch; i++ {
			serializableMessage := messagesGenerator.generate()
			payload, err := encodeEnvelope(serializableMessage)
			if err != nil {
				return fmt.Errorf("encode %s envelope: %w", serializableMessage.messageType(), err)
			}
			message, err := iggcon.NewIggyMessage(payload)
			if err != nil {
				return fmt.Errorf("build message: %w", err)
			}
			messages = append(messages, message)
		}

		if _, err := client.SendMessages(
			ctx,
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
