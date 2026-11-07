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

// Message-envelope consumer reads the JSON envelopes of the message-envelope
// producer and uses each envelope's message_type to decode its payload into
// OrderCreated, OrderConfirmed or OrderRejected.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/apache/iggy/examples/go/common"
	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
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

type OrderCreated struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Amount     int    `json:"amount"`
}

type OrderConfirmed struct {
	OrderID   string `json:"order_id"`
	Timestamp int64  `json:"timestamp"`
}

type OrderRejected struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
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

func main() {
	serverAddr, tcpOptions := getTcpOptions()

	cli, err := client.NewIggyClient(
		client.WithTcp(tcpOptions...),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := cli.Close(); err != nil {
			log.Printf("Error closing client: %v", err)
		}
	}()

	ctx := context.Background()
	log.Printf("Connecting to server at %s", serverAddr)
	if err := cli.Connect(ctx); err != nil {
		log.Printf("Failed to connect to server at %s: %v", serverAddr, err)
	}

	_, err = cli.LoginUser(ctx, common.DefaultRootUsername, common.DefaultRootPassword)
	if err != nil {
		log.Fatal(err)
	}

	err = consumeMessages(ctx, cli)
	if err != nil {
		log.Fatal(err)
	}
}

func consumeMessages(ctx context.Context, client iggcon.Client) error {
	interval := 500 * time.Millisecond
	log.Printf(
		"Messages will be consumed from stream: %s, topic: %s, partition: %d with interval %s.",
		StreamID, TopicID, PartitionID, interval,
	)

	streamIdentifier, err := iggcon.NewIdentifier(StreamID)
	if err != nil {
		return err
	}
	topicIdentifier, err := iggcon.NewIdentifier(TopicID)
	if err != nil {
		return err
	}

	offset := uint64(0)
	const messagePerBatch = 10
	consumedBatches := uint32(0)
	consumer := iggcon.DefaultConsumer()
	for {
		if consumedBatches == BatchesLimit {
			log.Printf("Consumed %d batches of messages, exiting.", consumedBatches)
			return nil
		}

		options := iggcon.DefaultPollOptions()
		options.Count = messagePerBatch
		pollMessages, err := client.
			PollMessages(
				ctx,
				streamIdentifier,
				topicIdentifier,
				PartitionID,
				consumer,
				iggcon.OffsetPollingStrategy(offset),
				options,
			)
		if err != nil {
			return err
		}

		if len(pollMessages.Messages) == 0 {
			log.Println("No messages found.")
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(interval):
			}
			continue
		}

		for _, message := range pollMessages.Messages {
			err = handleMessage(message)
			if err != nil {
				return err
			}
			offset = message.Header.Offset + 1
		}
		consumedBatches++
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
}

func handleMessage(message iggcon.IggyMessage) error {
	offset := message.Header.Offset
	envelope := &MessageEnvelope{}
	if err := json.Unmarshal(message.Payload, envelope); err != nil {
		return fmt.Errorf("decode envelope at offset %d: %w", offset, err)
	}
	log.Printf("Handling message type: %s...", envelope.MessageType)
	switch envelope.MessageType {
	case orderCreatedType:
		orderCreated := &OrderCreated{}
		if err := json.Unmarshal([]byte(envelope.Payload), orderCreated); err != nil {
			return fmt.Errorf("decode %s payload at offset %d: %w", envelope.MessageType, offset, err)
		}
		log.Printf("Order Created: %+v", orderCreated)
	case orderConfirmedType:
		orderConfirmed := &OrderConfirmed{}
		if err := json.Unmarshal([]byte(envelope.Payload), orderConfirmed); err != nil {
			return fmt.Errorf("decode %s payload at offset %d: %w", envelope.MessageType, offset, err)
		}
		log.Printf("Order Confirmed: %+v", orderConfirmed)
	case orderRejectedType:
		orderRejected := &OrderRejected{}
		if err := json.Unmarshal([]byte(envelope.Payload), orderRejected); err != nil {
			return fmt.Errorf("decode %s payload at offset %d: %w", envelope.MessageType, offset, err)
		}
		log.Printf("Order Rejected: %+v", orderRejected)
	default:
		log.Printf("Received unknown message type: %s", envelope.MessageType)
	}

	return nil
}

func getTcpOptions() (string, []tcp.Option) {
	tcpServerAddr := flag.String("tcp-server-address", "127.0.0.1:8090", "TCP server address")
	tlsEnabled := flag.Bool("tls", false, "Enable TLS")
	tlsCAFile := flag.String("tls-ca-file", "", "Path to CA certificate file for TLS")
	tlsDomain := flag.String("tls-domain", "", "TLS server domain name for SNI")
	flag.Parse()

	if _, err := net.ResolveTCPAddr("tcp", *tcpServerAddr); err != nil {
		log.Fatalf("Invalid server address %s! Usage: --tcp-server-address <server-address>", *tcpServerAddr)
	}

	tcpOptions := []tcp.Option{
		tcp.WithServerAddress(*tcpServerAddr),
	}

	if *tlsEnabled {
		var tlsOpts []tcp.TLSOption
		if *tlsCAFile != "" {
			tlsOpts = append(tlsOpts, tcp.WithTLSCAFile(*tlsCAFile))
		}
		if *tlsDomain != "" {
			tlsOpts = append(tlsOpts, tcp.WithTLSDomain(*tlsDomain))
		}
		tcpOptions = append(tcpOptions, tcp.WithTLS(tlsOpts...))
		log.Printf("TLS enabled with CA file: %s, domain: %s", *tlsCAFile, *tlsDomain)
	}

	return *tcpServerAddr, tcpOptions
}
