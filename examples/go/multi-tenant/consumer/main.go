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

// Package main reads every tenant's stream as that tenant, with the tenant's
// own restricted credentials.
//
// The streams and their users come from the multi-tenant producer, so run it
// first. The consumer only adds its own read-only user per tenant, then proves
// that each of those users can read its own stream and no other.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/iggy/examples/go/common"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

const (
	tenantPassword    = "secret"
	topicName         = "events"
	partitionID       = uint32(0)
	messagesPerBatch  = 5
	consumerUsername  = "tenant_%d_consumer"
	tenantStream      = "tenant_%d_stream"
	pollAttempts      = 20
	pollRetryInterval = 100 * time.Millisecond
)

func main() {
	serverAddr, tcpOptions := common.ServerOptions()
	tenantsCount := common.TenantsFromEnv()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	log.Printf("Multi-tenant consumer started, tenants: %d", tenantsCount)

	root, err := common.Dial(ctx, serverAddr, tcpOptions, common.DefaultRootUsername, common.DefaultRootPassword)
	if err != nil {
		log.Fatalf("Connect as the root user: %v", err)
	}

	tenants := make([]common.Tenant, 0, tenantsCount)
	for index := 1; index <= tenantsCount; index++ {
		current := common.Tenant{
			ID:     index,
			Stream: fmt.Sprintf(tenantStream, index),
			User:   fmt.Sprintf(consumerUsername, index),
		}
		stream, err := existingStream(ctx, root, current.Stream)
		if err != nil {
			common.Close(root)
			log.Fatalf("Tenant %d: %v", current.ID, err)
		}
		if err := existingTopic(ctx, root, stream.Id, topicName); err != nil {
			common.Close(root)
			log.Fatalf("Tenant %d: %v", current.ID, err)
		}
		if err := common.EnsureUser(ctx, root, current.User, tenantPassword, consumerPermissions(stream.Id)); err != nil {
			common.Close(root)
			log.Fatalf("Prepare user %q: %v", current.User, err)
		}
		tenants = append(tenants, current)
	}
	common.Close(root)

	for _, current := range tenants {
		consumer, err := common.Dial(ctx, serverAddr, tcpOptions, current.User, tenantPassword)
		if err != nil {
			log.Fatalf("Connect as %q: %v", current.User, err)
		}
		received, err := receiveMessages(ctx, consumer, current)
		if err != nil {
			common.Close(consumer)
			log.Fatalf("Tenant %d: %v", current.ID, err)
		}
		if err := common.EnsureNoAccess(ctx, consumer, current, tenants); err != nil {
			common.Close(consumer)
			log.Fatalf("Tenant %d: %v", current.ID, err)
		}
		log.Printf("Tenant %d (%s) read %d message(s) from its own stream %q",
			current.ID, current.User, received, current.Stream)
		common.Close(consumer)
	}

	log.Printf("Multi-tenant consumer finished, %d tenant(s) read their own stream only", len(tenants))
}

// receiveMessages polls the tenant's own topic from the first offset, with the
// tenant's own client.
func receiveMessages(ctx context.Context, cli iggcon.Client, current common.Tenant) (int, error) {
	streamIdentifier, err := iggcon.NewIdentifier(current.Stream)
	if err != nil {
		return 0, err
	}
	topicIdentifier, err := iggcon.NewIdentifier(topicName)
	if err != nil {
		return 0, err
	}

	options := iggcon.DefaultPollOptions()
	options.Count = messagesPerBatch

	for attempt := 1; ; attempt++ {
		polled, err := cli.PollMessages(ctx, streamIdentifier, topicIdentifier, partitionID,
			iggcon.DefaultConsumer(), iggcon.OffsetPollingStrategy(0), options)
		if err != nil {
			return 0, fmt.Errorf("poll stream %q: %w", current.Stream, err)
		}
		if len(polled.Messages) == 0 {
			if attempt >= pollAttempts {
				return 0, fmt.Errorf("no messages in stream %q after %d polls: run the multi-tenant producer first",
					current.Stream, pollAttempts)
			}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(pollRetryInterval):
			}
			continue
		}
		for _, message := range polled.Messages {
			log.Printf("Tenant %d (%s) received %q from %s at offset %d",
				current.ID, current.User, string(message.Payload), current.Stream, message.Header.Offset)
		}
		return len(polled.Messages), nil
	}
}

// existingStream returns a stream the producer created, and explains how to get
// one when it is missing.
func existingStream(ctx context.Context, cli iggcon.Client, name string) (*iggcon.StreamDetails, error) {
	identifier, err := iggcon.NewIdentifier(name)
	if err != nil {
		return nil, err
	}
	stream, err := cli.GetStream(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("stream %q is not available (%v): run the multi-tenant producer first", name, err)
	}
	return stream, nil
}

// existingTopic checks a topic the producer created is there.
func existingTopic(ctx context.Context, cli iggcon.Client, streamID uint32, name string) error {
	streamIdentifier, err := iggcon.NewIdentifier(streamID)
	if err != nil {
		return err
	}
	topicIdentifier, err := iggcon.NewIdentifier(name)
	if err != nil {
		return err
	}
	if _, err := cli.GetTopic(ctx, streamIdentifier, topicIdentifier); err != nil {
		return fmt.Errorf("topic %q of stream %d is not available (%v): run the multi-tenant producer first",
			name, streamID, err)
	}
	return nil
}

// consumerPermissions grant reading the tenant's own stream. A stream-level
// read_stream already covers reading that stream's topics and polling their
// messages, so no topic-level grant is needed.
func consumerPermissions(streamID uint32) *iggcon.Permissions {
	return &iggcon.Permissions{
		Streams: map[int]*iggcon.StreamPermissions{
			int(streamID): {
				ReadStream: true,
			},
		},
	}
}
