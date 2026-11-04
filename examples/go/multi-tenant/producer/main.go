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

// Package main shows one stream and one restricted user per tenant.
//
// The root user creates every stream and user, then each tenant connects with
// its own credentials and sends to its own stream only. The last step of every
// tenant is an attempt to read another tenant's stream, which the server must
// refuse.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/apache/iggy/examples/go/common"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	tenantPassword   = "secret"
	topicName        = "events"
	partitionID      = uint32(0)
	partitionsCount  = uint32(1)
	messagesPerBatch = 5
	batchesPerTenant = 2
	producerUsername = "tenant_%d_producer"
	tenantStream     = "tenant_%d_stream"
)

func main() {
	serverAddr, tcpOptions := common.ServerOptions()
	tenantsCount := common.TenantsFromEnv()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	log.Printf("Multi-tenant producer started, tenants: %d", tenantsCount)

	root, err := common.Dial(ctx, serverAddr, tcpOptions, common.DefaultRootUsername, common.DefaultRootPassword)
	if err != nil {
		log.Fatalf("Connect as the root user: %v", err)
	}

	tenants := make([]common.Tenant, 0, tenantsCount)
	for index := 1; index <= tenantsCount; index++ {
		current := common.Tenant{
			ID:     index,
			Stream: fmt.Sprintf(tenantStream, index),
			User:   fmt.Sprintf(producerUsername, index),
		}
		stream, err := ensureStream(ctx, root, current.Stream)
		if err != nil {
			log.Fatalf("Prepare stream for tenant %d: %v", current.ID, err)
		}
		if err := ensureTopic(ctx, root, stream.Id, topicName); err != nil {
			log.Fatalf("Prepare topic for tenant %d: %v", current.ID, err)
		}
		if err := common.EnsureUser(ctx, root, current.User, tenantPassword, producerPermissions(stream.Id)); err != nil {
			log.Fatalf("Prepare user %q: %v", current.User, err)
		}
		tenants = append(tenants, current)
	}
	common.Close(root)

	for _, current := range tenants {
		producer, err := common.Dial(ctx, serverAddr, tcpOptions, current.User, tenantPassword)
		if err != nil {
			log.Fatalf("Connect as %q: %v", current.User, err)
		}
		if err := sendMessages(ctx, producer, current); err != nil {
			common.Close(producer)
			log.Fatalf("Tenant %d: %v", current.ID, err)
		}
		if err := common.EnsureNoAccess(ctx, producer, current, tenants); err != nil {
			common.Close(producer)
			log.Fatalf("Tenant %d: %v", current.ID, err)
		}
		common.Close(producer)
	}

	log.Printf("Multi-tenant producer finished, %d tenant(s) sent %d message(s) each to their own stream",
		len(tenants), messagesPerBatch*batchesPerTenant)
}

// sendMessages writes to the tenant's own stream with the tenant's own client.
func sendMessages(ctx context.Context, cli iggcon.Client, current common.Tenant) error {
	streamIdentifier, err := iggcon.NewIdentifier(current.Stream)
	if err != nil {
		return err
	}
	topicIdentifier, err := iggcon.NewIdentifier(topicName)
	if err != nil {
		return err
	}

	for batch := 1; batch <= batchesPerTenant; batch++ {
		messages := make([]iggcon.IggyMessage, 0, messagesPerBatch)
		for index := 1; index <= messagesPerBatch; index++ {
			payload := fmt.Sprintf("event-%d-%d-tenant-%d", batch, index, current.ID)
			message, err := iggcon.NewIggyMessage([]byte(payload))
			if err != nil {
				return fmt.Errorf("build message: %w", err)
			}
			messages = append(messages, message)
		}
		if _, err := cli.SendMessages(ctx, streamIdentifier, topicIdentifier, iggcon.PartitionId(partitionID), messages); err != nil {
			return fmt.Errorf("send batch %d to stream %q: %w", batch, current.Stream, err)
		}
		log.Printf("Tenant %d (%s) sent %d message(s) to %s -> %s",
			current.ID, current.User, messagesPerBatch, current.Stream, topicName)
	}
	return nil
}

// ensureStream returns the named stream, creating it when this is the first run.
func ensureStream(ctx context.Context, cli iggcon.Client, name string) (*iggcon.StreamDetails, error) {
	identifier, err := iggcon.NewIdentifier(name)
	if err != nil {
		return nil, err
	}
	stream, err := cli.GetStream(ctx, identifier)
	if err == nil {
		log.Printf("Reusing stream %q (id %d) from an earlier run", name, stream.Id)
		return stream, nil
	}

	stream, err = cli.CreateStream(ctx, name, nil)
	if err == nil {
		log.Printf("Created stream %q (id %d)", name, stream.Id)
		return stream, nil
	}
	if !errors.Is(err, ierror.ErrStreamNameAlreadyExists) {
		return nil, fmt.Errorf("create stream %q: %w", name, err)
	}
	stream, err = cli.GetStream(ctx, identifier)
	if err != nil {
		return nil, fmt.Errorf("read stream %q after it already existed: %w", name, err)
	}
	log.Printf("Reusing stream %q (id %d) from an earlier run", name, stream.Id)
	return stream, nil
}

// ensureTopic returns the named topic of the stream, creating it when missing.
func ensureTopic(ctx context.Context, cli iggcon.Client, streamID uint32, name string) error {
	streamIdentifier, err := iggcon.NewIdentifier(streamID)
	if err != nil {
		return err
	}
	topicIdentifier, err := iggcon.NewIdentifier(name)
	if err != nil {
		return err
	}
	if _, err := cli.GetTopic(ctx, streamIdentifier, topicIdentifier); err == nil {
		log.Printf("Reusing topic %q of stream %d", name, streamID)
		return nil
	}

	partitions := partitionsCount
	if _, err := cli.CreateTopic(ctx, streamIdentifier, name, &iggcon.TopicOptions{PartitionsCount: &partitions}); err != nil {
		if errors.Is(err, ierror.ErrTopicNameAlreadyExists) {
			log.Printf("Reusing topic %q of stream %d", name, streamID)
			return nil
		}
		return fmt.Errorf("create topic %q in stream %d: %w", name, streamID, err)
	}
	log.Printf("Created topic %q in stream %d", name, streamID)
	return nil
}

// producerPermissions grant exactly what sending needs: reading the stream to
// confirm access, and appending messages to that stream only.
func producerPermissions(streamID uint32) *iggcon.Permissions {
	return &iggcon.Permissions{
		Streams: map[int]*iggcon.StreamPermissions{
			int(streamID): {
				ReadStream:   true,
				SendMessages: true,
			},
		},
	}
}
