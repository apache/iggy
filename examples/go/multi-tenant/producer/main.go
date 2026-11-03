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
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/apache/iggy/examples/go/common"
	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
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
	defaultTenants   = 3
	producerUsername = "tenant_%d_producer"
	tenantStream     = "tenant_%d_stream"
	connectTimeout   = 30 * time.Second
)

type tenant struct {
	id     int
	stream string
	user   string
}

func main() {
	serverAddr, tcpOptions := getTcpOptions()
	tenantsCount := tenantsFromEnv()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	log.Printf("Multi-tenant producer started, tenants: %d", tenantsCount)

	root, err := dial(ctx, serverAddr, tcpOptions, common.DefaultRootUsername, common.DefaultRootPassword)
	if err != nil {
		log.Fatalf("Connect as the root user: %v", err)
	}

	tenants := make([]tenant, 0, tenantsCount)
	for index := 1; index <= tenantsCount; index++ {
		current := tenant{
			id:     index,
			stream: fmt.Sprintf(tenantStream, index),
			user:   fmt.Sprintf(producerUsername, index),
		}
		stream, err := ensureStream(ctx, root, current.stream)
		if err != nil {
			log.Fatalf("Prepare stream for tenant %d: %v", current.id, err)
		}
		if err := ensureTopic(ctx, root, stream.Id, topicName); err != nil {
			log.Fatalf("Prepare topic for tenant %d: %v", current.id, err)
		}
		if err := ensureUser(ctx, root, current.user, producerPermissions(stream.Id)); err != nil {
			log.Fatalf("Prepare user %q: %v", current.user, err)
		}
		tenants = append(tenants, current)
	}
	closeClient(root)

	for _, current := range tenants {
		producer, err := dial(ctx, serverAddr, tcpOptions, current.user, tenantPassword)
		if err != nil {
			log.Fatalf("Connect as %q: %v", current.user, err)
		}
		if err := sendMessages(ctx, producer, current); err != nil {
			closeClient(producer)
			log.Fatalf("Tenant %d: %v", current.id, err)
		}
		if err := ensureNoAccess(ctx, producer, current, tenants); err != nil {
			closeClient(producer)
			log.Fatalf("Tenant %d: %v", current.id, err)
		}
		closeClient(producer)
	}

	log.Printf("Multi-tenant producer finished, %d tenant(s) sent %d message(s) each to their own stream",
		len(tenants), messagesPerBatch*batchesPerTenant)
}

// sendMessages writes to the tenant's own stream with the tenant's own client.
func sendMessages(ctx context.Context, cli iggcon.Client, current tenant) error {
	streamIdentifier, err := iggcon.NewIdentifier(current.stream)
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
			payload := fmt.Sprintf("event-%d-%d-tenant-%d", batch, index, current.id)
			message, err := iggcon.NewIggyMessage([]byte(payload))
			if err != nil {
				return fmt.Errorf("build message: %w", err)
			}
			messages = append(messages, message)
		}
		if _, err := cli.SendMessages(ctx, streamIdentifier, topicIdentifier, iggcon.PartitionId(partitionID), messages); err != nil {
			return fmt.Errorf("send batch %d to stream %q: %w", batch, current.stream, err)
		}
		log.Printf("Tenant %d (%s) sent %d message(s) to %s -> %s",
			current.id, current.user, messagesPerBatch, current.stream, topicName)
	}
	return nil
}

// ensureNoAccess proves the tenant can read its own stream and cannot read any
// other tenant's stream.
func ensureNoAccess(ctx context.Context, cli iggcon.Client, current tenant, tenants []tenant) error {
	own, err := iggcon.NewIdentifier(current.stream)
	if err != nil {
		return err
	}
	if _, err := cli.GetStream(ctx, own); err != nil {
		return fmt.Errorf("tenant %s cannot read its own stream %q: %w", current.user, current.stream, err)
	}
	log.Printf("Tenant %d (%s) can read its own stream %q", current.id, current.user, current.stream)

	for _, other := range tenants {
		if other.stream == current.stream {
			continue
		}
		identifier, err := iggcon.NewIdentifier(other.stream)
		if err != nil {
			return err
		}
		if _, err := cli.GetStream(ctx, identifier); err != nil {
			if errors.Is(err, ierror.ErrUnauthorized) || errors.Is(err, ierror.ErrStreamIdNotFound) {
				log.Printf("Tenant %d (%s) was refused stream %q as expected",
					current.id, current.user, other.stream)
				continue
			}
			return fmt.Errorf("read stream %q as %s: %w", other.stream, current.user, err)
		}
		return fmt.Errorf("tenant %s can read stream %q, the server should have refused it",
			current.user, other.stream)
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

// ensureUser creates the user with the given permissions, or replaces the
// permissions of a user an earlier run left behind, so a second run is safe.
func ensureUser(ctx context.Context, cli iggcon.Client, username string, permissions *iggcon.Permissions) error {
	identifier, err := iggcon.NewIdentifier(username)
	if err != nil {
		return err
	}
	if _, err := cli.GetUser(ctx, identifier); err == nil {
		if err := cli.UpdatePermissions(ctx, identifier, permissions); err != nil {
			return fmt.Errorf("update permissions of user %q: %w", username, err)
		}
		log.Printf("Reusing user %q with its permissions replaced", username)
		return nil
	}

	if _, err := cli.CreateUser(ctx, username, tenantPassword, iggcon.Active, permissions, nil); err == nil {
		log.Printf("Created user %q", username)
		return nil
	} else if !errors.Is(err, ierror.ErrUserAlreadyExists) {
		return fmt.Errorf("create user %q: %w", username, err)
	}
	if err := cli.UpdatePermissions(ctx, identifier, permissions); err != nil {
		return fmt.Errorf("update permissions of user %q: %w", username, err)
	}
	log.Printf("Reusing user %q with its permissions replaced", username)
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

func dial(ctx context.Context, address string, options []tcp.Option, username, password string) (iggcon.Client, error) {
	cli, err := client.NewIggyClient(client.WithTcp(options...))
	if err != nil {
		return nil, err
	}
	connectCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	if err := cli.Connect(connectCtx); err != nil {
		return nil, fmt.Errorf("connect to %s: %w", address, err)
	}
	if _, err := cli.LoginUser(connectCtx, username, password); err != nil {
		_ = cli.Close()
		return nil, fmt.Errorf("log in as %q: %w", username, err)
	}
	return cli, nil
}

func closeClient(cli iggcon.Client) {
	if cli == nil {
		return
	}
	if err := cli.Close(); err != nil {
		log.Printf("Error closing client: %v", err)
	}
}

func tenantsFromEnv() int {
	raw, ok := os.LookupEnv("TENANTS_COUNT")
	if !ok || raw == "" {
		return defaultTenants
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 1 {
		log.Fatalf("TENANTS_COUNT must be a positive integer, got %q", raw)
	}
	return count
}

func getTcpOptions() (string, []tcp.Option) {
	tcpServerAddr := flag.String("tcp-server-address", "127.0.0.1:8090", "TCP server address")
	flag.Parse()

	if _, err := net.ResolveTCPAddr("tcp", *tcpServerAddr); err != nil {
		log.Fatalf("Invalid server address %s! Usage: --tcp-server-address <server-address>", *tcpServerAddr)
	}

	return *tcpServerAddr, []tcp.Option{tcp.WithServerAddress(*tcpServerAddr)}
}
