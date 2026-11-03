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
	tenantPassword    = "secret"
	topicName         = "events"
	partitionID       = uint32(0)
	messagesPerBatch  = 5
	defaultTenants    = 3
	consumerUsername  = "tenant_%d_consumer"
	tenantStream      = "tenant_%d_stream"
	connectTimeout    = 30 * time.Second
	pollAttempts      = 20
	pollRetryInterval = 100 * time.Millisecond
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

	log.Printf("Multi-tenant consumer started, tenants: %d", tenantsCount)

	root, err := dial(ctx, serverAddr, tcpOptions, common.DefaultRootUsername, common.DefaultRootPassword)
	if err != nil {
		log.Fatalf("Connect as the root user: %v", err)
	}

	tenants := make([]tenant, 0, tenantsCount)
	for index := 1; index <= tenantsCount; index++ {
		current := tenant{
			id:     index,
			stream: fmt.Sprintf(tenantStream, index),
			user:   fmt.Sprintf(consumerUsername, index),
		}
		stream, err := existingStream(ctx, root, current.stream)
		if err != nil {
			closeClient(root)
			log.Fatalf("Tenant %d: %v", current.id, err)
		}
		topic, err := existingTopic(ctx, root, stream.Id, topicName)
		if err != nil {
			closeClient(root)
			log.Fatalf("Tenant %d: %v", current.id, err)
		}
		if err := ensureUser(ctx, root, current.user, consumerPermissions(stream.Id, topic.Id)); err != nil {
			closeClient(root)
			log.Fatalf("Prepare user %q: %v", current.user, err)
		}
		tenants = append(tenants, current)
	}
	closeClient(root)

	for _, current := range tenants {
		consumer, err := dial(ctx, serverAddr, tcpOptions, current.user, tenantPassword)
		if err != nil {
			log.Fatalf("Connect as %q: %v", current.user, err)
		}
		received, err := receiveMessages(ctx, consumer, current)
		if err != nil {
			closeClient(consumer)
			log.Fatalf("Tenant %d: %v", current.id, err)
		}
		if err := ensureNoAccess(ctx, consumer, current, tenants); err != nil {
			closeClient(consumer)
			log.Fatalf("Tenant %d: %v", current.id, err)
		}
		log.Printf("Tenant %d (%s) read %d message(s) from its own stream %q",
			current.id, current.user, received, current.stream)
		closeClient(consumer)
	}

	log.Printf("Multi-tenant consumer finished, %d tenant(s) read their own stream only", len(tenants))
}

// receiveMessages polls the tenant's own topic from the first offset, with the
// tenant's own client.
func receiveMessages(ctx context.Context, cli iggcon.Client, current tenant) (int, error) {
	streamIdentifier, err := iggcon.NewIdentifier(current.stream)
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
			return 0, fmt.Errorf("poll stream %q: %w", current.stream, err)
		}
		if len(polled.Messages) == 0 {
			if attempt >= pollAttempts {
				return 0, fmt.Errorf("no messages in stream %q after %d polls: run the multi-tenant producer first",
					current.stream, pollAttempts)
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
				current.id, current.user, string(message.Payload), current.stream, message.Header.Offset)
		}
		return len(polled.Messages), nil
	}
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

// existingTopic returns a topic the producer created.
func existingTopic(ctx context.Context, cli iggcon.Client, streamID uint32, name string) (*iggcon.TopicDetails, error) {
	streamIdentifier, err := iggcon.NewIdentifier(streamID)
	if err != nil {
		return nil, err
	}
	topicIdentifier, err := iggcon.NewIdentifier(name)
	if err != nil {
		return nil, err
	}
	topic, err := cli.GetTopic(ctx, streamIdentifier, topicIdentifier)
	if err != nil {
		return nil, fmt.Errorf("topic %q of stream %d is not available (%v): run the multi-tenant producer first",
			name, streamID, err)
	}
	return topic, nil
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

// consumerPermissions grant reading the tenant's own stream and polling the
// one topic it carries. read_stream is what the server checks when a user asks
// for a stream, so a topic-only grant would hide the tenant's own stream.
func consumerPermissions(streamID, topicID uint32) *iggcon.Permissions {
	return &iggcon.Permissions{
		Streams: map[int]*iggcon.StreamPermissions{
			int(streamID): {
				ReadStream: true,
				Topics: map[int]*iggcon.TopicPermissions{
					int(topicID): {
						ReadTopic:    true,
						PollMessages: true,
					},
				},
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
