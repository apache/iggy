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

// Reader-consumer reads the messages of the getting started producer as a
// member of a consumer group. The consumer waits on the server for new
// messages, and stores the offset of each message after its handler processed it.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"time"

	"github.com/apache/iggy/examples/go/common"
	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/reader"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

const (
	groupName     = "sample-readers"
	messagesLimit = 100
)

func main() {
	address := flag.String("tcp-server-address", "127.0.0.1:8090", "the TCP address of the server")
	flag.Parse()
	cli, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress(*address)))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := cli.Close(); err != nil {
			log.Printf("Error closing client: %v", err)
		}
	}()
	ctx := context.Background()
	if err := cli.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	if _, err := cli.LoginUser(ctx, common.DefaultRootUsername, common.DefaultRootPassword); err != nil {
		log.Fatal(err)
	}
	if err := consume(ctx, cli); err != nil {
		log.Fatal(err)
	}
}

func consume(ctx context.Context, cli iggcon.Client) error {
	stream, _ := iggcon.NewIdentifier(uint32(0))
	topic, _ := iggcon.NewIdentifier(uint32(0))
	group, _ := iggcon.NewIdentifier(groupName)
	if _, err := cli.GetConsumerGroup(ctx, stream, topic, group); err != nil {
		if _, err := cli.CreateConsumerGroup(ctx, stream, topic, groupName); err != nil {
			return err
		}
	}
	// Every 10,000 messages or 5 s, a message counts once its handler succeeded.
	options := iggcon.WaitPollOptions(5 * time.Second)
	options.Commit = iggcon.AutoCommitPolicy(iggcon.DefaultAutoCommit())
	consumer, err := reader.NewIggyConsumer(ctx, cli, stream, topic, iggcon.NewGroupConsumer(group),
		reader.WithPollOptions(options))
	if err != nil {
		return err
	}
	running, stop := context.WithCancel(ctx)
	defer stop()
	handled := 0
	consumed := consumer.Consume(running, func(_ context.Context, message reader.ReceivedMessage) error {
		log.Printf("Partition %d offset %d: %s", message.PartitionID, message.Checkpoint.Offset(), message.Message.Payload)
		handled++
		if handled == messagesLimit {
			stop()
		}
		return nil
	})
	// Counts the last message, stores the counted offsets and leaves the group.
	if err := consumer.Shutdown(ctx); err != nil {
		return err
	}
	if errors.Is(consumed, context.Canceled) {
		return nil
	}
	return consumed
}
