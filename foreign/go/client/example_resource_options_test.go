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

package client_test

import (
	"context"
	"crypto/rand"
	"log"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// The README's resource options sample, kept here so it compiles.
func Example_resourceOptions() {
	ctx := context.Background()
	cli, err := client.NewIggyClient(client.WithTcp(tcp.WithServerAddress("127.0.0.1:8090")))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = cli.Close() }()
	if err := cli.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	if _, err := cli.LoginUser(ctx, "iggy", "iggy"); err != nil {
		log.Fatal(err)
	}

	key := make([]byte, iggcon.StreamEncryptionKeyLength)
	if _, err := rand.Read(key); err != nil {
		log.Fatal(err)
	}
	stream, err := cli.CreateStream(ctx, "orders", &iggcon.StreamOptions{EncryptionKey: key})
	if err != nil {
		log.Fatal(err)
	}
	source, _ := stream.EncryptionKeySource()
	fingerprint, _ := stream.EncryptionKeyFingerprint()
	log.Printf("key source %s, fingerprint %s", source, fingerprint)

	streamId, _ := iggcon.NewIdentifier(stream.Id)
	partitions := uint32(3)
	expiry := iggcon.Duration(time.Hour.Microseconds())
	durability := iggcon.DurabilityPersisted
	topic, err := cli.CreateTopic(ctx, streamId, "events", &iggcon.TopicOptions{
		PartitionsCount: &partitions,
		MessageExpiry:   &expiry,
		Durability:      &durability,
	})
	if err != nil {
		log.Fatal(err)
	}

	topicId, _ := iggcon.NewIdentifier(topic.Id)
	size := uint64(10 << 30)
	if err := cli.UpdateTopic(ctx, streamId, topicId, iggcon.TopicUpdate{
		MaxTopicSize: &size,
		Reset:        []iggcon.TopicOptionKey{iggcon.TopicOptionMessageExpiry},
	}); err != nil {
		log.Fatal(err)
	}

	specs, err := cli.DescribeOptions(ctx, iggcon.OptionsScopeTopic)
	if err != nil {
		log.Fatal(err)
	}
	for _, spec := range specs {
		log.Printf("%s updatable=%t resettable=%t", spec.Key, spec.Updatable, spec.Resettable)
	}
}
