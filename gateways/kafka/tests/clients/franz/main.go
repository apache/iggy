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

// Command franz-probe is the client matrix probe for franz-go, at its defaults (idempotent
// producer, classic or KIP-848 groups as the broker offers). It speaks the probe contract in
// ../README.md: ACK, NACK, REC, COMMITTED and DONE lines on stdout.
package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	bootstrap = os.Getenv("BOOTSTRAP")
	topic     = os.Getenv("TOPIC")
	group     = os.Getenv("GROUP")
	count     = envInt("COUNT", 12)
	deadline  = time.Duration(envInt("DEADLINE_S", 45)) * time.Second
)

func envInt(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return value
	}
	return fallback
}

func text(raw []byte) string {
	if raw == nil {
		return "-"
	}
	return string(raw)
}

func produce(phase string, batch int) {
	client, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.ClientID("matrix-franz"))
	if err != nil {
		fmt.Printf("NACK %s client\n", phase)
		return
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	first := (batch - 1) * count
	for index := first; index < first+count; index++ {
		record := &kgo.Record{
			Topic: topic,
			Key:   []byte(fmt.Sprintf("k%03d", index)),
			Value: []byte(fmt.Sprintf("v%03d", index)),
			Headers: []kgo.RecordHeader{
				{Key: "idx", Value: []byte(strconv.Itoa(index))},
				{Key: "src", Value: []byte("matrix")},
			},
		}
		client.Produce(ctx, record, func(record *kgo.Record, err error) {
			if err != nil {
				fmt.Printf("NACK %s %s\n", phase, strings.ReplaceAll(err.Error(), " ", "_"))
				return
			}
			fmt.Printf("ACK %s %d %d\n", phase, record.Partition, record.Offset)
		})
	}
	if err := client.Flush(ctx); err != nil {
		fmt.Printf("NACK %s flush\n", phase)
	}
}

func consume(phase string) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(bootstrap),
		kgo.ClientID("matrix-franz"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		fmt.Printf("NACK %s client\n", phase)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	seen := 0
	for seen < count && ctx.Err() == nil {
		fetches := client.PollRecords(ctx, count-seen)
		fetches.EachError(func(t string, p int32, err error) {
			fmt.Fprintf(os.Stderr, "%s: %s[%d]: %v\n", phase, t, p, err)
		})
		fetches.EachRecord(func(record *kgo.Record) {
			headers := make([]string, 0, len(record.Headers))
			for _, header := range record.Headers {
				headers = append(headers, header.Key+"="+text(header.Value))
			}
			joined := strings.Join(headers, ",")
			if joined == "" {
				joined = "-"
			}
			fmt.Printf("REC %s %d %d %s %s %s\n", phase, record.Partition, record.Offset,
				text(record.Key), text(record.Value), joined)
			seen++
		})
	}
	commitCtx, commitCancel := context.WithTimeout(context.Background(), deadline)
	defer commitCancel()
	if err := client.CommitUncommittedOffsets(commitCtx); err != nil {
		fmt.Printf("NACK %s.commit %s\n", phase, strings.ReplaceAll(err.Error(), " ", "_"))
	}
	committed(commitCtx, client, phase)
	client.LeaveGroup()
	client.Close()
}

func committed(ctx context.Context, client *kgo.Client, phase string) {
	admin := kadm.NewClient(client)
	offsets, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		fmt.Printf("NACK %s.committed %s\n", phase, strings.ReplaceAll(err.Error(), " ", "_"))
		return
	}
	partitions := []int{}
	byPartition := map[int]int64{}
	offsets.Each(func(offset kadm.OffsetResponse) {
		if offset.Topic == topic {
			partitions = append(partitions, int(offset.Partition))
			byPartition[int(offset.Partition)] = offset.At
		}
	})
	sort.Ints(partitions)
	for _, partition := range partitions {
		fmt.Printf("COMMITTED %s %d %d\n", phase, partition, byPartition[partition])
	}
}

func main() {
	produce("produce", 1)
	consume("consume")
	produce("produce2", 2)
	consume("resume")
	fmt.Println("DONE")
}
