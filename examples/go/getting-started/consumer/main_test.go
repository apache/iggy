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
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestConsumeMessagesAdvancesPastRetainedMessages(t *testing.T) {
	originalLimit := BatchesLimit
	BatchesLimit = 2
	t.Cleanup(func() { BatchesLimit = originalLimit })

	for _, firstOffset := range []uint64{0, 25} {
		t.Run(strconv.FormatUint(firstOffset, 10), func(t *testing.T) {
			const retainedCount = 20
			client := &retainedMessagesClient{}
			expected := make([]uint64, retainedCount)
			for index := range expected {
				offset := firstOffset + uint64(index)
				expected[index] = offset
				client.messages = append(client.messages, iggcon.IggyMessage{
					Header:  iggcon.MessageHeader{Offset: offset},
					Payload: []byte("retained message"),
				})
			}

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := consumeMessages(ctx, client); err != nil {
				t.Fatalf("consume retained messages: %v", err)
			}
			if !slices.Equal(client.polledOffsets, expected) {
				t.Fatalf("consumer repeated or skipped retained messages: got offsets %v, want %v",
					client.polledOffsets, expected)
			}
		})
	}
}

type retainedMessagesClient struct {
	iggcon.Client
	messages      []iggcon.IggyMessage
	polledOffsets []uint64
}

func (c *retainedMessagesClient) PollMessages(
	_ context.Context,
	_, _ iggcon.Identifier,
	_ iggcon.Consumer,
	strategy iggcon.PollingStrategy,
	count uint32,
	_ bool,
	_ *uint32,
) (*iggcon.PolledMessage, error) {
	result := &iggcon.PolledMessage{}
	for _, message := range c.messages {
		if message.Header.Offset < strategy.Value {
			continue
		}
		result.Messages = append(result.Messages, message)
		c.polledOffsets = append(c.polledOffsets, message.Header.Offset)
		if len(result.Messages) == int(count) {
			break
		}
	}
	return result, nil
}
