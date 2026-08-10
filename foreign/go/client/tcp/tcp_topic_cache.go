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

package tcp

import (
	"sync"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// topicKey identifies a topic by the encoded identifiers a caller used, so a
// numeric and a named reference to the same topic stay distinct entries.
type topicKey struct {
	stream string
	topic  string
}

func newTopicKey(streamId, topicId iggcon.Identifier) topicKey {
	stream, _ := streamId.MarshalBinary()
	topic, _ := topicId.MarshalBinary()
	return topicKey{stream: string(stream), topic: string(topic)}
}

// encodedStream is the stream half of a topicKey.
func encodedStream(streamId iggcon.Identifier) string {
	stream, _ := streamId.MarshalBinary()
	return string(stream)
}

// topicCache holds what a send needs to pick a partition without a metadata
// round trip per batch.
type topicCache struct {
	mtx              sync.Mutex
	partitionsCounts map[topicKey]uint32
	balancedCursors  map[topicKey]uint32
}

func (c *topicCache) partitionsCount(key topicKey) (uint32, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	count, ok := c.partitionsCounts[key]
	return count, ok
}

func (c *topicCache) setPartitionsCount(key topicKey, count uint32) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.partitionsCounts == nil {
		c.partitionsCounts = make(map[topicKey]uint32)
	}
	c.partitionsCounts[key] = count
}

func (c *topicCache) invalidatePartitionsCount(key topicKey) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.partitionsCounts, key)
}

func (c *topicCache) drop(key topicKey) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.partitionsCounts, key)
	delete(c.balancedCursors, key)
}

// dropStream forgets every topic cached under the encoded stream identifier,
// because deleting a stream invalidates each topic under it.
func (c *topicCache) dropStream(stream string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for key := range c.partitionsCounts {
		if key.stream == stream {
			delete(c.partitionsCounts, key)
		}
	}
	for key := range c.balancedCursors {
		if key.stream == stream {
			delete(c.balancedCursors, key)
		}
	}
}

// nextBalanced returns the partition a balanced send targets and advances the
// round-robin cursor of the topic.
func (c *topicCache) nextBalanced(key topicKey, partitionsCount uint32) uint32 {
	if partitionsCount == 0 {
		return 0
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.balancedCursors == nil {
		c.balancedCursors = make(map[topicKey]uint32)
	}
	partition := c.balancedCursors[key] % partitionsCount
	c.balancedCursors[key]++
	return partition
}

// clearCounts forgets the partition counts so a new session rereads current
// metadata. Balanced cursors survive on purpose: they are client-side
// fairness state with no dependency on cluster metadata, and resetting them
// on a reconnect would point every producer's first post-failover batch at
// partition 0 at once.
func (c *topicCache) clearCounts() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	clear(c.partitionsCounts)
}
