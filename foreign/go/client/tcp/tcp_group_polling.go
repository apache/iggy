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
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// assignmentRefreshInterval is how long a cached assignment is trusted before
// the client asks the coordinator again. A rebalance the client missed shows
// up as a fenced poll long before this expires, so it only bounds the drift of
// an idle member.
const assignmentRefreshInterval = 5 * time.Second

// groupPollAttempts bounds how many times one poll re-syncs its assignment
// before giving up. A rebalance in flight resolves within one re-sync.
const groupPollAttempts = 2

// groupKey identifies a cached assignment by the encoded identifiers it was
// fetched for, so a numeric and a named reference to the same group stay
// distinct entries rather than aliasing.
type groupKey struct {
	stream string
	topic  string
	group  string
}

// groupAssignment is one member's view of the partitions it owns.
type groupAssignment struct {
	generation uint64
	partitions []uint32
	fetchedAt  time.Time
	// cursor is the round-robin position of the next partition to poll.
	cursor int
}

// groupAssignmentCache holds the assignments this client polls with. It is
// guarded by its own lock rather than the connection lock, because a poll
// reads it around an exchange that already holds the connection.
type groupAssignmentCache struct {
	mtx     sync.Mutex
	entries map[groupKey]*groupAssignment
}

func (c *groupAssignmentCache) get(key groupKey) (groupAssignment, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	entry, ok := c.entries[key]
	if !ok {
		return groupAssignment{}, false
	}
	return *entry, true
}

func (c *groupAssignmentCache) put(key groupKey, assignment groupAssignment) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.entries == nil {
		c.entries = make(map[groupKey]*groupAssignment)
	}
	c.entries[key] = &assignment
}

// advance moves the round-robin cursor of a cached entry past the partition
// that was just polled.
func (c *groupAssignmentCache) advance(key groupKey, cursor int) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if entry, ok := c.entries[key]; ok {
		entry.cursor = cursor
	}
}

func (c *groupAssignmentCache) drop(key groupKey) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.entries, key)
}

// clear forgets every assignment. A new connection registers a new client
// identity, so nothing the server assigned to the old one still holds.
func (c *groupAssignmentCache) clear() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	clear(c.entries)
}

// newGroupKey builds the cache key from the encoded identifiers.
func newGroupKey(streamId, topicId, groupId iggcon.Identifier) (groupKey, error) {
	stream, err := streamId.MarshalBinary()
	if err != nil {
		return groupKey{}, err
	}
	topic, err := topicId.MarshalBinary()
	if err != nil {
		return groupKey{}, err
	}
	group, err := groupId.MarshalBinary()
	if err != nil {
		return groupKey{}, err
	}
	return groupKey{stream: string(stream), topic: string(topic), group: string(group)}, nil
}

// pollGroup polls a consumer group that named no explicit partition. The
// broker does not pick a partition under consensus, so the client fetches its
// assignment and polls the partitions it owns in turn.
func (c *IggyTcpClient) pollGroup(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	consumer iggcon.Consumer,
	strategy iggcon.PollingStrategy,
	count uint32,
	autoCommit bool,
) (*iggcon.PolledMessage, error) {
	key, err := newGroupKey(streamId, topicId, consumer.Id)
	if err != nil {
		return nil, err
	}

	for range groupPollAttempts {
		assignment, err := c.ensureAssignment(ctx, key, streamId, topicId, consumer.Id)
		if err != nil {
			return nil, err
		}
		if len(assignment.partitions) == 0 {
			return &iggcon.PolledMessage{
				PartitionId: iggcon.NoAssignedPartition,
				Messages:    []iggcon.IggyMessage{},
			}, nil
		}

		cursor := assignment.cursor % len(assignment.partitions)
		partitionId := assignment.partitions[cursor]
		c.groups.advance(key, (cursor+1)%len(assignment.partitions))

		polled, err := c.pollPartition(
			ctx, streamId, topicId, consumer, strategy, count, autoCommit, &partitionId)
		if err != nil {
			if !isAssignmentStale(err) {
				return nil, err
			}
			c.groups.drop(key)
			continue
		}

		// The server marks a stale assignment by answering an empty batch on
		// the resync sentinel partition.
		if polled.PartitionId == iggcon.ResyncRequiredPartition && polled.MessageCount == 0 {
			c.groups.drop(key)
			continue
		}
		return polled, nil
	}

	return nil, ierror.ErrConsumerGroupPartitionNotOwned
}

// isAssignmentStale reports whether the error means the cached assignment no
// longer matches the group's generation.
func isAssignmentStale(err error) bool {
	return errors.Is(err, ierror.ErrConsumerGroupMemberNotFound) ||
		errors.Is(err, ierror.ErrConsumerGroupPartitionNotOwned)
}

// ensureAssignment returns a fresh enough assignment, joining the group first
// when the client turns out not to be a member.
func (c *IggyTcpClient) ensureAssignment(
	ctx context.Context,
	key groupKey,
	streamId, topicId, groupId iggcon.Identifier,
) (groupAssignment, error) {
	if cached, ok := c.groups.get(key); ok &&
		time.Since(cached.fetchedAt) < assignmentRefreshInterval {
		return cached, nil
	}

	synced, err := c.SyncConsumerGroup(ctx, streamId, topicId, groupId)
	if err != nil {
		return groupAssignment{}, err
	}
	if synced == nil {
		if err := c.JoinConsumerGroup(ctx, streamId, topicId, groupId); err != nil {
			return groupAssignment{}, err
		}
		synced, err = c.SyncConsumerGroup(ctx, streamId, topicId, groupId)
		if err != nil {
			return groupAssignment{}, err
		}
		if synced == nil {
			return groupAssignment{}, ierror.ErrConsumerGroupMemberNotFound
		}
	}

	assignment := groupAssignment{
		generation: synced.Generation,
		partitions: synced.Partitions,
		fetchedAt:  time.Now(),
	}
	c.logger.Debug("Synced the consumer group assignment",
		slog.Uint64("generation", assignment.generation),
		slog.Int("partitions", len(assignment.partitions)))
	c.groups.put(key, assignment)
	return assignment, nil
}
