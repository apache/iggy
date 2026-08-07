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
// up as a fenced poll long before this expires. The periodic re-sync catches
// what fencing cannot, like a partition added to an assignment that stays
// valid; a same-generation re-sync keeps the round-robin position.
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
	// left records the groups the caller explicitly left, so a later poll
	// does not join them back implicitly. An explicit join clears the mark.
	// The mark survives clear, because leaving is caller intent rather than
	// connection state.
	left map[groupKey]struct{}
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

// nextPartition returns the partition the next poll targets and advances the
// round-robin cursor, both under one lock. A separate read and advance would
// let two concurrent polls target the same partition and skip another.
func (c *groupAssignmentCache) nextPartition(key groupKey) (uint32, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	entry, ok := c.entries[key]
	if !ok || len(entry.partitions) == 0 {
		return 0, false
	}
	cursor := entry.cursor % len(entry.partitions)
	entry.cursor = (cursor + 1) % len(entry.partitions)
	return entry.partitions[cursor], true
}

// putSynced stores a fresh sync result, carrying the round-robin cursor
// forward when the generation is unchanged, all under one lock so a
// concurrent poll's advance is never overwritten with a stale value.
func (c *groupAssignmentCache) putSynced(key groupKey, assignment groupAssignment) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.entries == nil {
		c.entries = make(map[groupKey]*groupAssignment)
	}
	if current, ok := c.entries[key]; ok && current.generation == assignment.generation {
		// A same-generation refresh is not a rebalance: the member still owns
		// the same partitions, so the round-robin position carries over
		// instead of resetting and starving the partitions behind it.
		assignment.cursor = current.cursor
	}
	c.entries[key] = &assignment
}

func (c *groupAssignmentCache) drop(key groupKey) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.entries, key)
}

// markJoined drops the cached assignment after an explicit join and clears
// the left mark, so polling resumes with a fresh sync.
func (c *groupAssignmentCache) markJoined(key groupKey) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.entries, key)
	delete(c.left, key)
}

// markLeft drops the cached assignment after an explicit leave or a group
// deletion and records that the caller left on purpose.
func (c *groupAssignmentCache) markLeft(key groupKey) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	delete(c.entries, key)
	if c.left == nil {
		c.left = make(map[groupKey]struct{})
	}
	c.left[key] = struct{}{}
}

// hasLeft reports whether the caller explicitly left the group.
func (c *groupAssignmentCache) hasLeft(key groupKey) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	_, ok := c.left[key]
	return ok
}

// clear forgets every assignment. A new connection registers a new client
// identity, so nothing the server assigned to the old one still holds.
func (c *groupAssignmentCache) clear() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	clear(c.entries)
}

// newGroupKey builds the cache key from the encoded identifiers.
func newGroupKey(streamId, topicId, groupId iggcon.Identifier) groupKey {
	stream, _ := streamId.MarshalBinary()
	topic, _ := topicId.MarshalBinary()
	group, _ := groupId.MarshalBinary()
	return groupKey{stream: string(stream), topic: string(topic), group: string(group)}
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
	key := newGroupKey(streamId, topicId, consumer.Id)

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

		partitionId, owned := c.groups.nextPartition(key)
		if !owned {
			// The entry vanished between the sync and this read; re-sync.
			continue
		}

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
		// the resync sentinel partition. The gate reads the decoded batch, not
		// the server-declared count, which is the same signal Rust checks.
		if polled.PartitionId == iggcon.ResyncRequiredPartition && len(polled.Messages) == 0 {
			c.groups.drop(key)
			continue
		}
		return polled, nil
	}

	// The attempt budget ran out mid-rebalance. A rebalance is not a failure:
	// report an empty poll like a member that owns nothing yet, so a consumer
	// that does not special-case rebalances just polls again.
	return &iggcon.PolledMessage{
		PartitionId: iggcon.NoAssignedPartition,
		Messages:    []iggcon.IggyMessage{},
	}, nil
}

// isAssignmentStale reports whether the error means the cached assignment no
// longer matches the group's generation.
func isAssignmentStale(err error) bool {
	return errors.Is(err, ierror.ErrConsumerGroupMemberNotFound) ||
		errors.Is(err, ierror.ErrConsumerGroupPartitionNotOwned)
}

// ensureAssignment returns a fresh enough assignment, joining the group first
// when a client that never left turns out not to be a member.
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
	if errors.Is(err, ierror.ErrConsumerGroupMemberNotFound) && !c.groups.hasLeft(key) {
		// The first poll may run before the caller joined, so the membership
		// is established on its behalf. A group the caller explicitly left
		// stays left, and the poll reports the membership error instead.
		if err = c.JoinConsumerGroup(ctx, streamId, topicId, groupId); err != nil {
			return groupAssignment{}, err
		}
		synced, err = c.SyncConsumerGroup(ctx, streamId, topicId, groupId)
	}
	if err != nil {
		return groupAssignment{}, err
	}

	assignment := groupAssignment{
		generation: synced.Generation,
		partitions: synced.Partitions,
		fetchedAt:  time.Now(),
	}
	c.logger.Debug("Synced the consumer group assignment",
		slog.Uint64("generation", assignment.generation),
		slog.Int("partitions", len(assignment.partitions)))
	c.groups.putSynced(key, assignment)
	return assignment, nil
}
