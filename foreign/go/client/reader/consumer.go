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

package reader

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// ReceivedMessage is one message that an IggyConsumer handed over.
type ReceivedMessage struct {
	PartitionID uint32
	Message     iggcon.IggyMessage
	// Checkpoint is the position after the message, for a commit or a mark.
	Checkpoint iggcon.Checkpoint
	// EndsBatch marks the last message of the batch that the reader read.
	EndsBatch bool
}

// IggyConsumer hands over one message at a time from a PartitionReader, as a
// standalone consumer or a consumer group member. Under CommitAuto with
// CommitAfterProcessing a message counts when the next one is asked for or at
// Shutdown, and in Consume once its handler succeeds. A handler that fails
// leaves its message uncounted and ends Consume with its error. Under
// CommitAfterDelivery a message counts when it is handed over, and under
// CommitAfterMarked only Mark counts. One caller at a time calls Next or runs
// Consume.
type IggyConsumer struct {
	reader    *PartitionReader
	after     iggcon.CommitAfter
	auto      bool
	mu        sync.Mutex
	batch     *iggcon.PolledMessage
	index     int
	seeks     uint64
	uncounted *ReceivedMessage
	stopping  atomic.Bool
}

// NewIggyConsumer starts the reader of a consumer, which a group member joins
// its group for. It takes the options of NewPartitionReader.
func NewIggyConsumer(
	ctx context.Context,
	client iggcon.Client,
	streamId, topicId iggcon.Identifier,
	consumer iggcon.Consumer,
	options ...Option,
) (*IggyConsumer, error) {
	reader, err := NewPartitionReader(ctx, client, streamId, topicId, consumer, options...)
	if err != nil {
		return nil, err
	}
	c := &IggyConsumer{reader: reader}
	if reader.auto != nil {
		c.auto = true
		c.after = reader.auto.After
	}
	return c, nil
}

// Next returns the next message. It fails with a *ReadError for an error of a
// read, after which the consumer goes on.
func (c *IggyConsumer) Next(ctx context.Context) (ReceivedMessage, error) {
	c.mu.Lock()
	c.countUncountedLocked()
	c.mu.Unlock()
	message, err := c.take(ctx)
	if err == nil && c.auto && c.after == iggcon.CommitAfterProcessing {
		c.mu.Lock()
		c.uncounted = &message
		c.mu.Unlock()
	}
	return message, err
}

// Messages yields the messages until ctx ends, Shutdown, or an error, which it yields last.
func (c *IggyConsumer) Messages(ctx context.Context) iter.Seq2[ReceivedMessage, error] {
	return func(yield func(ReceivedMessage, error) bool) {
		for !c.stopping.Load() {
			message, err := c.Next(ctx)
			if !yield(message, err) || err != nil {
				return
			}
		}
	}
}

// Consume runs handler on each message, one after another, until ctx ends,
// Shutdown, or the first failure of a handler or a read, which it returns.
func (c *IggyConsumer) Consume(ctx context.Context, handler func(context.Context, ReceivedMessage) error) error {
	c.mu.Lock()
	c.countUncountedLocked()
	c.mu.Unlock()
	for !c.stopping.Load() {
		message, err := c.take(ctx)
		if err != nil {
			if c.stopping.Load() {
				return nil
			}
			return err
		}
		if err := handler(ctx, message); err != nil {
			return err
		}
		if c.auto && c.after == iggcon.CommitAfterProcessing {
			c.reader.countDelivered(message.Checkpoint, 1, message.EndsBatch)
		}
	}
	return nil
}

// Commit stores the offset of a checkpoint that the consumer handed over, see PartitionReader.Commit.
func (c *IggyConsumer) Commit(ctx context.Context, checkpoint iggcon.Checkpoint) error {
	return c.reader.Commit(ctx, checkpoint)
}

// Mark counts a checkpoint that the consumer handed over, see PartitionReader.Mark.
func (c *IggyConsumer) Mark(checkpoint iggcon.Checkpoint) error {
	return c.reader.Mark(checkpoint)
}

// Seek sets the next position of a partition, see PartitionReader.Seek. The
// messages of the partition that the consumer still holds are dropped.
func (c *IggyConsumer) Seek(ctx context.Context, partitionID uint32, start iggcon.PollingStrategy) error {
	c.mu.Lock()
	if c.batch != nil && c.batch.PartitionId == partitionID {
		c.batch = nil
	}
	if c.uncounted != nil && c.uncounted.PartitionID == partitionID {
		c.uncounted = nil
	}
	c.seeks++
	c.mu.Unlock()
	return c.reader.Seek(ctx, partitionID, start)
}

// Partitions are the partitions of the reader.
func (c *IggyConsumer) Partitions() []uint32 {
	return c.reader.Partitions()
}

// Shutdown stops a running Consume after its current handler, counts the last
// message under CommitAfterProcessing, and closes the reader, which stores,
// moves offsets back and leaves its group as its commit policy says.
func (c *IggyConsumer) Shutdown(ctx context.Context) error {
	c.stopping.Store(true)
	c.mu.Lock()
	c.countUncountedLocked()
	c.mu.Unlock()
	return c.reader.Close(ctx)
}

func (c *IggyConsumer) take(ctx context.Context) (ReceivedMessage, error) {
	for {
		if c.stopping.Load() {
			return ReceivedMessage{}, ierror.ErrClientShutdown
		}
		c.mu.Lock()
		if c.batch != nil && c.index < len(c.batch.Messages) {
			message := c.handOverLocked()
			c.mu.Unlock()
			return message, nil
		}
		seeks := c.seeks
		c.mu.Unlock()
		batch, err := c.reader.next(ctx, false)
		if err != nil {
			return ReceivedMessage{}, err
		}
		c.mu.Lock()
		if seeks == c.seeks {
			c.batch = batch
			c.index = 0
		}
		c.mu.Unlock()
	}
}

func (c *IggyConsumer) handOverLocked() ReceivedMessage {
	at := c.index
	c.index++
	checkpoint, _ := c.batch.CheckpointAt(at)
	message := ReceivedMessage{
		PartitionID: c.batch.PartitionId,
		Message:     c.batch.Messages[at],
		Checkpoint:  checkpoint,
		EndsBatch:   at == len(c.batch.Messages)-1,
	}
	c.reader.deliverMessage(checkpoint)
	if c.auto && c.after == iggcon.CommitAfterDelivery {
		c.reader.countDelivered(checkpoint, 1, message.EndsBatch)
	}
	return message
}

func (c *IggyConsumer) countUncountedLocked() {
	if c.uncounted != nil {
		message := c.uncounted
		c.uncounted = nil
		c.reader.countDelivered(message.Checkpoint, 1, message.EndsBatch)
	}
}
