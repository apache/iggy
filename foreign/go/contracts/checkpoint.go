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

package iggcon

// Checkpoint is the position after one delivered message: its partition, its
// offset and the history of the partition it came from. Only a poll or a
// reader makes one. Storing it stores the offset of that message.
type Checkpoint struct {
	partitionID uint32
	offset      uint64
	context     PartitionContext
}

// PartitionID is the partition of the message.
func (c Checkpoint) PartitionID() uint32 { return c.partitionID }

// Offset is the offset of the message.
func (c Checkpoint) Offset() uint64 { return c.offset }

// Context is the history of the partition that the message came from, which a
// store sends so that it fails on another one.
func (c Checkpoint) Context() PartitionContext { return c.context }

// Next is the continuation after the message, bound to its partition.
func (c Checkpoint) Next() PollingStrategy {
	partition := c.partitionID
	context := c.context
	return PollingStrategy{Kind: POLLING_OFFSET, Value: c.offset + 1, Context: &context, partition: &partition}
}

// Checkpoint is the position after the last message, or false for an empty batch.
func (p *PolledMessage) Checkpoint() (Checkpoint, bool) {
	return p.CheckpointAt(len(p.Messages) - 1)
}

// CheckpointAt is the position after the message at index, or false for an
// index outside the batch.
func (p *PolledMessage) CheckpointAt(index int) (Checkpoint, bool) {
	if index < 0 || index >= len(p.Messages) {
		return Checkpoint{}, false
	}
	return Checkpoint{partitionID: p.PartitionId, offset: p.Messages[index].Header.Offset, context: p.Context}, true
}

// Continuation is where the next poll of this partition starts, after the
// last message, or false for an empty batch, whose start the caller keeps. A
// poll or a seek of another partition refuses it.
func (p *PolledMessage) Continuation() (PollingStrategy, bool) {
	checkpoint, ok := p.Checkpoint()
	if !ok {
		return PollingStrategy{}, false
	}
	return checkpoint.Next(), true
}
