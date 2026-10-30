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

import (
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/batch"
)

const (
	// MaxDeferredPollWait is the longest wait that the protocol carries. A
	// server can admit less.
	MaxDeferredPollWait = 600 * time.Second
	// MaxDeferredPollTimeout leaves 30 seconds after the longest wait for the
	// final read and the reply.
	MaxDeferredPollTimeout = MaxDeferredPollWait + 30*time.Second
	// MinDeferredPollReplyMargin is how much a request timeout must exceed a
	// positive wait. The server needs it for the final read and the reply.
	MinDeferredPollReplyMargin = time.Second
	// MinDeferredPollMaxBytes holds the reply prefix (partition id, current
	// offset, message count and partition context) and one batch of one
	// empty message, the least room that a message needs.
	MinDeferredPollMaxBytes = 16 + PartitionContextSize + batch.HeaderSize + batch.MessageHeaderSize
	// MaxPartitionsPerRequest is the most partitions that one fetch names.
	MaxPartitionsPerRequest = 1000
)

// DeferredPollOptions are the limits of a deferred poll and a fetch. The server holds a deferred poll until MinCount messages are ready,
// until MaxBytes stops it from selecting more, or until MaxWait passes. Then
// it reads what it has, possibly nothing. RequestTimeout bounds the whole
// call with its retries, and a call that reaches it fails instead of
// returning an empty reply.
type DeferredPollOptions struct {
	MaxWait  time.Duration
	MinCount uint32
	// MaxBytes bounds the reply body, its prefix and batch framing included.
	MaxBytes       uint32
	RequestTimeout time.Duration
}

// DefaultDeferredPollOptions wait up to one second for one message, read up
// to 1 MiB and give the call 11 seconds.
func DefaultDeferredPollOptions() DeferredPollOptions {
	return DeferredPollOptions{
		MaxWait:        time.Second,
		MinCount:       1,
		MaxBytes:       1024 * 1024,
		RequestTimeout: 11 * time.Second,
	}
}

// Validate refuses the options that the server refuses, before anything is
// sent. count is the message count of the poll.
func (o DeferredPollOptions) Validate(count uint32) error {
	if o.MinCount == 0 || o.MinCount > count {
		return ierror.ErrInvalidMessagesCount
	}
	if err := o.ValidateWait(); err != nil {
		return err
	}
	if o.MaxBytes < MinDeferredPollMaxBytes {
		return ierror.ErrInvalidSizeBytes
	}
	return nil
}

// ValidateFetch checks the initial limits of a fetch. Even a zero-wait fetch
// needs one second for the server's read and reply, so the initial timeout must
// exceed it so that local setup can consume time without extending the call.
func (o DeferredPollOptions) ValidateFetch(count uint32) error {
	if err := o.Validate(count); err != nil {
		return err
	}
	if o.RequestTimeout.Truncate(time.Microsecond) <= MinDeferredPollReplyMargin {
		return ierror.ErrInvalidCommand
	}
	return nil
}

// ValidateWait refuses a MaxWait and a RequestTimeout that the server
// refuses. Other limits are checked by Validate and ValidateFetch.
func (o DeferredPollOptions) ValidateWait() error {
	// The server checks the reply margin on the microseconds that the wire carries.
	wait, timeout := o.MaxWait.Truncate(time.Microsecond), o.RequestTimeout.Truncate(time.Microsecond)
	if o.MaxWait < 0 || o.MaxWait > MaxDeferredPollWait ||
		o.RequestTimeout < time.Microsecond || o.RequestTimeout > MaxDeferredPollTimeout ||
		wait > 0 && timeout < wait+MinDeferredPollReplyMargin {
		return ierror.ErrInvalidCommand
	}
	return nil
}

// Remaining is what is left of the options after elapsed, so that the
// retries and the steps of one call share one budget. MaxWait and
// RequestTimeout shrink by elapsed rounded up to whole microseconds, and
// MaxWait stops at zero. The wire carries whole microseconds, so both lose
// the same number of them, and the reply margin that ValidateWait checks
// holds. Once the budget is spent, Remaining returns
// ierror.ErrTransientNotAccepted, because a request that is not sent is not
// accepted.
func (o DeferredPollOptions) Remaining(elapsed time.Duration) (DeferredPollOptions, error) {
	elapsed = (elapsed + time.Microsecond - time.Nanosecond).Truncate(time.Microsecond)
	o.RequestTimeout -= elapsed
	if o.RequestTimeout < time.Microsecond {
		return DeferredPollOptions{}, ierror.ErrTransientNotAccepted
	}
	o.MaxWait = max(o.MaxWait-elapsed, 0)
	return o, nil
}

// FetchMessagesTarget is one partition of a fetch and the position that it
// reads from, which must be Next or Offset. A Strategy.Context fences the
// read to that incarnation and owner of the partition. Without one, the
// fetch takes the incarnation from the route of the partition.
type FetchMessagesTarget struct {
	PartitionId uint32
	Strategy    PollingStrategy
}

// FetchMessagesSection is the outcome of one target of a fetch. Err is the
// refusal of its partition, such as ierror.ErrNotPrimary after a move, and a
// refused section carries no position and no records: the target keeps the
// position that it was sent with. The records of a served section stay
// encoded, and binaryserialization.DeserializeSectionMessages decodes them.
// They alias the reply buffer.
type FetchMessagesSection struct {
	PartitionId   uint32
	Err           error
	CurrentOffset uint64
	MessageCount  uint32
	// Context is the partition history that served the section. The next
	// fetch of the partition fences its target with it.
	Context PartitionContext
	// NextOffset is where the next fetch of the partition starts, also after
	// an empty section.
	NextOffset uint64
	Records    []byte
	// Compression is the message compression of the client that fetched the
	// section, which the decode of Records undoes.
	Compression IggyMessageCompression
}
