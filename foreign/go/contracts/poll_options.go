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
	// DefaultPollCount is the most messages of one batch unless the options say otherwise.
	DefaultPollCount = 1000
	// PollRequestTimeoutMargin is how much longer than its wait a poll may take.
	PollRequestTimeoutMargin = 10 * time.Second
	// MaxRecordSize is the largest record of one message in a read reply, with
	// its batch and message headers. A read returns a first record up to this
	// size even above its MaxBytes.
	MaxRecordSize             = batch.HeaderSize + batch.MessageHeaderSize + 64_000_000 + 100_000
	defaultAutoCommitInterval = 5 * time.Second
	defaultAutoCommitMessages = 10_000
)

// CommitPolicyKind says who stores the offsets of what a read returns.
type CommitPolicyKind uint8

const (
	// CommitManual stores nothing. The application commits checkpoints.
	CommitManual CommitPolicyKind = iota
	// CommitOnRead makes the server store the offset of the last message of
	// each read as part of the read: one round trip, at most once.
	CommitOnRead
	// CommitAuto makes the client store the latest counted position, at
	// least once with CommitAfterProcessing. A poll of one partition refuses
	// it, because it has nothing to count. A reader and a consumer accept it.
	CommitAuto
)

// CommitAfter is when a message counts under CommitAuto.
type CommitAfter uint8

const (
	// CommitAfterProcessing counts a message once the application asks for
	// the next one, returns from its handler with success, or closes the
	// reader. A handler that fails leaves its message uncounted.
	CommitAfterProcessing CommitAfter = iota
	// CommitAfterDelivery counts a message when it is handed over.
	CommitAfterDelivery
	// CommitAfterMarked counts only marked checkpoints, so messages processed
	// concurrently count once they are done.
	CommitAfterMarked
)

// CommitEveryKind is the event on which CommitAuto also stores.
type CommitEveryKind uint8

const (
	// CommitEveryNever stores only at the interval and on close.
	CommitEveryNever CommitEveryKind = iota
	// CommitEveryMessage stores on each counted message, or each counted batch of a reader.
	CommitEveryMessage
	// CommitEveryMessages stores once Messages messages of a partition counted since its last store.
	CommitEveryMessages
	// CommitEveryBatch stores on the last message of each batch.
	CommitEveryBatch
)

// CommitEvery is the event of AutoCommit.
type CommitEvery struct {
	Kind     CommitEveryKind
	Messages uint32
}

// AutoCommit says when CommitAuto stores. It needs an Interval or an Every
// event. It also stores on close, on shutdown and before a rebalance takes a
// partition away. A store only moves forward, and a position of an older
// generation of its partition, after a seek or a new assignment, is dropped.
type AutoCommit struct {
	// Interval stores the latest counted position this often, when it moved. Zero is none.
	Interval time.Duration
	Every    CommitEvery
	After    CommitAfter
}

// DefaultAutoCommit stores every 10,000 messages or 5 s, whichever comes
// first, after CommitAfterProcessing.
func DefaultAutoCommit() AutoCommit {
	return AutoCommit{
		Interval: defaultAutoCommitInterval,
		Every:    CommitEvery{Kind: CommitEveryMessages, Messages: defaultAutoCommitMessages},
		After:    CommitAfterProcessing,
	}
}

// CommitPolicy is who stores the offsets of what a read returns. Auto holds
// the settings of CommitAuto.
type CommitPolicy struct {
	Kind CommitPolicyKind
	Auto AutoCommit
}

// ManualCommit stores nothing, the default.
func ManualCommit() CommitPolicy { return CommitPolicy{Kind: CommitManual} }

// OnReadCommit makes the server store the offset of each read.
func OnReadCommit() CommitPolicy { return CommitPolicy{Kind: CommitOnRead} }

// AutoCommitPolicy makes the client store counted positions as auto says.
func AutoCommitPolicy(auto AutoCommit) CommitPolicy {
	return CommitPolicy{Kind: CommitAuto, Auto: auto}
}

// PollOptions are the limits of one poll of one partition and who stores its
// offsets. MaxWait zero answers at once. A first message larger than MaxBytes
// comes alone in the reply, up to MaxRecordSize.
type PollOptions struct {
	// Count is the most messages of one batch, at least 1.
	Count uint32
	// MaxWait is at most MaxDeferredPollWait.
	MaxWait time.Duration
	// MinCount is 1 to Count.
	MinCount uint32
	// MaxBytes is at least MinDeferredPollMaxBytes.
	MaxBytes uint32
	Commit   CommitPolicy
}

// DefaultPollOptions read up to 1000 messages and 1 MiB at once and store nothing.
func DefaultPollOptions() PollOptions {
	return PollOptions{Count: DefaultPollCount, MinCount: 1, MaxBytes: DefaultDeferredPollOptions().MaxBytes}
}

// WaitPollOptions are the defaults with a wait of up to maxWait.
func WaitPollOptions(maxWait time.Duration) PollOptions {
	options := DefaultPollOptions()
	options.MaxWait = maxWait
	return options
}

// RequestTimeout is MaxWait plus PollRequestTimeoutMargin.
func (o PollOptions) RequestTimeout() time.Duration {
	return o.MaxWait + PollRequestTimeoutMargin
}

// Deferred are the wire limits of the options.
func (o PollOptions) Deferred() DeferredPollOptions {
	return DeferredPollOptions{MaxWait: o.MaxWait, MinCount: o.MinCount, MaxBytes: o.MaxBytes, RequestTimeout: o.RequestTimeout()}
}

// Validate refuses what the server refuses, and an automatic commit without
// an interval or an event, which would store only on close.
func (o PollOptions) Validate() error {
	if o.Commit.Kind == CommitAuto {
		auto := o.Commit.Auto
		if (auto.Interval <= 0 && auto.Every.Kind == CommitEveryNever) ||
			(auto.Every.Kind == CommitEveryMessages && auto.Every.Messages == 0) {
			return ierror.ErrInvalidConfiguration
		}
	}
	return o.Deferred().Validate(o.Count)
}
