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
	"testing"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPollOptions_DefaultsAnswerAtOnceAndStoreNothing(t *testing.T) {
	options := DefaultPollOptions()

	assert.Equal(t, PollOptions{Count: 1000, MinCount: 1, MaxBytes: 1 << 20, Commit: ManualCommit()}, options)
	assert.Equal(t, 10*time.Second, options.RequestTimeout())
	require.NoError(t, options.Validate())
	assert.Equal(t, AutoCommit{Interval: 5 * time.Second, Every: CommitEvery{Kind: CommitEveryMessages, Messages: 10_000},
		After: CommitAfterProcessing}, DefaultAutoCommit())
}

func TestPollOptions_RefusesAnAutomaticCommitThatStoresOnlyOnClose(t *testing.T) {
	options := DefaultPollOptions()
	options.Commit = AutoCommitPolicy(AutoCommit{After: CommitAfterDelivery})

	require.ErrorIs(t, options.Validate(), ierror.ErrInvalidConfiguration)

	options.Commit = AutoCommitPolicy(AutoCommit{Interval: time.Second})
	require.NoError(t, options.Validate())
	options.Commit = AutoCommitPolicy(AutoCommit{Every: CommitEvery{Kind: CommitEveryMessages}})
	require.ErrorIs(t, options.Validate(), ierror.ErrInvalidConfiguration)
}

func TestPollOptions_RefusesLimitsThatTheServerRefuses(t *testing.T) {
	for name, change := range map[string]func(*PollOptions){
		"no count":          func(o *PollOptions) { o.Count = 0 },
		"min above count":   func(o *PollOptions) { o.MinCount = o.Count + 1 },
		"wait too long":     func(o *PollOptions) { o.MaxWait = MaxDeferredPollWait + time.Second },
		"too few max bytes": func(o *PollOptions) { o.MaxBytes = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			options := DefaultPollOptions()
			change(&options)
			require.Error(t, options.Validate())
		})
	}
}

func TestPolledMessage_BindsItsCheckpointAndContinuationToItsPartition(t *testing.T) {
	context := PartitionContext{Incarnation: 3, OwnerGeneration: 4, MetadataOp: 5}
	polled := &PolledMessage{PartitionId: 2, Context: context, Messages: []IggyMessage{
		{Header: MessageHeader{Offset: 7}}, {Header: MessageHeader{Offset: 8}},
	}}

	last, ok := polled.Checkpoint()
	require.True(t, ok)
	assert.Equal(t, []any{uint32(2), uint64(8), context}, []any{last.PartitionID(), last.Offset(), last.Context()})
	first, ok := polled.CheckpointAt(0)
	require.True(t, ok)
	assert.Equal(t, uint64(7), first.Offset())
	_, ok = polled.CheckpointAt(2)
	assert.False(t, ok)
	continuation, ok := polled.Continuation()
	require.True(t, ok)
	assert.Equal(t, uint64(9), continuation.Value)
	assert.Equal(t, POLLING_OFFSET, continuation.Kind)
	require.NoError(t, continuation.RequirePartition(2))
	require.ErrorIs(t, continuation.RequirePartition(3), ierror.ErrInvalidCommand)
	bound, ok := continuation.WithContext(PartitionContext{}).Partition()
	assert.True(t, ok)
	assert.Equal(t, uint32(2), bound)
	_, ok = (&PolledMessage{PartitionId: 2}).Continuation()
	assert.False(t, ok)
}
