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

package vsr

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSession_StartsUnboundWithANonzeroClientID(t *testing.T) {
	session := NewSession()

	assert.False(t, session.ClientID().IsZero())
	assert.False(t, session.Bound())
	assert.Zero(t, session.SessionID())
	assert.Equal(t, uint64(1), session.CurrentRequestID())
	assert.False(t, session.HasActivity())
}

func TestNewSession_DrawsADistinctClientIDPerSession(t *testing.T) {
	first := NewSession()
	second := NewSession()

	assert.NotEqual(t, first.ClientID(), second.ClientID())
}

func TestSessionBind_AcceptsOnePositiveFence(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 7})
	session.BeginRegister()

	assert.ErrorIs(t, session.Bind(0), ErrInvalidSession)
	require.NoError(t, session.Bind(42))
	assert.Equal(t, uint64(42), session.SessionID())
	assert.True(t, session.Bound())
	assert.ErrorIs(t, session.Bind(43), ErrSessionAlreadyBound)
	assert.Equal(t, uint64(42), session.SessionID())
}

func TestSessionBeginRegister_AlwaysReturnsRequestZero(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 7})

	assert.Zero(t, session.BeginRegister())
	assert.True(t, session.HasActivity())
}

func TestSessionNextRequestID_AdvancesMonotonically(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 7})
	session.BeginRegister()
	require.NoError(t, session.Bind(42))

	first, err := session.NextRequestID()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), first)

	second, err := session.NextRequestID()
	require.NoError(t, err)
	assert.Equal(t, uint64(2), second)

	assert.Equal(t, uint64(3), session.CurrentRequestID())
	assert.Equal(t, uint64(3), session.CurrentRequestID(), "reading does not advance")
}

func TestSessionNextRequestID_RejectsAnUnboundSession(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 7})

	_, err := session.NextRequestID()
	assert.ErrorIs(t, err, ErrNotBound)
	assert.Equal(t, uint64(1), session.CurrentRequestID(), "the failure burns no id")
}

func TestSessionNextRequestID_RejectsAnExhaustedCounter(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 7})
	session.BeginRegister()
	require.NoError(t, session.Bind(42))
	session.requestCounter.Store(math.MaxUint64)

	_, err := session.NextRequestID()
	assert.ErrorIs(t, err, ErrRequestCounterExhausted)
}

func TestSessionBeginRegister_RearmsWithAFreshClientID(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 7})
	session.BeginRegister()
	require.NoError(t, session.Bind(42))
	_, err := session.NextRequestID()
	require.NoError(t, err)

	assert.Zero(t, session.BeginRegister())
	assert.False(t, session.Bound())
	assert.Zero(t, session.SessionID())
	assert.Equal(t, uint64(2), session.CurrentRequestID())
	assert.NotEqual(t, ClientID{Lo: 7}, session.ClientID())
}

func TestSessionBeginRegister_RearmsAfterAConsumedRegister(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 7})
	session.BeginRegister()

	session.BeginRegister()
	assert.NotEqual(t, ClientID{Lo: 7}, session.ClientID())
	require.NoError(t, session.Bind(9), "the re-armed session binds cleanly")
}

func TestSessionReset_DropsTheFenceAndMintsANewClientID(t *testing.T) {
	session := NewSessionWithClientID(ClientID{Lo: 7})
	session.BeginRegister()
	require.NoError(t, session.Bind(42))

	session.Reset()

	assert.False(t, session.Bound())
	assert.Zero(t, session.SessionID())
	assert.Equal(t, uint64(1), session.CurrentRequestID())
	assert.False(t, session.HasActivity())
	assert.NotEqual(t, ClientID{Lo: 7}, session.ClientID())
	assert.False(t, session.ClientID().IsZero())
}

func TestSessionHasActivity_TracksEveryEntryPoint(t *testing.T) {
	t.Run("register", func(t *testing.T) {
		session := NewSessionWithClientID(ClientID{Lo: 7})
		session.BeginRegister()
		assert.True(t, session.HasActivity())
	})

	t.Run("bind", func(t *testing.T) {
		session := NewSessionWithClientID(ClientID{Lo: 7})
		require.NoError(t, session.Bind(3))
		assert.True(t, session.HasActivity())
	})

	t.Run("advanced counter", func(t *testing.T) {
		session := NewSessionWithClientID(ClientID{Lo: 7})
		require.NoError(t, session.Bind(3))
		_, err := session.NextRequestID()
		require.NoError(t, err)
		assert.True(t, session.HasActivity())
	})
}

func TestNewClientID_NonZeroWithTopBitClear(t *testing.T) {
	for range 64 {
		client := newClientID()
		assert.False(t, client.IsZero())
		assert.LessOrEqual(t, client.Hi, uint64(math.MaxInt64))
	}
}

func TestSessionForksAllocateOneCounterConcurrentlyAndSurviveParentReset(t *testing.T) {
	parent := NewSession()
	require.NoError(t, parent.Bind(42))
	const links = 4
	const requests = 128
	ids := make(chan uint64, links*requests)
	var workers sync.WaitGroup
	for range links {
		link := parent.Fork()
		workers.Go(func() {
			for range requests {
				id, err := link.NextRequestID()
				if err != nil {
					t.Error(err)
					return
				}
				ids <- id
			}
		})
	}
	workers.Wait()
	close(ids)
	seen := make(map[uint64]bool, links*requests)
	for id := range ids {
		require.False(t, seen[id], "duplicate request id %d", id)
		seen[id] = true
	}
	require.Len(t, seen, links*requests)
	old := parent.Fork()
	parent.Reset()
	id, err := old.NextRequestID()
	require.NoError(t, err)
	assert.Equal(t, uint64(links*requests+1), id)
	assert.Equal(t, uint64(links*requests+2), parent.CurrentRequestID())
	require.NoError(t, parent.Bind(43))
	newID, err := parent.NextRequestID()
	require.NoError(t, err)
	assert.Equal(t, id+1, newID)
	assert.NotEqual(t, parent.ClientID(), old.ClientID())
}
