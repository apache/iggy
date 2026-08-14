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
	session.requestCounter = math.MaxUint64

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
	assert.Equal(t, uint64(1), session.CurrentRequestID())
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

func TestNewClientID_NeverReturnsZero(t *testing.T) {
	for range 64 {
		assert.False(t, newClientID().IsZero())
	}
}
