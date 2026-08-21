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
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The node a client signed in on dies; its next request has to complete on a
// survivor the roster named, under the identity a fresh sign-in binds there.
// Mirrors `core/integration/tests/cluster/failover_client_continuity.rs`.
func TestFailover_ResumesOnASurvivorAfterTheSignedInNodeDies(t *testing.T) {
	var survivor *testListener
	var primary *testListener
	var primaryDead atomic.Bool

	survivor = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 1, primary.address(), survivor.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 512)
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	primary = listenVSR(t, nil, func(_, _ int, read request) []byte {
		// A dead node answers nothing; returning nil drops the connection the
		// way a killed process does.
		if primaryDead.Load() {
			return nil
		}
		switch {
		case read.code() == uint32(command.GetClusterMetadataCode):
			// The primary leads, so the sign-in settles here and the roster is
			// only remembered -- not acted on -- until the node dies.
			return clusterMetadataFrame(t, 0, primary.address(), survivor.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 128)
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	// No auto-login: the credentials come from the caller's own sign-in, which
	// is the shape that could not reconnect at all before.
	client := newDialingClient(t, primary.address())
	ctx := context.Background()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)
	require.NoError(t, client.Ping(ctx), "the live primary answers")
	require.Equal(t, primary.address(), client.currentServerAddress)

	primaryDead.Store(true)
	require.NoError(t, primary.listener.Close(), "stop accepting, so a redial is refused")

	require.NoError(t, client.Ping(ctx),
		"the client has to resume on the survivor the roster named")

	assert.Equal(t, survivor.address(), client.currentServerAddress,
		"the client moved off the dead endpoint")
	assert.True(t, client.session.Bound(), "the session was re-established")

	var registers int
	for _, read := range survivor.recorded() {
		if read.operation() == vsr.OperationRegister {
			registers++
		}
	}
	assert.Equal(t, 1, registers,
		"the remembered credentials signed in again on the survivor")
}

// Without any credentials there is nothing to sign in with, so a request on a
// dead node fails instead of reconnecting into an unauthenticated session.
func TestFailover_FailsFastWhenNothingEverSignedIn(t *testing.T) {
	var server *testListener
	var dead atomic.Bool
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if dead.Load() {
			return nil
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})

	client := newDialingClient(t, server.address())
	ctx := context.Background()
	require.NoError(t, client.Connect(ctx))
	require.NoError(t, client.Ping(ctx))

	dead.Store(true)
	require.NoError(t, server.listener.Close())

	assert.Error(t, client.Ping(ctx),
		"a client that never signed in cannot restore a session by reconnecting")
}

// An explicit sign-out is caller intent: the reconnect must not sign back in
// with the credentials the earlier sign-in used.
func TestFailover_DoesNotResurrectASignedOutSession(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address())
	ctx := context.Background()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)
	require.NoError(t, client.LogoutUser(ctx))

	credentials, ok := client.signInCredentials()
	assert.False(t, ok, "the sign-out forgot them")
	assert.Empty(t, credentials.username)
}

func TestFailover_LeavesTheReestablishPauseToSingleEndpointClients(t *testing.T) {
	client := NewIggyTcpClient(slog.New(slog.DiscardHandler),
		WithServerAddress("127.0.0.1:8090"))
	client.config.reconnection.reestablishAfter = time.Minute
	client.connectedAt = time.Now()

	// One endpoint: the pause is the only thing keeping a reconnect from
	// hammering the node it just lost.
	require.Len(t, client.connectionCandidates(), 1)

	client.knownServerAddresses = []string{"127.0.0.1:8091"}
	require.Len(t, client.connectionCandidates(), 2,
		"with somewhere else to go the pause only delays the failover")
}
