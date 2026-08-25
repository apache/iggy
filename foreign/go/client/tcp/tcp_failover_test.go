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
	"net"
	"sync/atomic"
	"testing"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"
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

// A stale-client eviction is the server ending the session authoritatively,
// like a logout: the remembered sign-in must not resurrect it, so the evicted
// request surfaces the loss instead of reconnecting into a fresh session.
func TestFailover_ServerEvictionForgetsTheRememberedSignIn(t *testing.T) {
	var server *testListener
	var evict atomic.Bool
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.operation() == vsr.OperationRegister {
			return registerReplyFrame(7, 128)
		}
		if evict.Load() {
			return evictionFrame(vsr.EvictionStaleClient, 0, 0)
		}
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 0, server.address())
		}
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	client := newDialingClient(t, server.address())
	ctx := context.Background()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)
	connectionsBefore := server.connections()

	evict.Store(true)
	require.Error(t, client.Ping(ctx), "the evicted request surfaces the loss")

	_, remembered := client.signInCredentials()
	assert.False(t, remembered, "the eviction forgot the remembered sign-in")
	assert.Equal(t, connectionsBefore, server.connections(),
		"no reconnect dial resurrected the evicted session")
}

// An explicit sign-out is caller intent: the reconnect must not sign back in
// with the credentials the earlier sign-in used.
func TestFailover_DoesNotResurrectASignedOutSession(t *testing.T) {
	var server *testListener
	var dropSocket atomic.Bool
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		// A dropped connection is what makes the client reconnect at all; nil
		// ends it the way a killed process does.
		if dropSocket.Load() {
			return nil
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})

	client := newDialingClient(t, server.address())
	ctx := context.Background()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)
	require.NoError(t, client.LogoutUser(ctx))

	credentials, ok := client.signInCredentials()
	require.False(t, ok, "the sign-out forgot them")
	require.Empty(t, credentials.username)

	// The socket dies under a signed-out client: the reconnect has nothing to
	// restore and must not invent a session.
	dropSocket.Store(true)
	assert.Error(t, client.Ping(ctx), "a signed-out client cannot replay through a sign-in")
	dropSocket.Store(false)

	require.NoError(t, client.Connect(ctx))
	require.NoError(t, client.Ping(ctx), "the transport recovers on its own")

	var registers int
	for _, read := range server.recorded() {
		if read.operation() == vsr.OperationRegister {
			registers++
		}
	}
	assert.Equal(t, 1, registers,
		"only the caller's own sign-in registered; the reconnect added none")
}

// A re-login over a dropped transport has to complete. The logout that ends
// the old session runs while the sign-in lock is held, so a logout that enters
// the reconnect path would reconnect, sign in with the remembered credentials,
// and deadlock on that same lock.
func TestFailover_ReLoginSurvivesALogoutTheTransportSwallowed(t *testing.T) {
	var server *testListener
	var dropLogout atomic.Bool
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if dropLogout.Load() && read.operation() == vsr.OperationLogout {
			// The frame is swallowed and the connection ends, exactly as a
			// node that dies mid-logout leaves it.
			return nil
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})

	client := newDialingClient(t, server.address())
	ctx := context.Background()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)

	dropLogout.Store(true)
	relogin := make(chan error, 1)
	go func() {
		_, err := client.LoginUser(ctx, "iggy", "iggy")
		relogin <- err
	}()
	select {
	case err := <-relogin:
		require.NoError(t, err, "the sign-in has to replay on the new connection")
	case <-time.After(15 * time.Second):
		t.Fatal("the re-login deadlocked on the sign-in lock")
	}

	assert.True(t, client.session.Bound(), "the replayed sign-in bound a session")
	require.NoError(t, client.Ping(ctx))
}

// reestablishAfter is a cooldown on redialing the endpoint that was lost. It
// is owed to that endpoint alone, so a failover to another one must not sit
// through it.
func TestFailover_DoesNotSpendTheLostEndpointsPauseOnAnotherEndpoint(t *testing.T) {
	var survivor *testListener
	survivor = listenVSR(t, nil, singleNodeHandler(t, func() string { return survivor.address() }))

	client := newDialingClient(t, deadAddress(t))
	client.config.reconnection.reestablishAfter = time.Minute
	client.knownServerAddresses = []string{survivor.address()}
	client.connectedAt = time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := time.Now()
	require.NoError(t, client.Connect(ctx))

	assert.Equal(t, survivor.address(), client.currentServerAddress)
	assert.Less(t, time.Since(started), 2*time.Second,
		"the failover waited out a pause it owed only the lost endpoint")
}

// The other half of the same promise: WithReestablishAfter is a cooldown on
// the endpoint that was lost, and a known roster does not cancel it.
func TestFailover_KeepsTheReestablishPauseForTheEndpointThatWasLost(t *testing.T) {
	var current *testListener
	current = listenVSR(t, nil, singleNodeHandler(t, func() string { return current.address() }))

	client := newDialingClient(t, current.address())
	client.config.reconnection.reestablishAfter = 500 * time.Millisecond
	client.knownServerAddresses = []string{deadAddress(t)}
	client.connectedAt = time.Now()

	started := time.Now()
	require.NoError(t, client.Connect(context.Background()))

	assert.Equal(t, current.address(), client.currentServerAddress)
	assert.GreaterOrEqual(t, time.Since(started), 350*time.Millisecond,
		"the cooldown on the endpoint that was lost was skipped")
}

// A node whose syns are dropped must not hold the sweep: without a bound on
// the dial the survivors behind it are never reached. A black-holed address
// cannot be arranged portably, so this pins the bound itself.
func TestFailover_BoundsTheDialWhenOtherEndpointsAreQueuedBehindIt(t *testing.T) {
	assert.Equal(t, 2*time.Second, failoverDialTimeout,
		"the dial bound has to match the other SDKs")

	var survivor *testListener
	survivor = listenVSR(t, nil, singleNodeHandler(t, func() string { return survivor.address() }))

	// A listener that accepts and never answers: the dial completes out of the
	// backlog, so only the bound ends the attempt.
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = silent.Close() })

	client := newDialingClient(t, silent.Addr().String(),
		WithTLS(WithTLSValidateCertificate(false)))
	client.knownServerAddresses = []string{survivor.address()}

	done := make(chan error, 1)
	go func() { done <- client.Connect(context.Background()) }()
	select {
	case <-done:
	case <-time.After(3 * failoverDialTimeout):
		t.Fatal("the sweep never got past an endpoint that answers nothing")
	}
}

// An endpoint that accepts TCP but fails the handshake is not where this
// client lives: recording it would make the next pass lead with it and shadow
// every endpoint behind it.
func TestFailover_DoesNotSettleOnAnEndpointThatFailedTheHandshake(t *testing.T) {
	hangup, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = hangup.Close() })
	go func() {
		for {
			connection, err := hangup.Accept()
			if err != nil {
				return
			}
			// Plain TCP behind a TLS client: the dial succeeds, the handshake
			// cannot.
			_ = connection.Close()
		}
	}()

	configured := deadAddress(t)
	client := newDialingClient(t, configured, WithTLS(WithTLSValidateCertificate(false)))
	client.config.reconnection.enabled = false
	client.knownServerAddresses = []string{hangup.Addr().String()}

	require.Error(t, client.Connect(context.Background()))
	assert.Equal(t, configured, client.currentServerAddress,
		"the endpoint that failed the handshake became the current one")
}

// A client with nothing to dial must say so: reporting success would leave
// every request answering ErrNotConnected while Connect keeps claiming a
// connection.
func TestFailover_RejectsAConnectWithNoEndpointToDial(t *testing.T) {
	client := NewIggyTcpClient(slog.New(slog.DiscardHandler), WithServerAddress(""))
	t.Cleanup(func() { _ = client.Close() })

	require.ErrorIs(t, client.Connect(context.Background()), ierror.ErrCannotEstablishConnection)
	assert.Error(t, client.Ping(context.Background()))
}

// deadAddress returns an address nothing listens on, so a dial to it is
// refused at once.
func deadAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}
