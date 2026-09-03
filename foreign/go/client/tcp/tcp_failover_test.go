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
	"crypto/tls"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
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

func TestFailover_WalksPastTwoRefusingReplicasToThePartitionPrimary(t *testing.T) {
	var metadataLeader *testListener

	partitionPrimary := listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch read.operation() {
		case vsr.OperationRegister:
			return registerReplyFrame(7, 384)
		case vsr.OperationCreateStream:
			return replyFrame(vsr.OperationCreateStream, resultSection())
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})
	follower := listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.operation() == vsr.OperationRegister {
			return registerReplyFrame(7, 256)
		}
		return statusReplyFrame(vsr.OperationCreateStream,
			uint32(ierror.TransientNotAcceptedCode), nil)
	})
	metadataLeader = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, metadataLeader.address(),
				follower.address(), partitionPrimary.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 128)
		default:
			return statusReplyFrame(vsr.OperationCreateStream,
				uint32(ierror.TransientNotAcceptedCode), nil)
		}
	})

	client := newDialingClient(t, metadataLeader.address())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)

	_, err = client.do(ctx, &command.CreateStream{Name: "orders"})
	require.NoError(t, err)
	assert.Equal(t, partitionPrimary.address(), client.currentServerAddress)
	assert.True(t, len(follower.recorded()) >= 2,
		"the roster walk skipped the second replica")
}

func TestFailover_RosterWalkVisitsEachEndpointOnce(t *testing.T) {
	roster := []string{"iggy-0:8090", "iggy-1:8090", "iggy-2:8090"}
	visited := make(map[string]struct{})

	second := nextRosterEndpoint(roster[0], roster, visited)
	third := nextRosterEndpoint(second, roster, visited)
	exhausted := nextRosterEndpoint(third, roster, visited)

	assert.Equal(t, roster[1], second)
	assert.Equal(t, roster[2], third)
	assert.Empty(t, exhausted)
	assert.Len(t, visited, len(roster))
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

// A stale-client eviction is not caller intent: the heartbeat verifier sends it
// after a gc pause or a laptop sleep, and a client that signed in by hand has
// to recover from it exactly like one with a configured auto-login. Same rule
// in every SDK.
func TestFailover_ServerEvictionReplaysTheRememberedSignIn(t *testing.T) {
	var server *testListener
	var evict atomic.Bool
	var registers atomic.Int32
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.operation() == vsr.OperationRegister {
			registers.Add(1)
			return registerReplyFrame(7, 128)
		}
		if evict.CompareAndSwap(true, false) {
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
	registersBefore := registers.Load()

	evict.Store(true)
	// A ping is non-replicated, so the eviction is absorbed: the reconnect it
	// triggers signs in again with the credentials the sign-in remembered and
	// the request completes over the session it re-established.
	require.NoError(t, client.Ping(ctx), "the evicted request was not recovered")

	_, remembered := client.signInCredentials()
	assert.True(t, remembered, "an eviction is not a sign-out; the credentials stay")
	require.NoError(t, client.Ping(ctx), "the session came back on its own")
	assert.Greater(t, registers.Load(), registersBefore,
		"the reconnect re-established the session")
	assert.True(t, client.session.Bound())
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

// The other way a logout fails to land: the node answers it as not-admitted,
// which is what a node that stopped being primary does. The redirect that
// follows must not sign in on its own -- this goroutine holds the sign-in lock,
// and the reconnect's automatic sign-in would wait on it forever.
func TestFailover_ReLoginSurvivesALogoutTheOldPrimaryRefused(t *testing.T) {
	var leader *testListener
	var follower *testListener
	var demoted atomic.Bool

	// The node the client is on: leader until the logout, then a follower that
	// refuses it as not-admitted and points at the survivor.
	follower = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.GetClusterMetadataCode):
			if demoted.Load() {
				return clusterMetadataFrame(t, 1, follower.address(), leader.address())
			}
			return clusterMetadataFrame(t, 0, follower.address(), leader.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 128)
		case read.operation() == vsr.OperationLogout:
			demoted.Store(true)
			return statusReplyFrame(vsr.OperationLogout,
				uint32(ierror.TransientNotAcceptedCode), nil)
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})
	leader = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 1, follower.address(), leader.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 256)
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	client := newDialingClient(t, follower.address())
	ctx := context.Background()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)

	relogin := make(chan error, 1)
	go func() {
		_, err := client.LoginUser(ctx, "iggy", "iggy")
		relogin <- err
	}()
	select {
	case err := <-relogin:
		require.NoError(t, err, "the sign-in has to settle on the node that leads")
	case <-time.After(15 * time.Second):
		t.Fatal("the re-login deadlocked on the sign-in lock")
	}

	assert.True(t, client.session.Bound(), "the replayed sign-in bound a session")
	assert.Equal(t, leader.address(), client.currentServerAddress)
}

// A logout that never landed still ended the session it belonged to, so the
// credentials that established it must not outlive it: a sign-in that then
// fails would otherwise leave them for the next dropped request to replay,
// signing the old user back in after the caller asked for another one.
func TestFailover_ARejectedReLoginDoesNotResurrectThePreviousUser(t *testing.T) {
	var server *testListener
	var dropLogout atomic.Bool
	var dropSocket atomic.Bool
	var rejectLogin atomic.Bool
	var registeredUsers atomic.Int32
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if dropSocket.Load() {
			return nil
		}
		if dropLogout.Load() && read.operation() == vsr.OperationLogout {
			return nil
		}
		if read.operation() == vsr.OperationRegister {
			registeredUsers.Add(1)
			if rejectLogin.Load() {
				return statusReplyFrame(vsr.OperationRegister,
					uint32(ierror.InvalidCredentialsCode), nil)
			}
			return registerReplyFrame(7, 128)
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})

	client := newDialingClient(t, server.address())
	ctx := context.Background()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "alice", "alice")
	require.NoError(t, err)

	// The logout is swallowed and the sign-in that follows is rejected, so the
	// client ends up with no session and no credentials it may use.
	dropLogout.Store(true)
	rejectLogin.Store(true)
	_, err = client.LoginUser(ctx, "bob", "bob")
	require.Error(t, err)

	_, remembered := client.signInCredentials()
	assert.False(t, remembered, "the ended session's credentials must not survive it")

	// The socket dies with nothing remembered: the reconnect has no session to
	// restore, and must not invent one out of the user who was signed in
	// before.
	dropLogout.Store(false)
	dropSocket.Store(true)
	registersBefore := registeredUsers.Load()
	assert.Error(t, client.Ping(ctx), "there is no session left to restore")
	assert.Equal(t, registersBefore, registeredUsers.Load(),
		"the reconnect signed the previous user back in")
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

// The cooldown is a pace limit, not a commitment: a caller that gave the
// connect a deadline has to get an answer inside it, and Close has to end the
// wait too.
func TestFailover_TheReestablishPauseHonoursTheCallersDeadline(t *testing.T) {
	current := listenVSR(t, nil, func(_, _ int, read request) []byte {
		return singleNodeHandler(t, func() string { return "127.0.0.1:8090" })(0, 0, read)
	})

	client := newDialingClient(t, current.address())
	client.config.reconnection.reestablishAfter = time.Minute
	client.connectedAt = time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	_ = client.Connect(ctx)

	assert.Less(t, time.Since(started), 5*time.Second,
		"the cooldown outlived the deadline the caller gave the connect")
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

// The SNI of a dial belongs to the endpoint being dialed. Taken from the
// endpoint the client just lost, a failover to a node the certificate does not
// cover fails the handshake -- which is every failover, once the addresses
// differ.
func TestFailover_UsesTheDialedEndpointAsTheServerName(t *testing.T) {
	certificate, caPath := selfSignedCert(t)
	survivor := listenVSR(t,
		func(conn net.Conn) net.Conn {
			return tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{certificate}})
		},
		singleNodeHandler(t, func() string { return "127.0.0.1:8090" }))

	// The certificate covers 127.0.0.1, and the endpoint the client starts on
	// is 127.0.0.2, where nothing listens: with the server name taken from
	// that endpoint, the handshake on the survivor is checked against the
	// address that died.
	_, port, err := net.SplitHostPort(survivor.address())
	require.NoError(t, err)
	client := newDialingClient(t, "127.0.0.2:"+port,
		WithTLS(WithTLSCAFile(caPath), WithTLSValidateCertificate(true)))
	client.knownServerAddresses = []string{"127.0.0.1:" + port}

	require.NoError(t, client.Connect(context.Background()))
	assert.Equal(t, "127.0.0.1:"+port, client.currentServerAddress)
}

// A TLS configuration the client itself cannot satisfy says the same thing on
// every attempt, so it has to reach the caller instead of being redialed every
// interval forever -- which is what the default unlimited retries did with it.
func TestFailover_AConfigFaultEndsTheConnectInsteadOfRetryingForever(t *testing.T) {
	certificate, _ := selfSignedCert(t)
	server := listenVSR(t,
		func(conn net.Conn) net.Conn {
			return tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{certificate}})
		},
		singleNodeHandler(t, func() string { return "127.0.0.1:8090" }))

	// A CA the server's certificate was not signed by: no retry makes that
	// certificate acceptable.
	_, unrelatedCA := selfSignedCert(t)
	client := NewIggyTcpClient(slog.New(slog.DiscardHandler),
		WithServerAddress(server.address()),
		WithTLS(WithTLSCAFile(unrelatedCA), WithTLSValidateCertificate(true)))
	t.Cleanup(func() { _ = client.Close() })
	client.config.reconnection.maxRetries = 0 // unlimited
	client.config.reconnection.interval = 10 * time.Millisecond

	done := make(chan error, 1)
	go func() { done <- client.Connect(context.Background()) }()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a connect that can never succeed has to end instead of retrying forever")
	}
}

// A peer that answers the handshake in plaintext says something about that
// endpoint, not about this client's TLS configuration. Ended the whole connect,
// one misconfigured node in the roster costs the client every endpoint behind
// it, including the ones that are only down for a moment.
func TestFailover_APlaintextEndpointDoesNotEndTheSweep(t *testing.T) {
	certificate, _ := selfSignedCert(t)
	var accepted atomic.Int32
	var survivor *testListener
	survivor = listenVSR(t,
		func(conn net.Conn) net.Conn {
			if accepted.Add(1) == 1 {
				// Down for the first pass, up for the second: without it the
				// sweep reaches this node and the pass succeeds either way.
				_ = conn.Close()
				return conn
			}
			return tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{certificate}})
		},
		singleNodeHandler(t, func() string { return survivor.address() }))

	plaintext, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = plaintext.Close() })
	go func() {
		for {
			conn, err := plaintext.Accept()
			if err != nil {
				return
			}
			_, _ = conn.Write([]byte("not a TLS record\n"))
			_ = conn.Close()
		}
	}()

	client := newDialingClient(t, plaintext.Addr().String(),
		WithTLS(WithTLSValidateCertificate(false)))
	client.config.reconnection.maxRetries = 2
	client.knownServerAddresses = []string{survivor.address()}

	require.NoError(t, client.Connect(context.Background()))
	assert.Equal(t, survivor.address(), client.currentServerAddress)
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

// Concurrent Connects are one attempt, and a caller that did not run it still
// gets a client it can use the moment its Connect returns. Told "connected"
// while the attempt is still signing in, its next request fails
// ErrNotConnected for no reason of its own.
func TestConnect_ConcurrentCallersShareOneAttempt(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.operation() == vsr.OperationRegister {
			// The sign-in is the slow part of an attempt, and it runs after the
			// dial: a caller that returned early would use the client here.
			time.Sleep(300 * time.Millisecond)
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))

	const callers = 8
	results := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			<-start
			if err := client.Connect(context.Background()); err != nil {
				results <- err
				return
			}
			// Usable right away, or the Connect that returned was a lie.
			results <- client.Ping(context.Background())
		}()
	}
	close(start)
	for range callers {
		require.NoError(t, <-results, "a caller was handed a client it could not use")
	}

	assert.Equal(t, 1, server.connections(), "the callers dialed more than once")
	var registers int
	for _, read := range server.recorded() {
		if read.operation() == vsr.OperationRegister {
			registers++
		}
	}
	assert.Equal(t, 1, registers, "one attempt, one sign-in")
}

// Two requests failing at once are one reconnect. Each tears the connection
// down before reconnecting, and a teardown that resets the state under an
// attempt already dialing lets the second caller start a second attempt: the
// two then fight over which socket is installed and which outcome the waiters
// are handed.
func TestConnect_ConcurrentReconnectsThroughExchangeShareOneAttempt(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		if connection == 0 && read.code() == uint32(command.PingCode) {
			// Ends the socket under both in-flight requests at once.
			return nil
		}
		if connection > 0 && read.operation() == vsr.OperationRegister {
			// The reconnect's sign-in is the slow part, so the second caller
			// reliably arrives while the first attempt is still running.
			time.Sleep(300 * time.Millisecond)
		}
		return singleNodeHandler(t, func() string { return server.address() })(connection, index, read)
	})

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	const callers = 2
	results := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		go func() {
			<-start
			results <- client.Ping(context.Background())
		}()
	}
	close(start)
	for range callers {
		require.NoError(t, <-results, "a request did not survive the reconnect")
	}

	assert.Equal(t, 2, server.connections(),
		"the two failing requests reconnected separately")
}

// The teardown that follows a failure belongs to the connection the request
// ran on. Connect marks the client connected before it signs in, so a caller
// parked on c.mtx for that sign-in wakes to a healthy-looking state that says
// nothing about which connection is installed.
func TestDisconnect_DoesNotCloseAConnectionItDidNotFailOn(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	// What a request that fails on this connection carries with it.
	failed := client.connGeneration

	require.NoError(t, client.disconnect())
	require.NoError(t, client.Connect(context.Background()))
	require.NotEqual(t, failed, client.connGeneration,
		"a reconnect installs a connection of its own")

	torn, err := client.disconnectGeneration(failed)
	require.NoError(t, err)
	assert.False(t, torn, "the stale generation reported a teardown it did not make")
	assert.Equal(t, iggcon.TransportStateConnected, client.transportState)
	require.NoError(t, client.Ping(context.Background()),
		"the reconnected client was torn down by a stale failure")
	assert.Equal(t, 2, server.connections(), "the stale teardown forced a third connection")

	// The connection the caller did fail on is still torn down.
	torn, err = client.disconnectGeneration(client.connGeneration)
	require.NoError(t, err)
	assert.True(t, torn)
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
}

// The sign-in transaction holds registerMtx across its reconnect, and an
// attempt started by a plain request ends in a sign-in that needs that same
// lock. Waiting for that attempt closes a cycle -- the owner blocked on
// registerMtx, the transaction blocked on the owner -- and callers pass a
// context with no deadline, so nothing breaks it.
func TestConnect_DoesNotWaitOnAnAttemptThatSignsIn(t *testing.T) {
	certificate, _ := selfSignedCert(t)
	var survivor *testListener
	survivor = listenVSR(t,
		func(conn net.Conn) net.Conn {
			return tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{certificate}})
		},
		singleNodeHandler(t, func() string { return survivor.address() }))

	// A listener that accepts and never answers the ClientHello: the attempt
	// spends the whole dial bound here, which is the window a second caller
	// arrives in.
	silent, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = silent.Close() })

	client := newDialingClient(t, silent.Addr().String(),
		WithTLS(WithTLSValidateCertificate(false)),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	client.knownServerAddresses = []string{survivor.address()}

	// Stands in for the sign-in transaction: register holds this across the
	// disconnect and the reconnect that follow it.
	client.registerMtx.Lock()
	owner := make(chan error, 1)
	go func() { owner <- client.Connect(context.Background()) }()
	require.Eventually(t, func() bool {
		client.mtx.Lock()
		defer client.mtx.Unlock()
		return client.transportState == iggcon.TransportStateConnecting
	}, time.Second, time.Millisecond, "the attempt never started dialing")

	suppressed := make(chan error, 1)
	go func() { suppressed <- client.Connect(suppressAutoLogin(context.Background())) }()
	select {
	case err := <-suppressed:
		require.Error(t, err, "the transaction was told a connection it does not have is up")
	case <-time.After(2 * failoverDialTimeout):
		t.Fatal("the sign-in transaction waited on an attempt that cannot finish without it")
	}

	client.registerMtx.Unlock()
	require.NoError(t, <-owner, "the attempt the transaction left alone did not finish")
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
