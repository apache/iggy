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
	"fmt"
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

func TestReconnect_BindVerdictPreservesOrReplacesOnlyTheLogicalSession(t *testing.T) {
	for _, refusal := range []ierror.Code{0, ierror.StaleClientCode, ierror.UnauthenticatedCode, ierror.TransientNotCommittedCode, ierror.TransientNotAcceptedCode} {
		t.Run(fmt.Sprint(refusal), func(t *testing.T) {
			var server *testListener
			var registrations, writes atomic.Int32
			var bindStatus atomic.Uint32
			bindStatus.Store(uint32(refusal))
			server = listenVSR(t, nil, func(_, _ int, read request) []byte {
				switch {
				case read.operation() == vsr.OperationRegister:
					return registerReplyFrame(7, uint64(128+registrations.Add(1)))
				case read.code() == uint32(command.BindSessionCode):
					if status := bindStatus.Swap(0); status != 0 {
						return statusReplyFrame(read.operation(), status, nil)
					}
					return bindReplyFrame(7, read.sessionID())
				case read.code() == uint32(command.GetClusterMetadataCode):
					return clusterMetadataFrame(t, 0, server.address())
				case read.operation() == vsr.OperationCreateStream:
					if writes.Add(1) == 1 {
						return nil
					}
					return committedReply(read, 0)
				default:
					return replyFrame(read.operation(), nil)
				}
			})
			client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			require.NoError(t, client.Connect(ctx))
			original := client.session.ClientID()
			_, err := client.do(ctx, &command.CreateStream{Name: "lost-reply"})
			terminal := refusal == ierror.StaleClientCode || refusal == ierror.UnauthenticatedCode
			if terminal {
				var uncertain ierror.UncertainOutcome
				require.ErrorAs(t, err, &uncertain)
				require.Eventually(t, func() bool {
					client.mtx.Lock()
					defer client.mtx.Unlock()
					return client.session.Bound() && client.session.ClientID() != original
				}, time.Second, time.Millisecond)
				require.ErrorIs(t, client.ResolveOperation(ctx, uncertain.OperationID), ierror.ErrOperationNotRetained)
				require.Equal(t, int32(1), writes.Load(), "a replacement session cannot replay the old mutation")
			} else {
				require.NoError(t, err)
				require.Equal(t, int32(2), writes.Load())
				require.Equal(t, original, client.session.ClientID())
			}
			_, err = client.do(ctx, &command.CreateStream{Name: "next-write"})
			require.NoError(t, err)
			var mutations []request
			for _, read := range server.recorded() {
				if read.operation() == vsr.OperationCreateStream {
					mutations = append(mutations, read)
				}
			}
			if terminal {
				require.Len(t, mutations, 2)
				require.NotEqual(t, mutations[0].clientID(), mutations[1].clientID())
				require.Equal(t, int32(2), registrations.Load())
			} else {
				require.Len(t, mutations, 3)
				require.Equal(t, mutations[0].header, mutations[1].header)
				require.Equal(t, mutations[0].payload, mutations[1].payload)
				require.Equal(t, int32(1), registrations.Load())
			}
			require.Greater(t, mutations[len(mutations)-1].requestID(), mutations[0].requestID())
		})
	}
}

func TestFailover_ResumesOnASurvivorAfterTheSignedInNodeDies(t *testing.T) {
	var survivor *testListener
	var primary *testListener
	var primaryDead atomic.Bool

	survivor = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
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
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
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
	require.Equal(t, primary.address(), client.GetConnectionInfo().ServerAddress)

	primaryDead.Store(true)
	require.NoError(t, primary.listener.Close(), "stop accepting, so a redial is refused")

	require.NoError(t, client.Ping(ctx),
		"the client has to resume on the survivor the roster named")

	assert.Equal(t, survivor.address(), client.GetConnectionInfo().ServerAddress,
		"the client moved off the dead endpoint")
	assert.True(t, client.session.Bound(), "the session was re-established")

	var registers int
	for _, read := range survivor.recorded() {
		if read.operation() == vsr.OperationRegister {
			registers++
		}
	}
	assert.Equal(t, 0, registers, "resume must not register another session")
	assert.Equal(t, 1, requestCount(survivor.recorded(), command.BindSessionCode))
}

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
		if read.code() == uint32(command.BindSessionCode) {
			return statusReplyFrame(vsr.OperationNonReplicated, uint32(ierror.ErrUnauthenticated.Code()), nil)
		}
		if read.operation() == vsr.OperationRegister {
			registers.Add(1)
			return registerReplyFrame(7, 128)
		}
		if evict.CompareAndSwap(true, false) {
			return evictionFrame(vsr.EvictionNoSession, 0, 0)
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
	require.ErrorIs(t, client.Ping(ctx), ierror.ErrTransientNotAccepted)
	_, remembered := client.signInCredentials()
	require.True(t, remembered)
	require.Eventually(t, func() bool {
		client.mtx.Lock()
		defer client.mtx.Unlock()
		return client.session.Bound() && registers.Load() > registersBefore
	}, time.Second, time.Millisecond)
	require.NoError(t, client.Ping(ctx))
}

func TestFailover_StaleClientEvictionRebindsTheSessionAndReplaysTheWrite(t *testing.T) {
	var server *testListener
	var evicted atomic.Bool
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 128)
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.operation() == vsr.OperationCreateStream && evicted.CompareAndSwap(false, true):
			return evictionFrame(vsr.EvictionStaleClient, 0, 0)
		case read.operation() == vsr.OperationCreateStream:
			return committedReply(read, 0)
		default:
			return replyFrame(read.operation(), nil)
		}
	})
	client := newDialingClient(t, server.address())
	ctx := context.Background()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)

	_, err = client.do(ctx, &command.CreateStream{Name: "orders"})

	require.NoError(t, err)
	reads := server.recorded()
	require.Len(t, requestsFor(reads, vsr.OperationRegister), 1, "the client keeps its session")
	binds := 0
	for _, read := range reads {
		if read.code() == uint32(command.BindSessionCode) {
			binds++
			require.Equal(t, uint64(128), read.sessionID())
		}
	}
	require.NotZero(t, binds, "the new link binds the session")
	writes := requestsFor(reads, vsr.OperationCreateStream)
	require.Len(t, writes, 2)
	require.Equal(t, writes[0], writes[1], "the replay is the evicted frame")
}

// scriptedWrites is a single node that answers its nth CreateStream with
// answers[n] and every later one with a committed success. Each sign-in gets
// the next session, from 129. If refuseBind is set, the first bind gets status
// 40, as for a session that ended.
func scriptedWrites(t *testing.T, answers []func(read request) []byte, refuseBind bool) *testListener {
	t.Helper()
	var registrations, writes atomic.Int32
	var refused atomic.Bool
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, uint64(128+registrations.Add(1)))
		case read.code() == uint32(command.BindSessionCode) && refuseBind && refused.CompareAndSwap(false, true):
			return statusReplyFrame(read.operation(), uint32(ierror.UnauthenticatedCode), nil)
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.operation() == vsr.OperationCreateStream:
			if write := int(writes.Add(1)) - 1; write < len(answers) {
				return answers[write](read)
			}
			return committedReply(read, 0)
		default:
			return replyFrame(read.operation(), nil)
		}
	})
	return server
}

// boundSessions returns the session that each BindSession request names.
func boundSessions(reads []request) []uint64 {
	var sessions []uint64
	for _, read := range reads {
		if read.code() == uint32(command.BindSessionCode) {
			sessions = append(sessions, read.sessionID())
		}
	}
	return sessions
}

// A committed result with status 30 means that the replicated metadata ended
// the session, so the client registers again without binding the old one.
func TestFailover_SessionEndingReplyRegistersANewSessionWithoutABind(t *testing.T) {
	committed := func(read request) []byte { return committedReply(read, ierror.StaleClientCode) }
	notCommitted := func(read request) []byte { return uncertainAnswer(0, read) }
	unproved := func(read request) []byte { return uncertainAnswer(1, read) }
	for _, test := range []struct {
		name     string
		answers  []func(read request) []byte
		call     error
		resolved error
	}{
		{"committed/first_attempt", []func(request) []byte{committed}, ierror.ErrStaleClient, ierror.ErrOperationNotRetained},
		{"committed/after_uncertain", []func(request) []byte{notCommitted, committed}, ierror.ErrStaleClient, ierror.ErrOperationNotRetained},
		{"committed/resolve", []func(request) []byte{notCommitted, unproved, committed}, ierror.ErrUncertainOutcome, ierror.ErrStaleClient},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := scriptedWrites(t, test.answers, false)
			client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			require.NoError(t, client.Connect(ctx))

			_, err := client.do(ctx, &command.CreateStream{Name: "orders"})

			require.ErrorIs(t, err, test.call)
			id := createStreams(server.recorded())[0].requestID()
			require.ErrorIs(t, client.ResolveOperation(ctx, id), test.resolved)
			written := len(createStreams(server.recorded()))
			require.ErrorIs(t, client.ResolveOperation(ctx, id), test.resolved)
			require.Len(t, createStreams(server.recorded()), written, "a second resolve sends nothing")
			awaitRecoveryEnd(t, client)
			_, err = client.do(ctx, &command.CreateStream{Name: "next"})
			require.NoError(t, err)
			reads := server.recorded()
			require.Empty(t, boundSessions(reads), "the session ends without a bind")
			require.Len(t, requestsFor(reads, vsr.OperationRegister), 2, "the ended session is replaced")
			creates := createStreams(reads)
			require.Len(t, creates, len(test.answers)+1)
			first, next := creates[0], creates[len(creates)-1]
			require.Equal(t, uint64(129), first.sessionID())
			require.Equal(t, uint64(130), next.sessionID(), "the next write goes on the new session")
			require.NotEqual(t, first.clientID(), next.clientID())
		})
	}
}

// A deny with status 30 refuses one attempt, because the server no longer
// attaches the link to the session. A replica that replaced its client table
// denies so for a live session too. So the client closes only that link, and
// the bind on the next link tells whether the session ended.
func TestFailover_StaleClientDenyClosesTheLinkAndTheBindDecides(t *testing.T) {
	deny := func(read request) []byte {
		return statusReplyFrame(read.operation(), uint32(ierror.StaleClientCode), nil)
	}
	notCommitted := func(read request) []byte { return uncertainAnswer(0, read) }
	for _, test := range []struct {
		name     string
		answers  []func(read request) []byte
		ended    bool
		call     error
		resolved error
	}{
		{"first_attempt/live", []func(request) []byte{deny}, false, ierror.ErrStaleClient, ierror.ErrOperationNotRetained},
		{"after_uncertain/live", []func(request) []byte{notCommitted, deny}, false, ierror.ErrUncertainOutcome, nil},
		{"first_attempt/ended", []func(request) []byte{deny}, true, ierror.ErrStaleClient, ierror.ErrOperationNotRetained},
		{"after_uncertain/ended", []func(request) []byte{notCommitted, deny}, true, ierror.ErrUncertainOutcome, ierror.ErrOperationNotRetained},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := scriptedWrites(t, test.answers, test.ended)
			client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			require.NoError(t, client.Connect(ctx))
			initial := client.session.ClientID()
			key := groupKey{stream: "s", topic: "t", group: "g"}
			client.groups.put(key, groupAssignment{generation: 1, partitions: []uint32{0}, fetchedAt: time.Now()})

			_, err := client.do(ctx, &command.CreateStream{Name: "orders"})

			require.ErrorIs(t, err, test.call)
			require.Len(t, createStreams(server.recorded()), len(test.answers), "the deny ends the call")
			if test.ended {
				// The refused bind ends the session after the call returns.
				require.Eventually(t, func() bool {
					client.mtx.Lock()
					defer client.mtx.Unlock()
					return client.session.Bound() && client.session.ClientID() != initial
				}, time.Second, time.Millisecond)
			}
			id := createStreams(server.recorded())[0].requestID()
			require.ErrorIs(t, client.ResolveOperation(ctx, id), test.resolved)
			written := len(createStreams(server.recorded()))
			require.ErrorIs(t, client.ResolveOperation(ctx, id), test.resolved)
			require.Len(t, createStreams(server.recorded()), written, "a second resolve sends nothing")
			_, err = client.do(ctx, &command.CreateStream{Name: "next"})
			require.NoError(t, err)
			reads := server.recorded()
			require.Equal(t, []uint64{129}, boundSessions(reads), "the next link binds the same session")
			creates := createStreams(reads)
			require.Len(t, creates, written+1)
			first, next := creates[0], creates[len(creates)-1]
			_, assigned := client.groups.get(key)
			if test.ended {
				require.Len(t, requestsFor(reads, vsr.OperationRegister), 2, "the refused bind replaces the session")
				require.Equal(t, uint64(130), next.sessionID())
				require.NotEqual(t, first.clientID(), next.clientID())
				require.False(t, assigned, "the ended session drops its group assignments")
				return
			}
			require.Len(t, requestsFor(reads, vsr.OperationRegister), 1, "the live session stays")
			require.Equal(t, uint64(129), next.sessionID())
			require.Equal(t, first.clientID(), next.clientID())
			require.True(t, assigned, "the live session keeps its group assignments")
		})
	}
}

// A route lookup that gets a deny with status 30 fails the call like a deny
// of the call itself, and the next link binds the live session again.
func TestFailover_StaleClientDenyOfARouteLookupKeepsTheSession(t *testing.T) {
	var denied atomic.Bool
	var server *testListener
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		switch {
		case read.code() == uint32(command.GetPartitionRouteCode) && denied.CompareAndSwap(false, true):
			return statusReplyFrame(read.operation(), uint32(ierror.StaleClientCode), nil)
		case read.code() == uint32(command.GetPartitionRouteCode):
			return pollRoutingReply(t, read, 0, 100)
		case read.operation() == vsr.OperationSendMessages:
			return replyFrame(read.operation(), zeroConfirmations())
		default:
			return singleNodeHandler(t, func() string { return server.address() })(connection, index, read)
		}
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	send := func() error {
		_, err := client.SendMessages(ctx, numericIdentifier(t, 1), numericIdentifier(t, 2),
			iggcon.PartitionId(0), []iggcon.IggyMessage{message})
		return err
	}

	require.ErrorIs(t, send(), ierror.ErrStaleClient)
	require.NoError(t, send())

	reads := server.recorded()
	require.Equal(t, 2, requestCount(reads, command.GetPartitionRouteCode))
	require.Equal(t, []uint64{128}, boundSessions(reads), "the next link binds the same session")
	registers := requestsFor(reads, vsr.OperationRegister)
	require.Len(t, registers, 1, "the live session stays")
	sends := requestsFor(reads, vsr.OperationSendMessages)
	require.Len(t, sends, 1)
	require.Equal(t, registers[0].clientID(), sends[0].clientID(), "the next write keeps its client id")
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
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
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
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
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
	assert.Equal(t, leader.address(), client.GetConnectionInfo().ServerAddress)
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
	client.links.roster = []string{survivor.address()}
	client.config.reconnection.enabled = false

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
	client.links.roster = []string{hangup.Addr().String()}

	require.Error(t, client.Connect(context.Background()))
	assert.Equal(t, configured, client.GetConnectionInfo().ServerAddress,
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
	client.links.roster = []string{"127.0.0.1:" + port}

	require.NoError(t, client.Connect(context.Background()))
	assert.Equal(t, "127.0.0.1:"+port, client.GetConnectionInfo().ServerAddress)
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
	client.links.roster = []string{survivor.address()}

	require.NoError(t, client.Connect(context.Background()))
	assert.Equal(t, survivor.address(), client.GetConnectionInfo().ServerAddress)
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
func deadAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}
