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
	"errors"
	"log/slog"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/require"
)

const (
	managedLinkTestBudget = 3 * time.Second
	managedLinkHeartbeat  = 20 * time.Millisecond
	managedLinkPoll       = 5 * time.Millisecond
	managedLinkSession    = uint64(128)
	// One ticker delivers at most one tick per interval. The slack covers a
	// tick buffered before the window and a ping still in transit at its start.
	heartbeatCadenceWindow = 10 * managedLinkHeartbeat
	heartbeatCadenceSlack  = 3
)

func TestManagedLinks_AdoptSingleReplicaBootstrapWithDifferentAddress(t *testing.T) {
	server := listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, managedLinkSession)
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, "127.0.0.1:1")
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	require.NotNil(t, client.links.replica(0), "the dialed bootstrap must fill the single replica slot")
	require.NoError(t, client.Ping(ctx))
	require.Equal(t, 1, server.connections())
}

func TestManagedLinks_KeepHeartbeatsOnHealthyReplicasAfterPrimaryLoss(t *testing.T) {
	var primaryLost atomic.Bool
	servers := make([]*testListener, 2)
	for replica := range servers {
		servers[replica] = listenVSR(t, nil, func(_, _ int, read request) []byte {
			if replica == 0 && primaryLost.Load() {
				return nil
			}
			switch {
			case read.operation() == vsr.OperationRegister:
				return registerReplyFrame(7, managedLinkSession)
			case read.code() == uint32(command.BindSessionCode):
				return bindReplyFrame(7, read.sessionID())
			case read.code() == uint32(command.GetClusterMetadataCode):
				return clusterMetadataFrame(t, 0, servers[0].address(), servers[1].address())
			default:
				return replyFrame(vsr.OperationNonReplicated, nil)
			}
		})
	}
	client := newDialingClient(t, servers[0].address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")),
		WithHeartbeatInterval(managedLinkHeartbeat))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	identity := client.session.ClientID()
	require.Eventually(t, func() bool {
		return requestCount(servers[0].recorded(), command.PingCode) >= 2 &&
			requestCount(servers[1].recorded(), command.PingCode) >= 2
	}, managedLinkTestBudget, managedLinkPoll)
	require.Equal(t, 1, servers[0].connections(), "the bootstrap becomes its replica link")
	require.Equal(t, 1, servers[1].connections(), "an unused replica connects proactively")

	before := requestCount(servers[1].recorded(), command.PingCode)
	primaryLost.Store(true)
	require.NoError(t, servers[0].listener.Close())
	require.Eventually(t, func() bool {
		return client.links.replica(0) == nil &&
			requestCount(servers[1].recorded(), command.PingCode) >= before+3
	}, managedLinkTestBudget, managedLinkPoll)
	require.Equal(t, identity, client.session.ClientID())
	require.Equal(t, managedLinkSession, client.session.SessionID())
	require.Equal(t, 1, servers[1].connections(), "healthy links stay open")

	registers := 0
	requestIDs := make(map[uint64]bool)
	for _, server := range servers {
		for _, read := range server.recorded() {
			require.Equal(t, identity, read.clientID())
			if read.operation() == vsr.OperationRegister {
				registers++
				continue
			}
			require.Equal(t, managedLinkSession, read.sessionID())
			require.NotZero(t, read.requestID())
			require.False(t, requestIDs[read.requestID()], "links share one unique request counter")
			requestIDs[read.requestID()] = true
		}
	}
	require.Equal(t, 1, registers, "primary socket loss must not register a new session")
}

type managedLinkTestConnection struct {
	net.Conn
	closed      chan<- struct{}
	readStopped chan<- struct{}
	resumeRead  <-chan struct{}
	resumeClose <-chan struct{}
}

func (connection *managedLinkTestConnection) Read(buffer []byte) (int, error) {
	count, err := connection.Conn.Read(buffer)
	if err != nil && connection.readStopped != nil {
		close(connection.readStopped)
		<-connection.resumeRead
	}
	return count, err
}

func (connection *managedLinkTestConnection) Close() error {
	err := connection.Conn.Close()
	select {
	case connection.closed <- struct{}{}:
	default:
	}
	if connection.resumeClose != nil {
		<-connection.resumeClose
	}
	return err
}

func TestSlowSocketCloseDoesNotHoldSessionLock(t *testing.T) {
	for _, action := range []string{"disconnect", "close", "failed_login"} {
		t.Run(action, func(t *testing.T) {
			peer, connection := net.Pipe()
			defer func() { _ = peer.Close() }()
			closing, resume := make(chan struct{}, 1), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			client := newTestClient(t, &managedLinkTestConnection{Conn: connection, closed: closing, resumeClose: resume})
			done := make(chan error, 1)
			go func() {
				switch action {
				case "disconnect":
					done <- client.Disconnect()
				case "close":
					done <- client.Close()
				default:
					_, err := client.LoginUser(context.Background(), "", "password")
					done <- err
				}
			}()
			select {
			case <-closing:
			case <-time.After(managedLinkTestBudget):
				t.Fatal("the socket did not start closing")
			}
			locked := client.mtx.TryLock()
			if locked {
				client.mtx.Unlock()
			}
			require.True(t, locked, "socket shutdown blocked the session lock")
			require.Nil(t, client.links.any())
			unblock()
			if action == "failed_login" {
				require.Error(t, <-done)
			} else {
				require.NoError(t, <-done)
			}
		})
	}
}

func TestManagedLinks_StopHeartbeatAndReconnectOnSessionEnd(t *testing.T) {
	for _, action := range []string{"logout", "disconnect", "close"} {
		t.Run(action, func(t *testing.T) {
			ended := make(chan struct{}, 1)
			var server *testListener
			server = listenVSR(t, func(connection net.Conn) net.Conn {
				return &managedLinkTestConnection{Conn: connection, closed: ended}
			}, func(_, _ int, read request) []byte {
				switch {
				case read.operation() == vsr.OperationRegister:
					return registerReplyFrame(7, managedLinkSession)
				case read.operation() == vsr.OperationLogout:
					return replyFrame(vsr.OperationLogout, nil)
				case read.code() == uint32(command.BindSessionCode):
					return bindReplyFrame(7, read.sessionID())
				case read.code() == uint32(command.GetClusterMetadataCode):
					return clusterMetadataFrame(t, 0, server.address())
				default:
					return replyFrame(vsr.OperationNonReplicated, nil)
				}
			})
			client := newDialingClient(t, server.address(),
				WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")),
				WithHeartbeatInterval(managedLinkHeartbeat))
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			require.NoError(t, client.Connect(ctx))
			require.Eventually(t, func() bool {
				return requestCount(server.recorded(), command.PingCode) >= 2
			}, managedLinkTestBudget, managedLinkPoll)
			switch action {
			case "logout":
				require.NoError(t, client.LogoutUser(ctx))
			case "disconnect":
				require.NoError(t, client.Disconnect())
			case "close":
				require.NoError(t, client.Close())
			}
			select {
			case <-ended:
			case <-ctx.Done():
				t.Fatal("the server connection outlived session termination")
			}
			requests := len(server.recorded())
			connections := server.connections()
			require.Never(t, func() bool {
				return len(server.recorded()) != requests || server.connections() != connections
			}, 3*managedLinkHeartbeat, managedLinkPoll,
				"terminated clients must stop heartbeat and reconnect tasks")
			require.False(t, client.session.Bound())
		})
	}
}

func TestManagedHeartbeatTerminalRefusalResetsSessionAndAssignments(t *testing.T) {
	var refuse atomic.Bool
	var ended atomic.Uint64
	var registers atomic.Uint64
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, managedLinkSession+registers.Add(1))
		case read.code() == uint32(command.BindSessionCode) && read.sessionID() == ended.Load():
			return statusReplyFrame(read.operation(), uint32(ierror.UnauthenticatedCode), nil)
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.code() == uint32(command.PingCode) && refuse.CompareAndSwap(true, false):
			ended.Store(read.sessionID())
			return statusReplyFrame(read.operation(), uint32(ierror.UnauthenticatedCode), nil)
		default:
			return replyFrame(read.operation(), nil)
		}
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")), WithHeartbeatInterval(managedLinkHeartbeat))
	require.NoError(t, client.Connect(context.Background()))
	client.metadataWatermark.Store(42)
	initial := client.session.ClientID()
	key := groupKey{stream: "s", topic: "t", group: "g"}
	client.groups.put(key, groupAssignment{generation: 1, partitions: []uint32{0}, fetchedAt: time.Now()})
	refuse.Store(true)
	require.Eventually(t, func() bool {
		client.mtx.Lock()
		defer client.mtx.Unlock()
		return client.session.Bound() && client.session.ClientID() != initial
	}, managedLinkTestBudget, managedLinkPoll)
	_, cached := client.groups.get(key)
	require.False(t, cached)
	before := requestCount(server.recorded(), command.PingCode)
	require.Eventually(t, func() bool { return requestCount(server.recorded(), command.PingCode) > before }, managedLinkTestBudget, managedLinkPoll)
	require.Equal(t, uint64(2), registers.Load())
	require.Equal(t, uint64(42), client.metadataWatermark.Load())
}

// Only the managed owner sends keepalive. The owner of this client never runs,
// so a ping after an idle gap longer than the interval would come from a call.
func TestManagedHeartbeat_CallerExchangesSendNoKeepalive(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.heartbeatInterval = managedLinkHeartbeat
	server := servePartitionOperations(t, serverConn, func(_ int, read request) []byte {
		if read.operation() == vsr.OperationNonReplicated {
			return replyFrame(read.operation(), nil)
		}
		return replyFrame(read.operation(), resultSection())
	})
	stream, topic, group := groupConsumer(t)
	requests := []command.Command{
		rawRequest{code: command.SendMessagesCode, payload: rawSendPayload(t, stream, topic, iggcon.PartitionId(0))},
		&command.PollMessages{StreamId: stream, TopicId: topic, Consumer: iggcon.NewSingleConsumer(group.Id), Strategy: iggcon.NextPollingStrategy(), Count: 1},
		&command.GetStreams{},
	}
	for _, request := range requests {
		time.Sleep(2 * managedLinkHeartbeat)
		_, err := client.do(context.Background(), request)
		require.NoError(t, err)
	}
	require.Zero(t, requestCount(server.recorded(), command.PingCode))
}

// One managed owner pings the links of a session. A repeated Connect keeps it,
// and an explicit sign-in or a session recovery replaces it: the ended session
// hears no more pings, and the next one hears them at the cadence of one owner.
func TestManagedHeartbeat_OneOwnerPerSessionAcrossResets(t *testing.T) {
	server, lost := lostSessionServer(t, func(number int, _ request) []byte {
		return registerReplyFrame(7, managedLinkSession+uint64(number))
	})
	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")),
		WithHeartbeatInterval(managedLinkHeartbeat))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	pings := func(register int) int {
		count := 0
		for _, read := range server.recorded() {
			if read.code() == uint32(command.PingCode) && read.sessionID() == managedLinkSession+uint64(register) {
				count++
			}
		}
		return count
	}
	requireOneOwner := func(register int) {
		t.Helper()
		require.Eventually(t, func() bool { return pings(register) > 0 }, managedLinkTestBudget, managedLinkPoll,
			"session %d has no heartbeat", register)
		started, before := time.Now(), pings(register)
		time.Sleep(heartbeatCadenceWindow)
		heard := pings(register) - before
		require.LessOrEqual(t, heard, int(time.Since(started)/managedLinkHeartbeat)+heartbeatCadenceSlack,
			"session %d is pinged by more than one owner", register)
	}
	requireSilent := func(register int) {
		t.Helper()
		heard := pings(register)
		require.Never(t, func() bool { return pings(register) != heard }, 3*managedLinkHeartbeat, managedLinkPoll,
			"the heartbeat of ended session %d still runs", register)
	}

	require.NoError(t, client.Connect(ctx))
	require.NoError(t, client.Connect(ctx))
	requireOneOwner(1)

	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)
	requireSilent(1)
	requireOneOwner(2)

	lost.Store(true)
	require.Eventually(t, func() bool { return len(registerRequests(server)) == 3 && sessionBound(client) },
		managedLinkTestBudget, managedLinkPoll, "the refused ping starts a recovery")
	requireSilent(2)
	requireOneOwner(3)
}

func TestDisconnectCancelsRecoveryWaitingToConnect(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		return singleNodeHandler(t, func() string { return server.address() })(connection, index, read)
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "secret")))
	require.NoError(t, client.Connect(context.Background()))
	fence := client.sessionFence
	client.connectMu.Lock()
	locked := true
	defer func() {
		if locked {
			client.connectMu.Unlock()
		}
	}()
	client.recoverSession(fence)
	disconnected := make(chan struct{})
	go func() { _ = client.Disconnect(); close(disconnected) }()
	require.Eventually(t, func() bool {
		client.mtx.Lock()
		defer client.mtx.Unlock()
		return client.sessionFence >= fence+2
	}, time.Second, time.Millisecond)
	client.connectMu.Unlock()
	locked = false
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("disconnect did not finish after recovery was released")
	}
	require.Never(t, func() bool { return server.connections() > 1 }, 3*managedLinkHeartbeat, managedLinkPoll,
		"a session recovery queued before disconnect must not create another lifetime")
	require.False(t, client.session.Bound())
}

func TestSessionRecoveryIgnoresAlreadyDisconnectedSession(t *testing.T) {
	client, _ := newPipeClient(t)
	require.NoError(t, client.Disconnect())
	fence := client.sessionFence
	client.recoverSession(fence)
	client.mtx.Lock()
	defer client.mtx.Unlock()
	require.Equal(t, fence, client.sessionFence)
	require.Nil(t, client.recoveryCancel)
}

func TestManagedLinksNeverUseFollowerForMetadataWhenRosterHasNoEligiblePrimary(t *testing.T) {
	client, _ := newPipeClient(t)
	client.links.mu.Lock()
	client.links.hasPrimary = false
	client.links.mu.Unlock()
	require.NotNil(t, client.links.any())
	require.Nil(t, client.links.metadata())
	metadata := iggcon.ClusterMetadata{Nodes: make([]iggcon.ClusterNode, maxReplicaSlots+1)}
	require.ErrorIs(t, client.links.install(&metadata), ierror.ErrInvalidCommand)
	require.Len(t, client.links.slots, 1, "invalid discovery cannot replace healthy slots")
}

func TestManagedLinksLimitConcurrentDialsAcrossReplicas(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	accepted := make(chan net.Conn, maxConcurrentDials+1)
	var acceptors sync.WaitGroup
	acceptors.Add(1)
	go func() {
		defer acceptors.Done()
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- connection
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		acceptors.Wait()
		close(accepted)
		for connection := range accepted {
			_ = connection.Close()
		}
	})
	client := NewIggyTcpClient(nil, WithTLS(WithTLSValidateCertificate(false)))
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	completed := make(chan error, maxConcurrentDials)
	for range maxConcurrentDials {
		go func() { _, err := client.links.dial(ctx, listener.Addr().String()); completed <- err }()
	}
	require.Eventually(t, func() bool { return len(accepted) == maxConcurrentDials }, time.Second, time.Millisecond)
	waiting, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	_, err = client.links.dial(waiting, listener.Addr().String())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, accepted, maxConcurrentDials, "the fifth replica waits before opening a socket")
	cancel()
	for range maxConcurrentDials {
		require.Error(t, <-completed)
	}
	require.Empty(t, client.links.permits)
}

func TestManagedLinks_RedialSingleNodeAtConfiguredEndpoint(t *testing.T) {
	// A port map in front of the node, which advertises an address that the
	// client cannot reach.
	unreachable, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	advertised := unreachable.Addr().String()
	require.NoError(t, unreachable.Close())
	server := listenVSR(t, nil, singleNodeHandler(t, func() string { return advertised }))
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	adopted := client.links.replica(0)
	require.NotNil(t, adopted)

	adopted.close(ierror.ErrTransientNotCommitted)
	require.Eventually(t, func() bool {
		link := client.links.replica(0)
		return link != nil && link != adopted
	}, managedLinkTestBudget, managedLinkPoll, "the slot redials the configured endpoint")
	redialed := client.links.replica(0)
	_, err = client.GetClusterMetadata(ctx)
	require.NoError(t, err)

	require.Same(t, redialed, client.links.replica(0), "a roster refresh keeps the slot")
	require.Equal(t, server.address(), redialed.endpoint)
	require.Equal(t, server.address(), client.GetConnectionInfo().ServerAddress)
	require.Equal(t, 2, server.connections())
	require.NoError(t, client.Ping(ctx))
}

// registerRequests returns the sign-ins that the servers saw.
func registerRequests(servers ...*testListener) []request {
	var registers []request
	for _, server := range servers {
		for _, read := range server.recorded() {
			if read.operation() == vsr.OperationRegister {
				registers = append(registers, read)
			}
		}
	}
	return registers
}

func sessionBound(client *IggyTcpClient) bool {
	client.mtx.Lock()
	defer client.mtx.Unlock()
	return client.session.Bound()
}

// lostSessionServer is a single node. After lost is set, its next ping finds
// the session gone, so the client recovers the session with a new sign-in.
// register answers the sign-in with the given number, which starts at one.
func lostSessionServer(t *testing.T, register func(number int, read request) []byte) (*testListener, *atomic.Bool) {
	t.Helper()
	var lost atomic.Bool
	var ended atomic.Pointer[vsr.ClientID]
	var count atomic.Int32
	var server *testListener
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationRegister:
			return register(int(count.Add(1)), read)
		case read.code() == uint32(command.BindSessionCode) && ended.Load() != nil && read.clientID() == *ended.Load():
			return statusReplyFrame(read.operation(), uint32(ierror.UnauthenticatedCode), nil)
		case read.code() == uint32(command.PingCode) && lost.CompareAndSwap(true, false):
			identity := read.clientID()
			ended.Store(&identity)
			return statusReplyFrame(read.operation(), uint32(ierror.UnauthenticatedCode), nil)
		default:
			return singleNodeHandler(t, func() string { return server.address() })(connection, index, read)
		}
	})
	return server, &lost
}

func TestSessionRecovery_SignsInAgainAfterItsSignInLostItsLink(t *testing.T) {
	server, lost := lostSessionServer(t, func(number int, _ request) []byte {
		if number == 2 {
			return nil
		}
		return registerReplyFrame(7, managedLinkSession+uint64(number))
	})
	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")),
		WithHeartbeatInterval(managedLinkHeartbeat))
	client.config.reconnection.maxRetries = 0
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))

	lost.Store(true)

	require.Eventually(t, func() bool { return len(registerRequests(server)) == 3 && sessionBound(client) },
		managedLinkTestBudget, managedLinkPoll, "the lost sign-in goes again")
	require.NoError(t, client.Ping(ctx))
}

// The heartbeat is off, so only the call can find each session gone. Its
// refused rebind starts the recovery, also after an earlier one finished, and
// the call completes under the new session.
func TestSessionRecovery_ACallStartsEachRecoveryWithoutAHeartbeat(t *testing.T) {
	server, lost := lostSessionServer(t, func(number int, _ request) []byte {
		return registerReplyFrame(7, managedLinkSession+uint64(number))
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))

	for registers := 2; registers <= 3; registers++ {
		lost.Store(true)
		require.NoError(t, client.Ping(ctx), "the call that found session %d gone recovers it", registers-1)
		require.Len(t, registerRequests(server), registers)
		require.True(t, sessionBound(client))
	}
	require.NoError(t, client.Ping(ctx))
}

// A recovery overtakes a roster read in flight. The read starts again under the
// session of the recovery and installs the roster that it reads there.
func TestSessionRecovery_ARosterReadRestartsUnderTheRecoveredSession(t *testing.T) {
	var held atomic.Bool
	received, release := make(chan struct{}), make(chan struct{})
	var server *testListener
	node := singleNodeHandler(t, func() string { return server.address() })
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) && held.CompareAndSwap(true, false) {
			close(received)
			<-release
			return nil
		}
		return node(connection, index, read)
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	held.Store(true)
	done := make(chan error, 1)
	go func() { _, err := client.GetClusterMetadata(ctx); done <- err }()
	<-received

	recoverNow(client)
	close(release)

	require.NoError(t, <-done)
	require.Len(t, registerRequests(server), 2, "the recovery signed in again")
	require.True(t, sessionBound(client))
}

func TestSessionRecovery_StopsWhenTheServerRejectsTheCredentials(t *testing.T) {
	server, lost := lostSessionServer(t, func(number int, _ request) []byte {
		if number > 1 {
			return evictionFrame(vsr.EvictionInvalidCredentials, 0, 0)
		}
		return registerReplyFrame(7, managedLinkSession)
	})
	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")),
		WithHeartbeatInterval(managedLinkHeartbeat))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))

	lost.Store(true)

	require.Eventually(t, func() bool { return len(registerRequests(server)) == 2 }, managedLinkTestBudget, managedLinkPoll)
	awaitRecoveryEnd(t, client)
	require.Never(t, func() bool { return len(registerRequests(server)) > 2 }, 5*managedLinkHeartbeat, managedLinkPoll,
		"a rejected credential stays rejected")
	require.False(t, sessionBound(client))
}

func TestSessionRecovery_StopsOnDisconnectOrClose(t *testing.T) {
	for _, action := range []string{"disconnect", "close"} {
		t.Run(action, func(t *testing.T) {
			var answer atomic.Bool
			server, lost := lostSessionServer(t, func(number int, _ request) []byte {
				if number > 1 && !answer.Load() {
					return nil
				}
				return registerReplyFrame(7, managedLinkSession+uint64(number))
			})
			client := newDialingClient(t, server.address(),
				WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")),
				WithHeartbeatInterval(managedLinkHeartbeat))
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			require.NoError(t, client.Connect(ctx))
			lost.Store(true)
			require.Eventually(t, func() bool { return len(registerRequests(server)) >= 2 }, managedLinkTestBudget, managedLinkPoll)

			if action == "disconnect" {
				require.NoError(t, client.Disconnect())
			} else {
				require.NoError(t, client.Close())
			}
			answer.Store(true)

			require.Never(t, func() bool { return sessionBound(client) }, 5*managedLinkHeartbeat, managedLinkPoll,
				"an explicit end stops the automatic sign-in")
		})
	}
}

// registerUsername reads the username of a Register body, which is [protocol
// version u32][sdk name][sdk version][bind secret][username][password], where
// a name is [length u8][bytes].
func registerUsername(payload []byte) string {
	offset := 4
	for range 2 {
		if offset >= len(payload) {
			return ""
		}
		offset += 1 + int(payload[offset])
	}
	offset += vsr.BindSecretBytes
	if offset >= len(payload) || offset+1+int(payload[offset]) > len(payload) {
		return ""
	}
	return string(payload[offset+1 : offset+1+int(payload[offset])])
}

func TestSessionRecovery_ExplicitSignInTakesOverALateAutomaticSignIn(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseReply := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseReply)
	var usersMtx sync.Mutex
	users := make(map[vsr.ClientID]string)
	server, lost := lostSessionServer(t, func(number int, read request) []byte {
		username := registerUsername(read.payload)
		usersMtx.Lock()
		owner, registered := users[read.clientID()]
		if !registered {
			users[read.clientID()] = username
		}
		usersMtx.Unlock()
		// As on a server, a client id keeps the user that registered it.
		if registered && owner != username {
			return evictionFrame(vsr.EvictionSessionError, 0, 0)
		}
		userID := uint32(7)
		if username == "other" {
			userID = 8
		}
		reply := registerReplyFrame(userID, managedLinkSession+uint64(number))
		// The server registers the automatic sign-in at once, but its reply
		// waits.
		if number == 2 {
			<-release
		}
		return reply
	})
	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")),
		WithHeartbeatInterval(managedLinkHeartbeat))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	lost.Store(true)
	require.Eventually(t, func() bool { return len(registerRequests(server)) == 2 }, managedLinkTestBudget, managedLinkPoll)

	identity, err := client.LoginUser(ctx, "other", "password")
	require.NoError(t, err, "the sign-in of the caller succeeds without waiting for the automatic one")
	releaseReply()

	require.Equal(t, uint32(8), identity.UserId)
	registers := registerRequests(server)
	require.Len(t, registers, 3)
	require.Equal(t, "other", registerUsername(registers[2].payload))
	require.NotEqual(t, registers[1].clientID(), registers[2].clientID())
	signedIn := func() (uint64, uint32) {
		client.mtx.Lock()
		defer client.mtx.Unlock()
		return client.session.SessionID(), client.sessionUserID
	}
	require.Never(t, func() bool {
		session, user := signedIn()
		return session != managedLinkSession+3 || user != 8
	}, 5*managedLinkHeartbeat, managedLinkPoll, "the late reply to the automatic sign-in does not bind")
	require.True(t, sessionBound(client))
}

func TestSessionRecovery_ExplicitSignInEndsTheAutomaticOneEvenWhenItFails(t *testing.T) {
	var server *testListener
	handler := singleNodeHandler(t, func() string { return server.address() })
	server = listenVSR(t, nil, handler)
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	release := holdRecovery(t, client)
	recoverNow(client)
	require.NoError(t, server.listener.Close())

	client.mtx.Lock()
	recovery := client.recovery
	client.mtx.Unlock()
	require.NotNil(t, recovery)

	_, err := client.LoginUser(ctx, "other", "password")
	require.ErrorIs(t, err, ierror.ErrCannotEstablishConnection, "the sign-in fails before its Register")
	healed := listenVSRAt(t, server.address(), nil, handler)
	release()
	require.ErrorIs(t, recovery.Err(), context.Canceled, "the caller ended the captured automatic recovery")

	signIns := func() []string {
		var usernames []string
		for _, read := range registerRequests(server, healed) {
			usernames = append(usernames, registerUsername(read.payload))
		}
		return usernames
	}
	require.Equal(t, []string{"iggy"}, signIns(), "the recovery does not sign in again as the last user")
	require.False(t, sessionBound(client))
	_, err = client.LoginUser(ctx, "other", "password")
	require.NoError(t, err)
	require.Equal(t, []string{"iggy", "other"}, signIns())
	require.True(t, sessionBound(client))
}

// The caller signs in as another user than the configured one. After a session
// loss, the recovery signs in as the caller's sign-in.
func TestSessionRecovery_SignsInAsTheLastSignInOfTheCaller(t *testing.T) {
	server, lost := lostSessionServer(t, func(number int, _ request) []byte {
		return registerReplyFrame(7, managedLinkSession+uint64(number))
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "other", "password")
	require.NoError(t, err)

	lost.Store(true)
	require.NoError(t, client.Ping(ctx), "the ping completes under the recovered session")

	var signIns []string
	for _, read := range registerRequests(server) {
		signIns = append(signIns, registerUsername(read.payload))
	}
	require.Equal(t, []string{"iggy", "other", "other"}, signIns)
}

// A create finds its session gone and waits for the recovery, and a second
// create waits for its lane. The caller then signs in as another user. Both
// creates started under the earlier sign-in, so they end with 40 at once and
// never go out under the new session.
func TestSessionRecovery_ACallerSignInEndsTheCallsOfTheEarlierSignIn(t *testing.T) {
	sent, queued := make(chan struct{}), make(chan struct{})
	var ended atomic.Pointer[vsr.ClientID]
	var registers atomic.Uint64
	var server *testListener
	node := singleNodeHandler(t, func() string { return server.address() })
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, managedLinkSession+registers.Add(1))
		case read.code() == uint32(command.BindSessionCode) && ended.Load() != nil && read.clientID() == *ended.Load():
			return statusReplyFrame(read.operation(), uint32(ierror.UnauthenticatedCode), nil)
		case read.operation() == vsr.OperationCreateStream && ended.Load() == nil:
			close(sent)
			<-queued
			identity := read.clientID()
			ended.Store(&identity)
			return statusReplyFrame(read.operation(), uint32(ierror.UnauthenticatedCode), nil)
		case read.operation() == vsr.OperationCreateStream:
			return committedReply(read, 0)
		default:
			return node(connection, index, read)
		}
	})
	client := newDialingClient(t, server.address())
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	_, err := client.LoginUser(ctx, "iggy", "iggy")
	require.NoError(t, err)
	release := holdRecovery(t, client)
	create := func(name string, result chan<- error) {
		_, err := client.do(ctx, &command.CreateStream{Name: name})
		result <- err
	}
	recovering, queuing := make(chan error, 1), make(chan error, 1)
	go create("recovering", recovering)
	<-sent
	go create("queuing", queuing)
	require.Eventually(t, func() bool {
		client.operations.mu.Lock()
		defer client.operations.mu.Unlock()
		return len(client.operations.lanes[laneKey{}].waiters) == 1
	}, managedLinkTestBudget, managedLinkPoll)
	close(queued)
	require.Eventually(t, func() bool {
		client.mtx.Lock()
		defer client.mtx.Unlock()
		return client.recovery != nil
	}, managedLinkTestBudget, managedLinkPoll, "the first create found its session gone")
	require.Never(t, func() bool { return len(recovering) != 0 }, recoveryWaitDeadline, managedLinkPoll,
		"the first create waits for the recovery")

	client.mtx.Lock()
	recovery := client.recovery
	client.mtx.Unlock()
	require.NotNil(t, recovery)

	_, err = client.LoginUser(ctx, "other", "password")
	require.NoError(t, err)

	for _, call := range []chan error{recovering, queuing} {
		select {
		case err := <-call:
			require.ErrorIs(t, err, ierror.ErrUnauthenticated)
		case <-ctx.Done():
			t.Fatal("a call of the earlier sign-in still waits")
		}
	}
	release()
	require.ErrorIs(t, recovery.Err(), context.Canceled, "the caller ended the captured automatic recovery")
	creates := requestsFor(server.recorded(), vsr.OperationCreateStream)
	require.Len(t, creates, 1, "only the first create went out")
	require.Equal(t, managedLinkSession+1, creates[0].sessionID(), "under the earlier session")
}

// A metadata mutation commits while an offset store waits for LifecycleBusy.
// The mutation invalidates its cached route, so the store's next lookups wait
// for a route refused under the first session, with no store in flight. The
// caller then logs out and signs in as another user, disconnects and connects
// again, or the server refuses a recovery and the caller explicitly signs in.
// The store started under the first session, so it ends with 40 and never
// goes out under a later one.
func TestSessionRecovery_ALaterSessionEndsACallBetweenAttempts(t *testing.T) {
	for _, reopen := range []struct {
		name    string
		session func(ctx context.Context, t *testing.T, client *IggyTcpClient, refuse *atomic.Bool) error
	}{
		{"logout_then_sign_in", func(ctx context.Context, _ *testing.T, client *IggyTcpClient, _ *atomic.Bool) error {
			if err := client.LogoutUser(ctx); err != nil {
				return err
			}
			_, err := client.LoginUser(ctx, "another", "password")
			return err
		}},
		{"disconnect_then_connect", func(ctx context.Context, _ *testing.T, client *IggyTcpClient, _ *atomic.Bool) error {
			if err := client.Disconnect(); err != nil {
				return err
			}
			return client.Connect(ctx)
		}},
		{"refused_recovery_then_explicit_sign_in", func(ctx context.Context, t *testing.T, client *IggyTcpClient, refuse *atomic.Bool) error {
			refuse.Store(true)
			recoverNow(client)
			awaitRecoveryEnd(t, client)
			if err := client.Connect(ctx); err != nil {
				return err
			}
			require.False(t, sessionBound(client), "a refused credential requires an explicit sign-in")
			_, err := client.LoginUser(ctx, "iggy", "iggy")
			return err
		}},
	} {
		t.Run(reopen.name, func(t *testing.T) {
			// The connect signs in as iggy and the caller as other.
			const firstSession = managedLinkSession + 2
			parked := make(chan struct{})
			park := sync.OnceFunc(func() { close(parked) })
			var busy, refuse atomic.Bool
			var registers atomic.Uint64
			received, resume := make(chan struct{}), make(chan struct{})
			receive := sync.OnceFunc(func() { close(received) })
			release := sync.OnceFunc(func() { close(resume) })
			defer release()
			fixture := newPrimaryPollFixture(t, func(_, _ int, read request) ([]byte, bool) {
				if read.operation() != vsr.OperationStoreConsumerOffset {
					return nil, false
				}
				if read.sessionID() == firstSession {
					busy.Store(true)
					receive()
					<-resume
					return committedReply(read, ierror.LifecycleBusyCode), true
				}
				return committedReply(read, 0), true
			}, func(_ int, read request) ([]byte, bool) {
				switch {
				case read.operation() == vsr.OperationRegister:
					number := registers.Add(1)
					if refuse.Load() && registerUsername(read.payload) == "other" {
						return evictionFrame(vsr.EvictionSessionError, 0, 0), true
					}
					return registerReplyFrame(7, managedLinkSession+number), true
				case read.code() == uint32(command.GetPartitionRouteCode) && read.sessionID() == firstSession && busy.Load():
					park()
					return statusReplyFrame(read.operation(), uint32(ierror.TransientNotAcceptedCode), nil), true
				case read.code() == uint32(command.GetPartitionRouteCode):
					return routeReply(t, read, 1), true
				default:
					return nil, false
				}
			})
			client := fixture.client
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			_, err := client.LoginUser(ctx, "other", "password")
			require.NoError(t, err)
			stream, topic, consumer := groupConsumer(t)
			partition := uint32(1)
			stored := make(chan error, 1)
			go func() { stored <- client.StoreConsumerOffset(ctx, consumer, stream, topic, 42, &partition) }()
			select {
			case <-received:
			case <-ctx.Done():
				t.Fatal("the first store did not reach its primary")
			}
			require.NoError(t, client.CreatePartitions(ctx, stream, topic, 1))
			release()
			select {
			case <-parked:
			case <-ctx.Done():
				t.Fatal("the store never looked up its route again")
			}

			require.NoError(t, reopen.session(ctx, t, client, &refuse))
			require.True(t, sessionBound(client), "the caller opened a later session")

			select {
			case err := <-stored:
				require.ErrorIs(t, err, ierror.ErrUnauthenticated)
			case <-ctx.Done():
				t.Fatal("the store still waits")
			}
			reads := append(fixture.coordinator.recorded(), fixture.primaries[0].recorded()...)
			reads = append(reads, fixture.primaries[1].recorded()...)
			stores := requestsFor(reads, vsr.OperationStoreConsumerOffset)
			require.Len(t, stores, 1, "only the first attempt of the store went out")
			require.Equal(t, firstSession, stores[0].sessionID())
		})
	}
}

// errorLog records the errors that a client logs.
type errorLog struct {
	mtx    sync.Mutex
	errors []error
}

func (l *errorLog) Enabled(context.Context, slog.Level) bool { return true }
func (l *errorLog) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *errorLog) WithGroup(string) slog.Handler            { return l }
func (l *errorLog) Handle(_ context.Context, record slog.Record) error {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	record.Attrs(func(attr slog.Attr) bool {
		if err, ok := attr.Value.Any().(error); ok {
			l.errors = append(l.errors, err)
		}
		return true
	})
	return nil
}

func (l *errorLog) holds(target error) bool {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	return slices.ContainsFunc(l.errors, func(err error) bool { return errors.Is(err, target) })
}

// The server refuses the credentials of the sign-in of a recovery with an
// eviction or with a status. A create and a read wait for the recovery, and
// both end with 40, also when no link is left, with the refusal as their
// cause. The refusal blocks automatic sign-in until the caller explicitly
// signs in again.
func TestSessionRecovery_ARefusedSignInEndsTheCallsThatWaitForIt(t *testing.T) {
	// The connect signs in as iggy and the caller as other, so the recovery
	// sends the third sign-in.
	const recoverySignIn = 3
	for _, refusal := range []struct {
		name   string
		answer []byte
		cause  error
		token  bool
	}{
		{"evicted_invalid_credentials", evictionFrame(vsr.EvictionInvalidCredentials, 0, 0), ierror.ErrInvalidCredentials, false},
		{"evicted_user_inactive", evictionFrame(vsr.EvictionUserInactive, 0, 0), ierror.ErrUnauthenticated, false},
		{"evicted_session_error", evictionFrame(vsr.EvictionSessionError, 0, 0), ierror.ErrUnauthenticated, false},
		{"evicted_invalid_token", evictionFrame(vsr.EvictionInvalidToken, 0, 0), ierror.ErrInvalidPersonalAccessToken, true},
		{"status_invalid_credentials", statusReplyFrame(vsr.OperationRegister, uint32(ierror.InvalidCredentialsCode), nil),
			ierror.ErrInvalidCredentials, false},
		{"status_invalid_token", statusReplyFrame(vsr.OperationRegister, uint32(ierror.InvalidPersonalAccessTokenCode), nil),
			ierror.ErrInvalidPersonalAccessToken, true},
		{"status_expired_token", statusReplyFrame(vsr.OperationRegister, uint32(ierror.PersonalAccessTokenExpiredCode), nil),
			ierror.ErrPersonalAccessTokenExpired, true},
		{"status_invalid_format", statusReplyFrame(vsr.OperationRegister, uint32(ierror.InvalidFormatCode), nil),
			ierror.ErrInvalidFormat, false},
		{"status_invalid_username", statusReplyFrame(vsr.OperationRegister, uint32(ierror.InvalidUsernameCode), nil),
			ierror.ErrInvalidUsername, false},
		{"status_invalid_password", statusReplyFrame(vsr.OperationRegister, uint32(ierror.InvalidPasswordCode), nil),
			ierror.ErrInvalidPassword, false},
		{"status_user_inactive", statusReplyFrame(vsr.OperationRegister, uint32(ierror.UserInactiveCode), nil),
			ierror.ErrUserInactive, false},
		{"status_unauthenticated", statusReplyFrame(vsr.OperationRegister, uint32(ierror.UnauthenticatedCode), nil),
			ierror.ErrUnauthenticated, false},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			server, _ := lostSessionServer(t, func(number int, _ request) []byte {
				if number == recoverySignIn {
					return refusal.answer
				}
				return registerReplyFrame(7, managedLinkSession+uint64(number))
			})
			client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
			logged := &errorLog{}
			client.logger = slog.New(logged)
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			require.NoError(t, client.Connect(ctx))
			caller := "other"
			if refusal.token {
				caller = "refused-token"
				_, err := client.LoginWithPersonalAccessToken(ctx, caller)
				require.NoError(t, err)
			} else {
				_, err := client.LoginUser(ctx, caller, "password")
				require.NoError(t, err)
			}
			release := holdRecovery(t, client)
			recoverNow(client)
			// The calls take their sign-in epoch here, before the refusal.
			callCtx, endCalls := client.operationContext(ctx)
			defer endCalls()
			created, pinged := make(chan error, 1), make(chan error, 1)
			go func() {
				_, err := client.do(callCtx, &command.CreateStream{Name: "waiting"})
				created <- err
			}()
			go func() { pinged <- client.Ping(callCtx) }()
			require.Never(t, func() bool { return len(created) != 0 || len(pinged) != 0 }, recoveryWaitDeadline, managedLinkPoll,
				"the calls wait for the recovery")

			release()
			awaitRecoveryEnd(t, client)

			for _, call := range []chan error{created, pinged} {
				select {
				case err := <-call:
					require.ErrorIs(t, err, ierror.ErrUnauthenticated)
					if !errors.Is(err, refusal.cause) {
						t.Errorf("waiting call lost sign-in refusal %v: %v", refusal.cause, err)
					}
				case <-ctx.Done():
					t.Fatal("a call still waits")
				}
			}
			require.True(t, logged.holds(refusal.cause), "the log keeps the refusal")
			require.Empty(t, requestsFor(server.recorded(), vsr.OperationCreateStream), "the create never went out")
			require.NoError(t, client.Connect(ctx))
			require.NoError(t, client.Disconnect())
			require.NoError(t, client.Connect(ctx))
			require.Len(t, registerRequests(server), recoverySignIn, "a refused credential must not fall back to AutoLogin")
			require.False(t, sessionBound(client))
			_, err := client.LoginUser(ctx, "retry", "password")
			require.NoError(t, err)
			var signIns []string
			for _, read := range registerRequests(server) {
				signIns = append(signIns, registerUsername(read.payload))
			}
			require.Equal(t, []string{"iggy", caller, caller, "retry"}, signIns)
		})
	}
}

// The server refuses the sign-in of a recovery for a reason other than its
// credentials. The session stays owed, so the calls that wait for the recovery
// keep waiting and end with 58 with their context, and the recovery signs in
// again as the caller once the server accepts it.
func TestSessionRecovery_ASignInRefusedForAnotherReasonKeepsTheSessionOwed(t *testing.T) {
	// The connect signs in as iggy and the caller as other, so the recovery
	// sends the third sign-in.
	const recoverySignIn = 3
	newerProtocol := vsr.ProtocolVersion + 1
	for _, refusal := range []struct {
		name   string
		answer []byte
		cause  error
	}{
		{"evicted_incompatible_protocol", evictionFrame(vsr.EvictionIncompatibleProtocol, newerProtocol, newerProtocol),
			ierror.ErrIncompatibleProtocolVersion},
		{"evicted_release_too_low", evictionFrame(vsr.EvictionClientReleaseTooLow, 0, 0), ierror.ErrInvalidCommand},
		{"status_stale_client", statusReplyFrame(vsr.OperationRegister, uint32(ierror.StaleClientCode), nil), ierror.ErrStaleClient},
	} {
		t.Run(refusal.name, func(t *testing.T) {
			var refuse atomic.Bool
			refuse.Store(true)
			server, _ := lostSessionServer(t, func(number int, _ request) []byte {
				if number >= recoverySignIn && refuse.Load() {
					// Each answer goes out on its own link and gets its own request id.
					return slices.Clone(refusal.answer)
				}
				return registerReplyFrame(7, managedLinkSession+uint64(number))
			})
			client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
			logged := &errorLog{}
			client.logger = slog.New(logged)
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			require.NoError(t, client.Connect(ctx))
			_, err := client.LoginUser(ctx, "other", "password")
			require.NoError(t, err)
			epoch := client.epoch.Load()
			release := holdRecovery(t, client)
			recoverNow(client)
			// Ending the wait stands in for the deadline of the calls.
			wait, endWait := context.WithCancel(ctx)
			defer endWait()
			callCtx, endCalls := client.operationContext(wait)
			defer endCalls()
			created, pinged := make(chan error, 1), make(chan error, 1)
			go func() {
				_, err := client.do(callCtx, &command.CreateStream{Name: "waiting"})
				created <- err
			}()
			go func() { pinged <- client.Ping(callCtx) }()

			release()
			require.Eventually(t, func() bool { return logged.holds(refusal.cause) }, managedLinkTestBudget, managedLinkPoll,
				"the log keeps the refusal")
			require.Never(t, func() bool { return len(created) != 0 || len(pinged) != 0 }, recoveryWaitDeadline, managedLinkPoll,
				"the calls still wait for the owed session")
			endWait()
			for _, call := range []chan error{created, pinged} {
				select {
				case err := <-call:
					require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
				case <-ctx.Done():
					t.Fatal("a call still waits")
				}
			}
			require.Same(t, epoch, client.epoch.Load(), "the refusal keeps the epoch of the caller")
			require.Empty(t, requestsFor(server.recorded(), vsr.OperationCreateStream), "the create never went out")

			refuse.Store(false)
			require.Eventually(t, func() bool { return sessionBound(client) }, managedLinkTestBudget, managedLinkPoll,
				"the recovery signs in again")
			require.NoError(t, client.Ping(context.Background()))
			var signIns []string
			for _, read := range registerRequests(server) {
				signIns = append(signIns, registerUsername(read.payload))
			}
			require.Greater(t, len(signIns), recoverySignIn, "the recovery signs in again after the refusal")
			require.Equal(t, "iggy", signIns[0])
			for _, signIn := range signIns[1:] {
				require.Equal(t, "other", signIn, "the recovery signs in as the caller")
			}
		})
	}
}

func TestManagedLinksIgnoreDisabledTransportEndpoints(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))
	frame := clusterMetadataFrame(t, 0, server.address())
	var metadata iggcon.ClusterMetadata
	require.NoError(t, metadata.UnmarshalBinary(frame[vsr.HeaderSize:]))
	metadata.Nodes = append(metadata.Nodes, iggcon.ClusterNode{ReplicaID: 1, IP: "127.0.0.1", Role: iggcon.RoleFollower, Status: iggcon.Healthy})
	require.NoError(t, client.links.install(&metadata))
	require.False(t, client.links.contains(1))
	require.Len(t, client.links.slots, 1)
}

func TestManagedHeartbeatRestartsExhaustedReplicaRetries(t *testing.T) {
	var down atomic.Bool
	var server *testListener
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		if down.Load() {
			return nil
		}
		return singleNodeHandler(t, func() string { return server.address() })(connection, index, read)
	})
	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")),
		WithHeartbeatInterval(managedLinkHeartbeat))
	client.config.reconnection.maxRetries = 1
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	down.Store(true)
	require.Eventually(t, func() bool { return server.connections() >= 4 },
		managedLinkTestBudget, managedLinkPoll, "heartbeats restart exhausted slots")
	down.Store(false)
	require.Eventually(t, func() bool { return client.links.replica(0) != nil },
		managedLinkTestBudget, managedLinkPoll)
	require.NoError(t, client.Ping(ctx))
	require.Len(t, registerRequests(server), 1)
}

func TestPublicMultiRequestCallsStopAfterCallerSignIn(t *testing.T) {
	stream, topic, consumer := groupConsumer(t)
	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	send := func(partitioning iggcon.Partitioning) func(context.Context, *IggyTcpClient) error {
		return func(ctx context.Context, client *IggyTcpClient) error {
			_, err := client.SendMessages(ctx, stream, topic, partitioning, []iggcon.IggyMessage{message})
			return err
		}
	}
	for _, operation := range []struct {
		name  string
		first command.Code
		call  func(context.Context, *IggyTcpClient) error
	}{
		{"balanced_send", command.GetPartitionRouteCode, send(iggcon.None())},
		{"keyed_send", command.GetPartitionRouteCode, send(iggcon.Partitioning{Kind: iggcon.MessageKey, Value: []byte("key")})},
		{"roster_before_send", command.GetClusterMetadataCode, send(iggcon.PartitionId(0))},
		{"group_poll", command.SyncGroupCode, func(ctx context.Context, client *IggyTcpClient) error {
			_, err := client.PollMessages(ctx, stream, topic, consumer, iggcon.NextPollingStrategy(), 1, false, nil)
			return err
		}},
	} {
		t.Run(operation.name, func(t *testing.T) {
			received, release := make(chan struct{}), make(chan struct{})
			answer := sync.OnceFunc(func() { close(release) })
			defer answer()
			var held atomic.Bool
			var server *testListener
			node := singleNodeHandler(t, func() string { return server.address() })
			server = listenVSR(t, nil, func(connection, index int, read request) []byte {
				if read.code() == uint32(operation.first) && held.CompareAndSwap(true, false) {
					close(received)
					<-release
				}
				switch {
				case read.code() == uint32(command.GetPartitionRouteCode):
					return routeReply(t, read, 0)
				case read.code() == uint32(command.SyncGroupCode):
					return replyFrame(read.operation(), assignmentBody(7, 0))
				case read.code() == uint32(command.PollMessagesCode):
					return replyFrame(read.operation(), emptyBatchBody(0))
				case read.operation() == vsr.OperationSendMessages:
					return replyFrame(read.operation(), zeroConfirmations())
				default:
					return node(connection, index, read)
				}
			})
			client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "secret")))
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			require.NoError(t, client.Connect(ctx))
			if operation.first == command.GetClusterMetadataCode {
				client.links.hasRoster.Store(false)
			}
			held.Store(true)
			epoch := client.epoch.Load()
			done := make(chan error, 1)
			go func() { done <- operation.call(ctx, client) }()
			select {
			case <-received:
			case <-ctx.Done():
				t.Fatal("the call did not send its first request")
			}
			signedIn := make(chan error, 1)
			go func() { _, err := client.LoginUser(ctx, "other", "password"); signedIn <- err }()
			select {
			case <-epoch.ended:
			case <-ctx.Done():
				t.Fatal("the caller sign-in did not end the old epoch")
			}
			answer()
			require.NoError(t, <-signedIn)
			require.ErrorIs(t, <-done, ierror.ErrUnauthenticated)
			require.Empty(t, requestsFor(server.recorded(), vsr.OperationSendMessages))
			require.Zero(t, requestCount(server.recorded(), command.PollMessagesCode), "the later request must not go out")
		})
	}
}
