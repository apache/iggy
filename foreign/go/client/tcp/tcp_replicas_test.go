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
	"net"
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
	closed chan<- struct{}
}

func (connection *managedLinkTestConnection) Close() error {
	err := connection.Conn.Close()
	select {
	case connection.closed <- struct{}{}:
	default:
	}
	return err
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

	_, err := client.LoginUser(ctx, "other", "password")
	require.ErrorIs(t, err, ierror.ErrCannotEstablishConnection, "the sign-in fails before its Register")
	healed := listenVSRAt(t, server.address(), nil, handler)
	release()
	awaitRecoveryEnd(t, client)

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
