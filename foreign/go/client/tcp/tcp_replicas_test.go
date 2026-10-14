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
	var registers atomic.Uint64
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, managedLinkSession+registers.Add(1))
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.code() == uint32(command.PingCode) && refuse.CompareAndSwap(true, false):
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
