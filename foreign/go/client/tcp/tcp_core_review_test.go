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
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newUnboundPipeClient is newPipeClient without the session bind, for tests
// that need the unauthenticated state.
func newUnboundPipeClient(t *testing.T) (*IggyTcpClient, net.Conn) {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	client := newTestClient(t, clientConn)
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	return client, serverConn
}

func TestExchange_DoesNotReconnectOnALocalStampFailure(t *testing.T) {
	client, serverConn := newUnboundPipeClient(t)
	client.config.autoLogin = NewAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy"))
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateStream, resultSection())
	})

	// The session is unbound, so stamping a replicated request fails locally
	// before any byte reaches the wire.
	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})

	assert.ErrorIs(t, err, ierror.ErrUnauthenticated)
	assert.Equal(t, iggcon.TransportStateConnected, client.transportState,
		"a healthy connection survives a purely local failure")
	assert.Empty(t, server.recorded(), "nothing reached the wire")
}

func TestLogoutUser_StaysSignedOutThroughTheReconnectPath(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 128)
		case read.operation() == vsr.OperationLogout:
			return replyFrame(vsr.OperationLogout, nil)
		default:
			// The server refuses the signed-out session, which is the shape
			// that previously drove reconnect plus automatic re-login.
			return evictionFrame(vsr.EvictionNoSession, 0, 0)
		}
	})

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))
	require.NoError(t, client.LogoutUser(context.Background()))

	err := client.Ping(context.Background())
	assert.ErrorIs(t, err, ierror.ErrUnauthenticated,
		"the caller hears about the missing session instead of being re-logged in")

	registers := 0
	for _, read := range server.recorded() {
		if read.operation() == vsr.OperationRegister {
			registers++
		}
	}
	assert.Equal(t, 1, registers, "an explicit sign-out is not reversed by an automatic sign-in")
	assert.Equal(t, 1, server.connections(), "no reconnect was attempted")
}

func TestLoginUser_ClearsTheExplicitLogoutSuppression(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(connection, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, uint64(128+connection))
		case read.operation() == vsr.OperationLogout:
			return replyFrame(vsr.OperationLogout, nil)
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))
	require.NoError(t, client.LogoutUser(context.Background()))

	_, err := client.LoginUser(context.Background(), "iggy", "iggy")
	require.NoError(t, err)

	client.mtx.Lock()
	loggedOut := client.loggedOut
	client.mtx.Unlock()
	assert.False(t, loggedOut, "an explicit sign-in lifts the suppression")
	require.NoError(t, client.Ping(context.Background()))
}

func TestConnect_SuppressedSignInSendsNothing(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	// The state a replayed login leaves behind before its reconnect.
	client.skipAutoLoginOnce = true

	require.NoError(t, client.Connect(context.Background()))

	assert.Empty(t, server.recorded(),
		"the replayed login owns the sign-in; Connect must not preempt it")
	assert.False(t, client.skipAutoLoginOnce, "the suppression is consumed exactly once")
}

func TestClose_InterruptsAnInFlightReplayWait(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return statusReplyFrame(vsr.OperationCreateStream,
			uint32(ierror.TransientNotCommittedCode), nil)
	})

	done := make(chan error, 1)
	go func() {
		_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)

	closed := time.Now()
	require.NoError(t, client.Close())
	assert.Less(t, time.Since(closed), 5*time.Second,
		"Close must not wait out the request budget")

	select {
	case err := <-done:
		assert.ErrorIs(t, err, ierror.ErrClientShutdown)
	case <-time.After(5 * time.Second):
		t.Fatal("the in-flight request never returned after Close")
	}
}

func TestExchange_DropsTheConnectionOnAMismatchedReplyEcho(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	go func() {
		read, err := readRequest(serverConn)
		if err != nil {
			return
		}
		answer := replyFrame(vsr.OperationCreateStream, resultSection())
		binary.LittleEndian.PutUint64(answer[replyFrameOffsetRequest:],
			read.requestID()+7)
		_, _ = serverConn.Write(answer)
	}()

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	assert.ErrorIs(t, err, ierror.ErrDisconnected,
		"a reply for some other request means the stream is desynced")
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
}

func TestExchange_NormalizesAWriteFailure(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	_ = serverConn.Close()

	_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
	assert.ErrorIs(t, err, ierror.ErrDisconnected,
		"a write failure classifies like a read failure")
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
}

func TestLoginUser_ConcurrentSignInsAreSingleFlight(t *testing.T) {
	var server *testListener
	sessions := uint64(128)
	var sessionMtx sync.Mutex
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.operation() == vsr.OperationRegister:
			sessionMtx.Lock()
			sessions++
			session := sessions
			sessionMtx.Unlock()
			return registerReplyFrame(7, session)
		case read.operation() == vsr.OperationLogout:
			return replyFrame(vsr.OperationLogout, nil)
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	client := newDialingClient(t, server.address())
	require.NoError(t, client.Connect(context.Background()))

	var group sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			_, errs[index] = client.LoginUser(context.Background(), "iggy", "iggy")
		}(i)
	}
	group.Wait()

	assert.NoError(t, errs[0])
	assert.NoError(t, errs[1])
	assert.True(t, client.session.Bound(),
		"the surviving sign-in leaves one bound session, not an orphaned register")
}

func TestLoginUser_DropsTheConnectionWhenTheBindFails(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 0, server.address())
		}
		// A zero session fence is not bindable.
		return registerReplyFrame(7, 0)
	})

	client := newDialingClient(t, server.address())
	require.NoError(t, client.Connect(context.Background()))

	_, err := client.LoginUser(context.Background(), "iggy", "iggy")
	require.Error(t, err)
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState,
		"the server committed a Register this client cannot adopt; the connection is unusable")
}

func TestTopicCache_ClearCountsKeepsTheBalancedCursors(t *testing.T) {
	cache := topicCache{}
	key := topicKey{stream: "s", topic: "t"}
	cache.setPartitionsCount(key, 4)
	require.Equal(t, uint32(0), cache.nextBalanced(key, 4))
	require.Equal(t, uint32(1), cache.nextBalanced(key, 4))

	cache.clearCounts()

	_, cached := cache.partitionsCount(key)
	assert.False(t, cached, "the count must be reread after a reconnect")
	assert.Equal(t, uint32(2), cache.nextBalanced(key, 4),
		"the fairness cursor survives, so a fleet-wide reconnect does not stampede partition 0")
}

func TestSendMessages_InvalidatesTheCountWhenThePartitionVanished(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, read request) []byte {
		if read.operation() == vsr.OperationSendMessages {
			return statusReplyFrame(vsr.OperationSendMessages,
				uint32(ierror.PartitionNotFoundCode), nil)
		}
		return replyFrame(vsr.OperationNonReplicated, topicDetailsBody(t, 4))
	})

	streamId := numericIdentifier(t, 1)
	topicId := numericIdentifier(t, 1)
	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)

	_, err = client.SendMessages(context.Background(),
		streamId, topicId, iggcon.None(), []iggcon.IggyMessage{message})
	require.ErrorIs(t, err, ierror.ErrPartitionNotFound)

	_, cached := client.topics.partitionsCount(newTopicKey(streamId, topicId))
	assert.False(t, cached,
		"the topic was likely recreated smaller; the count must be reread")
}

func TestGetDefaultOptions_DisablesNagle(t *testing.T) {
	assert.True(t, GetDefaultOptions().config.noDelay,
		"a lockstep client must not stall frames behind Nagle's algorithm")
}
