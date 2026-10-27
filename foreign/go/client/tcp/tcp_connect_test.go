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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// selfSignedCert mints a throwaway server certificate for the loopback
// listener, so the test carries no expiry or checkout-layout dependency.
func selfSignedCert(t *testing.T) (tls.Certificate, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	certificate, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	caPath := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caPath, certPEM, 0o600))
	return certificate, caPath
}

func TestConnectBoundsSilentSignInWithoutCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		serverConnection, connection := net.Pipe()
		defer func() { _ = serverConnection.Close() }()
		client := newTestClient(t, connection)
		client.rememberedLogin = NewAutoLogin(NewUsernamePasswordCredentials("iggy", "secret"))
		server := serve(serverConnection, func(_ int, read request) []byte { return nil })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- client.Connect(ctx) }()
		time.Sleep(responseReadTimeout)
		synctest.Wait()
		select {
		case err := <-done:
			require.Error(t, err)
		default:
			t.Fatal("Connect exceeded its sign-in budget without a caller deadline")
		}
		require.Len(t, server.recorded(), 1)
	})
}

func TestExplicitSilentSignInFailsAsNotAcceptedAtItsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		serverConnection, connection := net.Pipe()
		defer func() { _ = serverConnection.Close() }()
		client := newTestClient(t, connection)
		server := serve(serverConnection, func(_ int, read request) []byte { return nil })
		done := make(chan error, 1)
		go func() {
			_, err := client.LoginUser(context.Background(), "iggy", "secret")
			done <- err
		}()
		time.Sleep(responseReadTimeout)
		synctest.Wait()
		select {
		case err := <-done:
			require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
		default:
			t.Fatal("LoginUser outlived its sign-in deadline")
		}
		require.Len(t, server.recorded(), 1)
	})
}

func TestConnectDoesNotSpendSignInBudgetWaitingForDial(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := newDialingClient(t, "127.0.0.1:1")
		for range cap(client.links.permits) {
			client.links.permits <- struct{}{}
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- client.Connect(ctx) }()
		time.Sleep(responseReadTimeout + time.Second)
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Connect consumed the sign-in budget before dialing: %v", err)
		default:
		}
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}

func TestExplicitSignInCannotReplaceEpochAfterCloseStarts(t *testing.T) {
	serverConnection, connection := net.Pipe()
	defer func() { _ = serverConnection.Close() }()
	client := newTestClient(t, connection)
	server := serve(serverConnection, func(_ int, read request) []byte {
		return registerReplyFrame(signedInUserID, managedLinkSession)
	})
	epoch := client.epoch.Load()
	client.closeOnce.Do(func() { close(client.closed) })
	_, err := client.LoginUser(context.Background(), "user", "password")
	require.ErrorIs(t, err, ierror.ErrClientShutdown)
	require.Same(t, epoch, client.epoch.Load(), "a new epoch would cancel Close's pending detach")
	require.Empty(t, server.recorded())
	require.NoError(t, client.Close())
	require.False(t, linkReady(client.links.bootstrap))
}

// testListener is a VSR server on a loopback port. The handler is called with
// the zero-based index of the accepted connection and of the frame within it.
type testListener struct {
	listener net.Listener
	mtx      sync.Mutex
	requests []request
	accepted int
}

func listenVSR(t *testing.T, wrap func(net.Conn) net.Conn,
	handler func(connection, index int, read request) []byte,
) *testListener {
	t.Helper()
	return listenVSRAt(t, "127.0.0.1:0", wrap, handler)
}

// listenVSRAt is listenVSR on a given address, so that a test can bring a
// closed server back where the client dials.
func listenVSRAt(t *testing.T, address string, wrap func(net.Conn) net.Conn,
	handler func(connection, index int, read request) []byte,
) *testListener {
	t.Helper()

	listener, err := net.Listen("tcp", address)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	server := &testListener{listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if wrap != nil {
				conn = wrap(conn)
			}

			server.mtx.Lock()
			connection := server.accepted
			server.accepted++
			server.mtx.Unlock()

			go func(conn net.Conn, connection int) {
				defer func() { _ = conn.Close() }()
				for index := 0; ; index++ {
					read, err := readRequest(conn)
					if err != nil {
						return
					}
					server.mtx.Lock()
					server.requests = append(server.requests, read)
					server.mtx.Unlock()

					answer := handler(connection, index, read)
					if answer == nil {
						return
					}
					echoReplyRequest(answer, read)
					if _, err := conn.Write(answer); err != nil {
						return
					}
				}
			}(conn, connection)
		}
	}()
	return server
}

func (s *testListener) address() string { return s.listener.Addr().String() }

func (s *testListener) recorded() []request {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return append([]request(nil), s.requests...)
}

func (s *testListener) connections() int {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.accepted
}

// newDialingClient builds a client that dials address with the given options.
func newDialingClient(t *testing.T, address string, options ...Option) *IggyTcpClient {
	t.Helper()

	all := append([]Option{WithServerAddress(address), WithHeartbeatInterval(0)}, options...)
	client := NewIggyTcpClient(slog.New(slog.DiscardHandler), all...)
	client.config.reconnection.maxRetries = 1
	client.config.reconnection.interval = 10 * time.Millisecond
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// singleNodeHandler answers cluster metadata for a one-node roster and signs
// in any register it sees, so tests can focus on the flow under test.
func singleNodeHandler(t *testing.T, address func() string) func(int, int, request) []byte {
	t.Helper()
	return func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 128)
		default:
			return replyFrame(read.operation(), nil)
		}
	}
}

func TestConnect_SignsInAndThenSettlesLeadership(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	recorded := server.recorded()
	require.Len(t, recorded, 2)
	assert.Equal(t, vsr.OperationRegister, recorded[0].operation(),
		"the roster read is auth-gated, so the sign-in comes first")
	assert.Zero(t, recorded[0].code(), "only a non-replicated frame carries the code")
	assert.Zero(t, recorded[0].requestID(), "a register is always request zero")
	assert.Zero(t, recorded[0].sessionID())

	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[1].code(),
		"leadership is settled after the sign-in")
	assert.Equal(t, uint64(128), recorded[1].sessionID(), "the roster read is authenticated")

	assert.True(t, client.session.Bound())
	assert.Equal(t, uint64(128), client.session.SessionID())
	assert.True(t, client.session.Bound())
}

func TestConnect_SignsInWithAPersonalAccessToken(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewPersonalAccessTokenCredentials("token-value")))
	require.NoError(t, client.Connect(context.Background()))

	recorded := server.recorded()
	require.Len(t, recorded, 2)
	assert.Equal(t, vsr.OperationRegister, recorded[0].operation())
	assert.Contains(t, string(recorded[0].payload), "token-value")
	assert.Contains(t, string(recorded[0].payload), vsr.SDKName)
}

func TestConnect_SendsNothingWithoutAutoLogin(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address())
	require.NoError(t, client.Connect(context.Background()))

	// The roster read is auth-gated, so an unauthenticated connection has
	// nothing to settle leadership with; the caller's sign-in does it later.
	assert.Empty(t, server.recorded())
	assert.False(t, client.session.Bound(), "no sign-in happened")
}

func TestConnect_RedirectsToTheLeaderAfterSigningIn(t *testing.T) {
	var leader *testListener
	leader = listenVSR(t, nil, singleNodeHandler(t, func() string { return leader.address() }))

	// The follower completes the sign-in (the server forwards the register to
	// the primary) and only then reports the leader elsewhere.
	follower := listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 1, "127.0.0.1:1", leader.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 512)
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	client := newDialingClient(t, follower.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	onFollower := follower.recorded()
	require.Len(t, onFollower, 2, "the follower answered the sign-in and the roster")
	assert.Equal(t, vsr.OperationRegister, onFollower[0].operation())
	assert.Equal(t, uint32(command.GetClusterMetadataCode), onFollower[1].code())
	assert.Equal(t, leader.address(), client.GetConnectionInfo().ServerAddress)

	require.Eventually(t, func() bool { return client.links.replica(1) != nil }, time.Second, time.Millisecond)
	onLeader := leader.recorded()
	require.Len(t, onLeader, 1, "the advertised replica proactively binds the existing session")
	assert.Equal(t, uint32(command.BindSessionCode), onLeader[0].code())
	assert.True(t, client.session.Bound())
	assert.Equal(t, uint64(512), client.session.SessionID(),
		"the leader resumes the original session")
}

func TestConnect_KeepsTheConnectionWhenTheRosterHasOneNode(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	assert.Equal(t, 1, server.connections(), "a one-node roster never redirects")
}

func TestExchange_ResumesBeforeReplayingANonReplicatedRequest(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		switch {
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, uint64(128+connection))
		case connection == 0 && index == 2:
			// Drop the first ping to force the reconnect path.
			return nil
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	require.NoError(t, client.Ping(context.Background()))

	assert.Equal(t, 2, server.connections())
	recorded := server.recorded()
	require.Len(t, recorded, 5)

	assert.Equal(t, uint32(command.PingCode), recorded[2].code(), "the dropped attempt")
	assert.Equal(t, uint32(command.BindSessionCode), recorded[3].code(),
		"the replay resumes before it repeats the request")
	assert.Equal(t, uint32(command.PingCode), recorded[4].code())

	assert.Equal(t, recorded[2].header, recorded[4].header)
	assert.Equal(t, recorded[2].clientID(), recorded[4].clientID(),
		"a replacement connection preserves the client identity")
	assert.Equal(t, uint64(128), client.session.SessionID())
}

func TestExchange_ReplaysAReplicatedRequestWithTheExactIdentity(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		switch {
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, uint64(128+connection))
		case connection == 0 && index == 2:
			// Drop the reply so the stream creation's outcome is unknown.
			return nil
		default:
			return replyFrame(vsr.OperationCreateStream, resultSection())
		}
	})

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	require.NoError(t, err)
	require.Equal(t, 2, server.connections())
	var mutations []request
	for _, read := range server.recorded() {
		if read.operation() == vsr.OperationCreateStream {
			mutations = append(mutations, read)
		}
	}
	require.Len(t, mutations, 2)
	require.Equal(t, mutations[0].header, mutations[1].header)
	require.Equal(t, mutations[0].payload, mutations[1].payload)
}

func TestExchange_ReconnectsThroughAKnownFollowerAfterTheLeaderDies(t *testing.T) {
	var leaderFailed atomic.Bool
	var follower, leader *testListener
	follower = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			leaderIndex := 1
			if leaderFailed.Load() {
				leaderIndex = 0
			}
			return clusterMetadataFrame(t, leaderIndex, follower.address(), leader.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 256)
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
			return registerReplyFrame(7, 128)
		default:
			leaderFailed.Store(true)
			_ = leader.listener.Close()
			return nil
		}
	})

	client := newDialingClient(t, follower.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	client.config.reconnection.maxRetries = 2
	require.NoError(t, client.Connect(context.Background()))

	require.NoError(t, client.Ping(context.Background()))

	assert.Equal(t, follower.address(), client.GetConnectionInfo().ServerAddress)
	assert.True(t, client.session.Bound())
	recorded := follower.recorded()
	require.Equal(t, vsr.OperationRegister, recorded[0].operation())
	require.GreaterOrEqual(t, requestCount(recorded, command.GetClusterMetadataCode), 2)
	require.Equal(t, 1, requestCount(recorded, command.PingCode))
	require.Equal(t, 1, follower.connections(), "the already managed follower remains available")
}

func TestConnect_DropsTheConnectionWhenAutomaticSignInFails(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 0, server.address())
		}
		return evictionFrame(vsr.EvictionInvalidCredentials, 0, 0)
	})

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "wrong")))
	err := client.Connect(context.Background())

	assert.ErrorIs(t, err, ierror.ErrInvalidCredentials)
	var refusal ierror.IggyError
	require.ErrorAs(t, err, &refusal)
	assert.Equal(t, ierror.InvalidCredentialsCode, refusal.Code(), "Connect returns the refusal, not the 40 of the epoch it ended")
	assert.False(t, linkReady(client.links.any()))
	assert.Nil(t, client.links.any())
}

func TestExchange_FailsFastWhenAutoLoginIsOff(t *testing.T) {
	server := listenVSR(t, nil, func(_, index int, read request) []byte {
		if index == 0 {
			return nil
		}
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	client := newDialingClient(t, server.address())
	require.NoError(t, client.Connect(context.Background()))
	client.session.BeginRegister()
	require.NoError(t, client.session.Bind(9))

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	assert.ErrorIs(t, err, ierror.ErrNotConnected)
	assert.Equal(t, 1, server.connections(), "ordinary requests do not bootstrap a connection")
}

func TestLogoutUser_ResetsTheSessionSoTheNextSignInIsANewIdentity(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(7, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 128)
		case read.operation() == vsr.OperationLogout:
			return replyFrame(vsr.OperationLogout, nil)
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))
	first := client.session.ClientID()

	_, err := client.LoginUser(context.Background(), "iggy", "iggy")
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 5)
	assert.Equal(t, vsr.OperationLogout, recorded[2].operation(),
		"a re-login logs the live session out first")
	assert.Equal(t, vsr.OperationRegister, recorded[3].operation())
	assert.NotEqual(t, first, recorded[3].clientID(),
		"the re-login presents a new client identity")
	assert.True(t, client.session.Bound())
}

func TestConnect_UnwindsWhenEveryReadFailsInsideTheConnectFlow(t *testing.T) {
	// An accept-then-close listener makes every dial succeed and every first
	// read fail, the shape that previously recursed Connect through the
	// reconnect path until stack exhaustion.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, acceptErr := listener.Accept()
			if acceptErr != nil {
				return
			}
			_ = conn.Close()
		}
	}()

	client := newDialingClient(t, listener.Addr().String(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = client.Connect(ctx)

	require.Error(t, err, "the failure unwinds instead of recursing")
	assert.NotErrorIs(t, err, context.DeadlineExceeded,
		"Connect gave up on its own, not through the safety-net timeout")
	assert.False(t, linkReady(client.links.any()))
}

func TestConnect_ExchangesOverTLS(t *testing.T) {
	certificate, caPath := selfSignedCert(t)

	var server *testListener
	server = listenVSR(t,
		func(conn net.Conn) net.Conn {
			return tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{certificate}})
		},
		singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address(),
		WithTLS(
			WithTLSCAFile(caPath),
			WithTLSDomain("localhost"),
		),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	require.NoError(t, client.Ping(context.Background()))

	recorded := server.recorded()
	require.Len(t, recorded, 3)
	assert.Equal(t, vsr.OperationRegister, recorded[0].operation())
	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[1].code())
	assert.Equal(t, uint32(command.PingCode), recorded[2].code())
	assert.True(t, client.session.Bound())
}

func TestCreateTLSConfig_ExtractsAnIPv6ServerName(t *testing.T) {
	client := NewIggyTcpClient(nil, WithServerAddress("[::1]:8090"), WithTLS())

	config, err := client.createTLSConfig("[::1]:8090")
	require.NoError(t, err)
	assert.Equal(t, "::1", config.ServerName)
}

func TestLoginUserCanRetryRejectedCredentialsOnTheSameClient(t *testing.T) {
	var registers atomic.Uint32
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.operation() == vsr.OperationRegister && registers.Add(1) == 1 {
			return evictionFrame(vsr.EvictionInvalidCredentials, 0, 0)
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})
	client := newDialingClient(t, server.address())
	require.NoError(t, client.Connect(context.Background()))
	_, err := client.LoginUser(context.Background(), "iggy", "wrong")
	require.ErrorIs(t, err, ierror.ErrInvalidCredentials)
	require.Empty(t, client.operations.retained)
	require.Empty(t, client.operations.lanes)
	_, err = client.LoginUser(context.Background(), "iggy", "correct")
	require.NoError(t, err)
	require.True(t, client.session.Bound())
	require.Equal(t, uint32(2), registers.Load())
	require.NoError(t, client.Ping(context.Background()))
}

// Disconnect forgets the sign-in of the caller, as the Rust SDK does. The next
// connect signs in as the configured user, or not at all without one, and the
// client keeps its metadata floor.
func TestConnectAfterDisconnectForgetsTheCallerSignInAndKeepsTheMetadataFloor(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "without_auto_login"
		if configured {
			name = "with_auto_login"
		}
		t.Run(name, func(t *testing.T) {
			var registers atomic.Uint64
			var server *testListener
			server = listenVSR(t, nil, func(_, _ int, read request) []byte {
				if read.operation() == vsr.OperationRegister {
					return registerReplyFrame(7, 100+registers.Add(1))
				}
				return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
			})
			var options []Option
			if configured {
				options = append(options, WithAutoLogin(NewUsernamePasswordCredentials("iggy", "secret")))
			}
			client := newDialingClient(t, server.address(), options...)
			require.NoError(t, client.Connect(context.Background()))
			_, err := client.LoginUser(context.Background(), "other", "password")
			require.NoError(t, err)
			client.metadataWatermark.Store(42)
			identity := client.session.ClientID()
			requestID := client.session.CurrentRequestID()
			require.NoError(t, client.Disconnect())
			require.False(t, sessionBound(client))

			require.NoError(t, client.Connect(context.Background()))

			require.NotEqual(t, identity, client.session.ClientID())
			require.Equal(t, uint64(42), client.metadataWatermark.Load())
			var signIns []string
			for _, read := range registerRequests(server) {
				signIns = append(signIns, registerUsername(read.payload))
			}
			if !configured {
				require.False(t, sessionBound(client), "the connect has no user to sign in as")
				require.Equal(t, []string{"other"}, signIns)
				return
			}
			require.True(t, sessionBound(client))
			require.Equal(t, []string{"iggy", "other", "iggy"}, signIns)
			require.Greater(t, client.session.CurrentRequestID(), requestID)
			require.NoError(t, client.Ping(context.Background()))
		})
	}
}

// Disconnect also ends a LogoutUser, as in the Rust SDK, so the next connect
// signs in as the configured user.
func TestConnectAfterLogoutAndDisconnectSignsInAsTheConfiguredUser(t *testing.T) {
	var registers atomic.Uint64
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.operation() == vsr.OperationRegister {
			return registerReplyFrame(7, 100+registers.Add(1))
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "secret")))
	require.NoError(t, client.Connect(context.Background()))
	require.NoError(t, client.LogoutUser(context.Background()))
	require.NoError(t, client.Disconnect())

	require.NoError(t, client.Connect(context.Background()))

	require.True(t, sessionBound(client), "the connect signed in as the configured user")
	require.Equal(t, uint64(2), registers.Load())
	require.NoError(t, client.Ping(context.Background()))
}

func TestConcurrentConnectWaiterCannotRestartOrCancelSharedRegistration(t *testing.T) {
	registered, release := make(chan struct{}), make(chan struct{})
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.operation() == vsr.OperationRegister {
			close(registered)
			<-release
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	first := make(chan error, 1)
	go func() { first <- client.Connect(context.Background()) }()
	<-registered
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, client.Connect(ctx), context.DeadlineExceeded)
	close(release)
	require.NoError(t, <-first)
	require.Equal(t, 1, server.connections())
	var registrations int
	for _, read := range server.recorded() {
		if read.operation() == vsr.OperationRegister {
			registrations++
		}
	}
	require.Equal(t, 1, registrations)
	require.Equal(t, 1, requestCount(server.recorded(), command.GetClusterMetadataCode))
	require.True(t, client.session.Bound())
}

// A call that starts while Connect signs in waits for that session, as it
// waits for a recovery, and then goes out under it.
func TestCallsThatStartDuringConnectWaitForItsSession(t *testing.T) {
	registered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch read.operation() {
		case vsr.OperationRegister:
			close(registered)
			<-release
		case vsr.OperationCreateStream:
			return replyFrame(vsr.OperationCreateStream, resultSection())
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	connected := make(chan error, 1)
	go func() { connected <- client.Connect(context.Background()) }()
	<-registered
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	created, pinged := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := client.do(ctx, &command.CreateStream{Name: "waiting"})
		created <- err
	}()
	go func() { pinged <- client.Ping(ctx) }()
	require.Never(t, func() bool { return len(created) != 0 || len(pinged) != 0 }, recoveryWaitDeadline, managedLinkPoll,
		"the calls wait for the session of the Connect")

	unblock()
	require.NoError(t, <-connected)
	require.NoError(t, <-created)
	require.NoError(t, <-pinged)
	calls := createStreams(server.recorded())
	for _, read := range server.recorded() {
		if read.code() == uint32(command.PingCode) {
			calls = append(calls, read)
		}
	}
	require.Len(t, calls, 2)
	for _, call := range calls {
		require.Equal(t, managedLinkSession, call.sessionID(), "the call goes out under the new session")
	}
}

// The server refuses the sign-in of Connect for a reason other than its
// credentials. Connect returns the refusal, and the calls that wait for it end
// with 58 and the refusal as their cause. The client keeps its sign-in.
func TestCallsThatWaitForAConnectRefusedForAnotherReasonEndWith58(t *testing.T) {
	registered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	newerProtocol := vsr.ProtocolVersion + 1
	var refused atomic.Bool
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.operation() == vsr.OperationRegister && refused.CompareAndSwap(false, true) {
			close(registered)
			<-release
			return evictionFrame(vsr.EvictionIncompatibleProtocol, newerProtocol, newerProtocol)
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})
	client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	epoch := client.epoch.Load()
	connected := make(chan error, 1)
	go func() { connected <- client.Connect(context.Background()) }()
	<-registered
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	created, pinged := make(chan error, 1), make(chan error, 1)
	go func() {
		_, err := client.do(ctx, &command.CreateStream{Name: "waiting"})
		created <- err
	}()
	go func() { pinged <- client.Ping(ctx) }()
	require.Never(t, func() bool { return len(created) != 0 || len(pinged) != 0 }, recoveryWaitDeadline, managedLinkPoll,
		"the calls wait for the session of the Connect")

	unblock()
	err := <-connected
	var refusal ierror.IggyError
	require.ErrorAs(t, err, &refusal)
	require.Equal(t, ierror.IncompatibleProtocolVersionCode, refusal.Code(), "Connect returns its own refusal")
	for _, call := range []chan error{created, pinged} {
		err := <-call
		require.ErrorIs(t, err, ierror.ErrTransientNotAccepted)
		require.ErrorIs(t, err, ierror.ErrIncompatibleProtocolVersion, "the call keeps the refusal as its cause")
	}
	require.Same(t, epoch, client.epoch.Load(), "the refusal keeps the epoch")
	require.Empty(t, createStreams(server.recorded()), "the create never went out")
	require.NoError(t, client.Connect(ctx), "the client keeps its sign-in")
	require.True(t, sessionBound(client))
}

func TestLoginUserRacingDisconnectCannotRememberItsCredentials(t *testing.T) {
	serverConnection, connection := net.Pipe()
	defer func() { _ = serverConnection.Close() }()
	client := newTestClient(t, connection)
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))
	client.config.serverAddress = server.address()
	link := client.links.bootstrap
	received, release := make(chan struct{}), make(chan struct{})
	answer := sync.OnceFunc(func() { close(release) })
	defer answer()
	serve(serverConnection, func(_ int, read request) []byte {
		close(received)
		<-release
		return registerReplyFrame(signedInUserID, managedLinkSession)
	})
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	signedIn := make(chan error, 1)
	go func() { _, err := client.LoginUser(ctx, "other", "password"); signedIn <- err }()
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal("the sign-in did not send its registration")
	}

	// Hold the accepted reply before Bind, then stop the link. Holding its
	// mutex pauses signIn at markBound after Bind, the former gap before
	// register remembered the credentials. Disconnect can finish because
	// the link is already closed.
	client.mtx.Lock()
	unlockSession := sync.OnceFunc(client.mtx.Unlock)
	defer unlockSession()
	answer()
	require.Eventually(t, func() bool { return len(link.capacity) == 0 }, managedLinkTestBudget, managedLinkPoll,
		"the registration exchange did not finish")
	link.close(ierror.ErrDisconnected)
	link.done.Wait()
	link.mu.Lock()
	unlockLink := sync.OnceFunc(link.mu.Unlock)
	defer unlockLink()
	unlockSession()
	require.Eventually(t, func() bool { return sessionBound(client) }, managedLinkTestBudget, managedLinkPoll,
		"the registration reply did not bind")

	require.NoError(t, client.Disconnect())
	unlockLink()
	select {
	case err := <-signedIn:
		require.Error(t, err)
	case <-ctx.Done():
		t.Fatal("the interrupted sign-in did not end")
	}
	_, remembered := client.signInCredentials()
	require.False(t, remembered, "the sign-in restored credentials after Disconnect")
	require.NoError(t, client.Connect(ctx))
	require.Empty(t, registerRequests(server), "Connect must not reuse the interrupted sign-in")
}

func TestExplicitSignInFailureRequiresAnotherExplicitSignIn(t *testing.T) {
	for _, test := range []struct {
		name     string
		login    func(context.Context, *IggyTcpClient) error
		phase    command.Code
		cancel   bool
		deadline bool
	}{
		{name: "invalid_username", login: func(ctx context.Context, client *IggyTcpClient) error {
			_, err := client.LoginUser(ctx, "", signInPassword)
			return err
		}},
		{name: "invalid_token", login: func(ctx context.Context, client *IggyTcpClient) error {
			_, err := client.LoginWithPersonalAccessToken(ctx, strings.Repeat("x", 256))
			return err
		}},
		{name: "nil_context", login: func(_ context.Context, client *IggyTcpClient) error {
			//nolint:staticcheck // A nil caller context is part of the failed-attempt policy.
			_, err := client.LoginUser(nil, "other", signInPassword)
			return err
		}},
		{name: "cancelled_context", login: func(ctx context.Context, client *IggyTcpClient) error {
			ctx, cancel := context.WithCancel(ctx)
			cancel()
			_, err := client.LoginUser(ctx, "other", signInPassword)
			return err
		}},
		{name: "register_refused", phase: command.LoginRegisterCode},
		{name: "register_cancelled", phase: command.LoginRegisterCode, cancel: true},
		{name: "register_deadline", phase: command.LoginRegisterCode, deadline: true},
		{name: "roster_refused", phase: command.GetClusterMetadataCode},
		{name: "roster_cancelled", phase: command.GetClusterMetadataCode, cancel: true},
		{name: "roster_deadline", phase: command.GetClusterMetadataCode, deadline: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var fail atomic.Bool
			reached, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			var server *testListener
			server = listenVSR(t, nil, func(_, _ int, read request) []byte {
				phase := read.code() == uint32(test.phase)
				if test.phase == command.LoginRegisterCode {
					phase = read.operation() == vsr.OperationRegister
				}
				if fail.Load() && test.phase != 0 && phase {
					if test.cancel || test.deadline {
						close(reached)
						<-release
					}
					return statusReplyFrame(read.operation(), uint32(ierror.InvalidCredentialsCode), nil)
				}
				return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
			})
			client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials(signInUsername, signInPassword)))
			require.NoError(t, client.Connect(context.Background()))
			fail.Store(true)
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			loginCtx := ctx
			if test.deadline {
				var endDeadline context.CancelFunc
				loginCtx, endDeadline = context.WithTimeout(ctx, time.Second)
				defer endDeadline()
			}
			done := make(chan error, 1)
			go func() {
				if test.login != nil {
					done <- test.login(loginCtx, client)
					return
				}
				_, err := client.LoginUser(loginCtx, "other", signInPassword)
				done <- err
			}()
			if test.cancel || test.deadline {
				select {
				case <-reached:
				case <-ctx.Done():
					t.Fatal("the explicit sign-in did not reach its held reply")
				}
				if test.cancel {
					cancel()
				}
			}
			require.Error(t, <-done)
			unblock()
			fail.Store(false)
			assert.False(t, sessionBound(client), "a failed public sign-in must discard its tentative session")
			before := len(registerRequests(server))
			binds := requestCount(server.recorded(), command.BindSessionCode)
			require.NoError(t, client.Connect(context.Background()))
			require.NoError(t, client.Disconnect())
			require.NoError(t, client.Connect(context.Background()))
			assert.Len(t, registerRequests(server), before, "Connect must not unblock automatic fallback")
			assert.Equal(t, binds, requestCount(server.recorded(), command.BindSessionCode), "the failed attempt must not resume with BindSession")
			_, err := client.do(context.Background(), &command.CreateStream{Name: "requires-sign-in"})
			assert.ErrorIs(t, err, ierror.ErrUnauthenticated)
			_, err = client.LoginUser(context.Background(), "retry", signInPassword)
			require.NoError(t, err)
			require.NoError(t, client.Ping(context.Background()))
			require.Equal(t, "retry", registerUsername(registerRequests(server)[len(registerRequests(server))-1].payload))
		})
	}
}

func TestExplicitSignInSupersedesInitialAutomaticConnect(t *testing.T) {
	for _, token := range []bool{false, true} {
		t.Run(fmt.Sprintf("token_%t", token), func(t *testing.T) {
			reached, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			var server *testListener
			server = listenVSR(t, nil, func(_, _ int, read request) []byte {
				if read.operation() == vsr.OperationRegister && registerUsername(read.payload) == signInUsername {
					close(reached)
					<-release
				}
				return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
			})
			client := newDialingClient(t, server.address(), WithAutoLogin(NewUsernamePasswordCredentials(signInUsername, signInPassword)))
			automatic := make(chan error, 1)
			go func() { automatic <- client.Connect(context.Background()) }()
			<-reached
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			var err error
			if token {
				_, err = client.LoginWithPersonalAccessToken(ctx, "caller-token")
			} else {
				_, err = client.LoginUser(ctx, "caller", signInPassword)
			}
			require.NoError(t, err, "explicit credentials must not wait behind the held automatic Register")
			require.Error(t, <-automatic)
			unblock()
			require.NoError(t, client.Ping(ctx))
			credentials, enabled := client.signInCredentials()
			require.True(t, enabled)
			if token {
				require.Equal(t, "caller-token", credentials.personalAccessToken)
			} else {
				require.Equal(t, "caller", credentials.username)
			}
		})
	}
}

func TestInvalidExplicitSignInSupersedesAnOlderAttemptBeforeItsGate(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprintf("invalid_%t", invalid), func(t *testing.T) {
			reached, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			var server *testListener
			server = listenVSR(t, nil, func(_, _ int, read request) []byte {
				if read.operation() == vsr.OperationRegister {
					close(reached)
					<-release
				}
				return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
			})
			client := newDialingClient(t, server.address())
			require.NoError(t, client.Connect(context.Background()))
			ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
			defer cancel()
			first := make(chan error, 1)
			go func() { _, err := client.LoginUser(ctx, "first", signInPassword); first <- err }()
			<-reached
			username := "second"
			secondCtx := ctx
			if invalid {
				username = ""
			} else {
				var end context.CancelFunc
				secondCtx, end = context.WithCancel(ctx)
				end()
			}
			_, err := client.LoginUser(secondCtx, username, signInPassword)
			require.Error(t, err)
			unblock()
			require.Error(t, <-first, "the superseded attempt must not bind after the newer attempt failed")
			require.False(t, sessionBound(client))
			require.NoError(t, client.Connect(ctx))
			require.Len(t, registerRequests(server), 1)
		})
	}
}

func TestNewExplicitSignInSupersedesHeldRosterCompletion(t *testing.T) {
	reached, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	var registrations atomic.Uint32
	var server *testListener
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.operation() == vsr.OperationRegister {
			return registerReplyFrame(signedInUserID, managedLinkSession+uint64(registrations.Add(1)))
		}
		if read.code() == uint32(command.GetClusterMetadataCode) && registrations.Load() == 1 {
			close(reached)
			<-release
			return statusReplyFrame(read.operation(), uint32(ierror.InvalidCredentialsCode), nil)
		}
		return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
	})
	client := newDialingClient(t, server.address())
	require.NoError(t, client.Connect(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := client.LoginUser(ctx, "first", signInPassword); first <- err }()
	<-reached
	_, err := client.LoginUser(ctx, "second", signInPassword)
	require.NoError(t, err, "the newer caller must end the tentative older session")
	unblock()
	require.Error(t, <-first)
	credentials, enabled := client.signInCredentials()
	require.True(t, enabled)
	require.Equal(t, "second", credentials.username)
	require.NoError(t, client.Ping(ctx))
}

func TestDisconnectCompletionCannotForgetANewerExplicitSignIn(t *testing.T) {
	server, evict := credentialServer(t, func(_ int, read request) []byte { return committedReply(read, 0) })
	peer, connection := net.Pipe()
	defer func() { _ = peer.Close() }()
	readStopped, resumeRead := make(chan struct{}), make(chan struct{})
	resume := sync.OnceFunc(func() { close(resumeRead) })
	defer resume()
	client := newTestClient(t, &managedLinkTestConnection{Conn: connection, readStopped: readStopped, resumeRead: resumeRead})
	client.config.serverAddress = server.address()
	client.config.autoLogin = NewAutoLogin(NewUsernamePasswordCredentials(signInUsername, signInPassword))
	client.links.bootstrap.endpoint = server.address()
	serve(peer, func(index int, read request) []byte {
		return singleNodeHandler(t, func() string { return server.address() })(0, index, read)
	})
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	require.NoError(t, client.Connect(ctx))
	disconnected := make(chan error, 1)
	go func() { disconnected <- client.Disconnect() }()
	select {
	case <-readStopped:
	case <-ctx.Done():
		t.Fatal("Disconnect did not stop the old reader")
	}
	_, err := client.LoginUser(ctx, "newer", signInPassword)
	require.NoError(t, err)
	resume()
	require.NoError(t, <-disconnected)
	credentials, enabled := client.signInCredentials()
	require.True(t, enabled)
	assert.Equal(t, "newer", credentials.username, "old Disconnect cleanup must not forget the later sign-in")
	requireSignInAgain(t, client, server, evict, "newer", signInPassword)
}
