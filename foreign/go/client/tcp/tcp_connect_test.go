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
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
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

func TestConnectAfterDisconnectKeepsCredentialsAndMetadataFloor(t *testing.T) {
	for _, configured := range []bool{false, true} {
		name := "remembered"
		if configured {
			name = "configured"
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
			if !configured {
				_, err := client.LoginUser(context.Background(), "iggy", "secret")
				require.NoError(t, err)
			}
			client.metadataWatermark.Store(42)
			identity := client.session.ClientID()
			requestID := client.session.CurrentRequestID()
			require.NoError(t, client.Disconnect())
			require.False(t, client.session.Bound())
			require.NoError(t, client.Connect(context.Background()))
			require.True(t, client.session.Bound())
			require.NotEqual(t, identity, client.session.ClientID())
			require.Greater(t, client.session.CurrentRequestID(), requestID)
			require.Equal(t, uint64(42), client.metadataWatermark.Load())
			require.Equal(t, uint64(2), registers.Load())
			require.NoError(t, client.Ping(context.Background()))
		})
	}
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
