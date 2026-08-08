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
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
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

	listener, err := net.Listen("tcp", "127.0.0.1:0")
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

	all := append([]Option{WithServerAddress(address)}, options...)
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
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, address())
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(7, 128)
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	}
}

func TestConnect_DiscoversTheLeaderAndSignsIn(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	recorded := server.recorded()
	require.Len(t, recorded, 2)
	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[0].code(),
		"leadership is settled before the sign-in")
	assert.Equal(t, uint64(0), recorded[0].sessionID(), "metadata works unauthenticated")

	assert.Equal(t, vsr.OperationRegister, recorded[1].operation())
	assert.Zero(t, recorded[1].code(), "only a non-replicated frame carries the code")
	assert.Zero(t, recorded[1].requestID(), "a register is always request zero")
	assert.Zero(t, recorded[1].sessionID())

	assert.True(t, client.session.Bound())
	assert.Equal(t, uint64(128), client.session.SessionID())
	assert.Equal(t, iggcon.SessionStateAuthenticated, client.sessionState)
}

func TestConnect_SignsInWithAPersonalAccessToken(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewPersonalAccessTokenCredentials("token-value")))
	require.NoError(t, client.Connect(context.Background()))

	recorded := server.recorded()
	require.Len(t, recorded, 2)
	assert.Equal(t, vsr.OperationRegister, recorded[1].operation())
	assert.Contains(t, string(recorded[1].payload), "token-value")
	assert.Contains(t, string(recorded[1].payload), vsr.SDKName)
}

func TestConnect_ChecksLeadershipEvenWithoutAutoLogin(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address())
	require.NoError(t, client.Connect(context.Background()))

	recorded := server.recorded()
	require.Len(t, recorded, 1)
	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[0].code())
	assert.False(t, client.session.Bound(), "no sign-in happened")
}

func TestConnect_RedirectsToTheLeaderBeforeSigningIn(t *testing.T) {
	var leader *testListener
	leader = listenVSR(t, nil, singleNodeHandler(t, func() string { return leader.address() }))

	// The follower reports the leader elsewhere, so the client reconnects
	// before it registers anywhere.
	follower := listenVSR(t, nil, func(_, _ int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 1, "127.0.0.1:1", leader.address())
		}
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	client := newDialingClient(t, follower.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	assert.Len(t, follower.recorded(), 1, "the follower only answered the roster")
	assert.Equal(t, leader.address(), client.currentServerAddress)

	onLeader := leader.recorded()
	require.Len(t, onLeader, 2)
	assert.Equal(t, vsr.OperationRegister, onLeader[1].operation())
	assert.True(t, client.session.Bound())
}

func TestConnect_KeepsTheConnectionWhenTheRosterHasOneNode(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, singleNodeHandler(t, func() string { return server.address() }))

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	assert.Equal(t, 1, server.connections(), "a one-node roster never redirects")
}

func TestExchange_ReSignsInBeforeReplayingANonReplicatedRequest(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		switch {
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
	require.Len(t, recorded, 6)

	assert.Equal(t, uint32(command.PingCode), recorded[2].code(), "the dropped attempt")
	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[3].code())
	assert.Equal(t, vsr.OperationRegister, recorded[4].operation(),
		"the replay signs in again before it repeats the request")
	assert.Equal(t, uint32(command.PingCode), recorded[5].code())

	assert.NotEqual(t, recorded[2].clientID(), recorded[5].clientID(),
		"a fresh connection registers a new client identity")
	assert.Equal(t, uint64(129), client.session.SessionID())
}

func TestExchange_RefusesToReplayAReplicatedRequestWithAnUnknownOutcome(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		switch {
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
	assert.ErrorIs(t, err, ierror.ErrDisconnected,
		"a replay would carry a fresh client identity the server cannot deduplicate")

	assert.Equal(t, 1, server.connections(), "no reconnect was attempted")
	recorded := server.recorded()
	require.Len(t, recorded, 3)
	assert.Equal(t, vsr.OperationCreateStream, recorded[2].operation(),
		"the request reached the wire exactly once")
}

func TestExchange_ReconnectsThroughAKnownFollowerAfterTheLeaderDies(t *testing.T) {
	var leaderFailed atomic.Bool
	var follower, leader *testListener
	follower = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
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

	assert.Equal(t, follower.address(), client.currentServerAddress)
	assert.True(t, client.session.Bound())
	recorded := follower.recorded()
	require.Len(t, recorded, 4)
	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[1].code())
	assert.Equal(t, vsr.OperationRegister, recorded[2].operation())
	assert.Equal(t, uint32(command.PingCode), recorded[3].code())
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
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
	assert.Nil(t, client.conn)
}

func TestExchange_DoesNotPreemptAReplayedSignIn(t *testing.T) {
	var server *testListener
	registers := 0
	var mtx sync.Mutex
	server = listenVSR(t, nil, func(connection, index int, read request) []byte {
		switch {
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.operation() == vsr.OperationRegister:
			mtx.Lock()
			registers++
			count := registers
			mtx.Unlock()
			// Drop the second register so the explicit sign-in reconnects.
			if connection == 0 && count == 2 {
				return nil
			}
			return registerReplyFrame(7, uint64(128+connection))
		default:
			return replyFrame(vsr.OperationNonReplicated, nil)
		}
	})

	client := newDialingClient(t, server.address(),
		WithAutoLogin(NewUsernamePasswordCredentials("iggy", "iggy")))
	require.NoError(t, client.Connect(context.Background()))

	identity, err := client.LoginUser(context.Background(), "other", "secret")
	require.NoError(t, err)
	assert.Equal(t, uint32(7), identity.UserId)

	var signIns int
	for _, recorded := range server.recorded() {
		if recorded.operation() == vsr.OperationRegister {
			signIns++
		}
	}
	// The connect sign-in, the dropped explicit one, and its replay. An
	// automatic sign-in on the new connection would make a fourth.
	assert.Equal(t, 3, signIns)
	assert.False(t, client.skipAutoLoginOnce, "the suppression fires exactly once")
}

func TestExchange_FailsFastWhenAutoLoginIsOff(t *testing.T) {
	var server *testListener
	server = listenVSR(t, nil, func(_, index int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 0, server.address())
		}
		if index == 1 {
			return nil
		}
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	client := newDialingClient(t, server.address())
	require.NoError(t, client.Connect(context.Background()))
	client.session.BeginRegister()
	require.NoError(t, client.session.Bind(9))

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	assert.ErrorIs(t, err, ierror.ErrDisconnected)
	assert.Equal(t, 1, server.connections(), "no reconnect was attempted")
}

func TestLogoutUser_ResetsTheSessionSoTheNextSignInIsANewIdentity(t *testing.T) {
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
	require.Len(t, recorded, 4)
	assert.Equal(t, vsr.OperationLogout, recorded[2].operation(),
		"a re-login logs the live session out first")
	assert.Equal(t, vsr.OperationRegister, recorded[3].operation())
	assert.NotEqual(t, first, recorded[3].clientID(),
		"the re-login presents a new client identity")
	assert.True(t, client.session.Bound())
}

func TestHandleLeaderRedirection_StopsAtTheRedirectBudget(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return clusterMetadataFrame(t, 0, "10.0.0.1:8090", "10.0.0.2:8090")
	})
	for range iggcon.MaxLeaderRedirects {
		client.leaderRedirectionState.IncrementRedirect("10.0.0.1:8090")
	}

	redirect, err := client.HandleLeaderRedirection(context.Background())
	require.NoError(t, err)
	assert.False(t, redirect, "the budget stops a redirect storm")
	assert.Equal(t, []string{"10.0.0.1:8090", "10.0.0.2:8090"}, client.knownServerAddresses,
		"the roster still feeds the reconnect candidates")
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
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
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
	assert.Equal(t, vsr.OperationRegister, recorded[1].operation())
	assert.Equal(t, uint32(command.PingCode), recorded[2].code())
	assert.True(t, client.session.Bound())
}

func TestCreateTLSConfig_ExtractsAnIPv6ServerName(t *testing.T) {
	client := NewIggyTcpClient(nil, WithServerAddress("[::1]:8090"), WithTLS())

	config, err := client.createTLSConfig()
	require.NoError(t, err)
	assert.Equal(t, "::1", config.ServerName)
}
