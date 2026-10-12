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

package client

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// applyOptions runs options the way NewIggyClient does, and returns the
// resulting TCP client so a test can observe what the parser produced.
func applyOptions(t *testing.T, options []tcp.Option) *tcp.IggyTcpClient {
	t.Helper()
	return tcp.NewIggyTcpClient(nil, options...)
}

func TestParseConnectionString_ParsesTheDefaultSchemeWithPasswordCredentials(t *testing.T) {
	opts, err := ParseConnectionString("iggy://iggy:secret@127.0.0.1:8090")
	require.NoError(t, err)
	client := applyOptions(t, opts)

	assert.Equal(t, "127.0.0.1:8090", client.GetConnectionInfo().ServerAddress)
}

func TestParseConnectionString_ParsesTheExplicitTcpSchemeWithAPersonalAccessToken(t *testing.T) {
	opts, err := ParseConnectionString("iggy+tcp://iggypat-1234567890abcdef@localhost:8090")
	require.NoError(t, err)
	client := applyOptions(t, opts)

	assert.Equal(t, "localhost:8090", client.GetConnectionInfo().ServerAddress)
}

func TestParseConnectionString_ParsesIPv6HostWithoutBrackets(t *testing.T) {
	opts, err := ParseConnectionString("iggy://iggy:secret@[::1]:8090")
	require.NoError(t, err)
	client := applyOptions(t, opts)

	assert.Equal(t, "[::1]:8090", client.GetConnectionInfo().ServerAddress)
}

// Frame offsets the fake server reads and writes. They mirror the
// unexported tables of the codec package, which this package cannot reach.
const (
	frameOffsetSize      = 48
	frameOffsetCommand   = 60
	frameOffsetClient    = 128
	frameOffsetRequest   = 168
	frameOffsetOperation = 176
	frameOffsetSession   = 184
	frameOffsetReserved  = 196

	replyFrameOffsetRequest   = 200
	replyFrameOffsetOperation = 208
	replyFrameOffsetStatus    = 216
)

// wireRequest is one frame the fake server read off the wire.
type wireRequest struct {
	header  [vsr.HeaderSize]byte
	payload []byte
}

func (r wireRequest) operation() vsr.Operation {
	return vsr.Operation(r.header[frameOffsetOperation])
}

func (r wireRequest) code() uint32 {
	return binary.LittleEndian.Uint32(r.header[frameOffsetReserved:])
}

// readWireRequest reads one complete frame from conn.
func readWireRequest(conn net.Conn) (wireRequest, error) {
	var read wireRequest
	if _, err := io.ReadFull(conn, read.header[:]); err != nil {
		return read, err
	}
	size := binary.LittleEndian.Uint32(read.header[frameOffsetSize:])
	if size > vsr.HeaderSize {
		read.payload = make([]byte, int(size)-vsr.HeaderSize)
		if _, err := io.ReadFull(conn, read.payload); err != nil {
			return read, err
		}
	}
	return read, nil
}

// replyFrameFor builds a reply whose header carries the given operation.
func replyFrameFor(operation vsr.Operation, body []byte) []byte {
	frame := make([]byte, vsr.HeaderSize+len(body))
	binary.LittleEndian.PutUint32(frame[frameOffsetSize:], uint32(len(frame)))
	frame[frameOffsetCommand] = 8
	frame[replyFrameOffsetOperation] = byte(operation)
	binary.LittleEndian.PutUint32(frame[replyFrameOffsetStatus:], 0)
	copy(frame[vsr.HeaderSize:], body)
	return frame
}

// resultSection builds the committed result prefix of a metadata reply.
func resultSection(codes ...uint32) []byte {
	section := binary.LittleEndian.AppendUint32(nil, uint32(len(codes)))
	for index, code := range codes {
		section = binary.LittleEndian.AppendUint32(section, uint32(index))
		section = binary.LittleEndian.AppendUint32(section, code)
	}
	return section
}

// registerReply builds the reply of a successful sign-in.
func registerReply(userID uint32, session uint64) []byte {
	body := binary.LittleEndian.AppendUint32(resultSection(), userID)
	body = binary.LittleEndian.AppendUint64(body, session)
	body = binary.LittleEndian.AppendUint32(body, vsr.ProtocolVersion)
	body = append(body, byte(len("0.11.0-edge.1")))
	body = append(body, "0.11.0-edge.1"...)
	return replyFrameFor(vsr.OperationRegister, body)
}

// bindReply builds the reply of a successful session bind.
func bindReply(userID uint32, session uint64) []byte {
	registered := registerReply(userID, session)
	return replyFrameFor(vsr.OperationNonReplicated, registered[vsr.HeaderSize+len(resultSection()):])
}

// clusterMetadataReply builds a roster where the node at the listener
// address is the healthy leader.
func clusterMetadataReply(t *testing.T, address string) []byte {
	t.Helper()

	metadata := iggcon.ClusterMetadata{Name: "test-cluster"}
	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	tcpPort, err := net.LookupPort("tcp", port)
	require.NoError(t, err)
	metadata.Nodes = append(metadata.Nodes, iggcon.ClusterNode{
		Name:      host,
		IP:        host,
		Role:      iggcon.RoleLeader,
		Status:    iggcon.Healthy,
		Endpoints: iggcon.TransportEndpoints{Tcp: uint16(tcpPort)},
	})

	body, err := metadata.MarshalBinary()
	require.NoError(t, err)
	return replyFrameFor(vsr.OperationNonReplicated, body)
}

// fakeVSRServer answers frames the way a single-node Iggy server does, so a
// test can drive a full connect-and-login flow without a real server.
type fakeVSRServer struct {
	mtx      sync.Mutex
	requests []wireRequest
	listener net.Listener
}

func listenFakeVSR(t *testing.T, wrap func(net.Conn) net.Conn) *fakeVSRServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	server := &fakeVSRServer{listener: listener}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			if wrap != nil {
				conn = wrap(conn)
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				for {
					read, err := readWireRequest(conn)
					if err != nil {
						return
					}
					server.mtx.Lock()
					server.requests = append(server.requests, read)
					server.mtx.Unlock()

					var answer []byte
					switch {
					case read.code() == uint32(command.BindSessionCode):
						answer = bindReply(7, read.sessionID())
					case read.code() == uint32(command.GetClusterMetadataCode):
						answer = clusterMetadataReply(t, server.listener.Addr().String())
					case read.operation() == vsr.OperationRegister:
						answer = registerReply(7, 128)
					default:
						answer = replyFrameFor(vsr.OperationNonReplicated, nil)
					}
					// Echo the request id the way the server correlates replies.
					if len(answer) >= vsr.HeaderSize && answer[frameOffsetCommand] == 8 {
						copy(answer[replyFrameOffsetRequest:replyFrameOffsetRequest+8],
							read.header[frameOffsetRequest:frameOffsetRequest+8])
					}
					if _, err := conn.Write(answer); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return server
}

func (s *fakeVSRServer) recorded() []wireRequest {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return append([]wireRequest(nil), s.requests...)
}

// sessionID reads the logical session the request carries.
func (r wireRequest) sessionID() uint64 {
	return binary.LittleEndian.Uint64(r.header[frameOffsetSession:])
}

// TestParseConnectionString_ConnectsAndLogsInAgainstAServer drives the
// full connect-and-login flow from the issue acceptance criteria: opts
// from ParseConnectionString, wired into NewIggyClient via WithTcp,
// connecting and signing in against a running (fake) server.
func TestParseConnectionString_ConnectsAndLogsInAgainstAServer(t *testing.T) {
	server := listenFakeVSR(t, nil)
	address := server.listener.Addr().String()

	opts, err := ParseConnectionString("iggy://iggy:iggy@" + address)
	require.NoError(t, err)
	cli, err := NewIggyClient(WithTcp(opts...))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	require.NoError(t, cli.Connect(context.Background()))

	recorded := server.recorded()
	require.Len(t, recorded, 2)
	// The roster read is auth-gated, so the sign-in comes first.
	assert.Equal(t, vsr.OperationRegister, recorded[0].operation())
	assert.Contains(t, string(recorded[0].payload), "iggy")
	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[1].code())
}

// TestParseConnectionString_TLSEnabledByQueryString drives the same flow
// with tls=true and a TLS-only fake server: a client that ignored the
// option would send its register frame in plaintext and the TLS handshake
// would fail, so a successful connect-and-login proves the option landed.
func TestParseConnectionString_TLSEnabledByQueryString(t *testing.T) {
	certificate, caPath := selfSignedCert(t)

	server := listenFakeVSR(t, func(conn net.Conn) net.Conn {
		return tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{certificate}})
	})
	address := server.listener.Addr().String()
	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)

	connectionString := "iggy://iggy:iggy@" + host + ":" + port +
		"?tls=true&tls_domain=localhost&tls_ca_file=" + caPath
	opts, err := ParseConnectionString(connectionString)
	require.NoError(t, err)
	cli, err := NewIggyClient(WithTcp(opts...))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	require.NoError(t, cli.Connect(context.Background()))

	recorded := server.recorded()
	require.Len(t, recorded, 2)
	assert.Equal(t, vsr.OperationRegister, recorded[0].operation())
	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[1].code())
}

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

// TestParseConnectionString_RejectsMalformedConnectionStrings pins every
// malformed shape the other SDKs reject, copied from their test tables.
func TestParseConnectionString_RejectsMalformedConnectionStrings(t *testing.T) {
	for _, value := range []string{
		"",
		"iggy",
		"iggy://",
		"iggy://:secret@localhost:8090",
		"iggy://iggy:@localhost:8090",
		"iggy://iggy:secret@localhost",
		"iggy://iggy:secret@:8090",
		"iggy://iggy:secret@localhost:port",
		"iggy://iggy:secret@localhost:70000",
		"iggy://iggy:secret@localhost:8090?unknown=value",
		"iggy://iggy:secret@localhost:8090?tls=maybe",
		"iggy://iggy:secret@localhost:8090?reconnection_retries=three",
		"iggy://iggy:secret@localhost:8090?reconnection_retries=4294967296",
		"iggy://iggy:secret@localhost:8090?reconnection_interval=5",
		"iggy://iggy:secret@localhost:8090?reconnection_interval=garbage",
		"iggy://iggy:secret@localhost:8090?reconnection_interval=-1s",
		"iggy://iggy:secret@localhost:8090?nodelay=yes",
		"iggy://iggy:secret@[::1:8090",
		"iggy://iggy:secret@[]:8090",
		"iggy://iggy:secret@[::1]x:8090",
		"iggy://iggy:secret@2001:db8::1:8090",
		"iggy://iggy:secret@host:8090:9090",
		"iggy://iggy:secret@localhost:8090?",
		"iggy://iggy:secret@localhost:8090?&",
		"iggy://iggy:secret@localhost:8090?reestablish_after=garbage",
	} {
		t.Run(value, func(t *testing.T) {
			opts, err := ParseConnectionString(value)
			require.Error(t, err)
			assert.Empty(t, opts)
		})
	}
}

// TestParseConnectionString_AcceptsPortZero pins the u16 port rule the
// other SDKs share: port 0 parses, because the port is only a u16 on the
// wire and the dial fails later instead of the parse.
func TestParseConnectionString_AcceptsPortZero(t *testing.T) {
	opts, err := ParseConnectionString("iggy://iggy:secret@localhost:0")
	require.NoError(t, err)
	client := applyOptions(t, opts)
	assert.Equal(t, "localhost:0", client.GetConnectionInfo().ServerAddress)
}

// TestParseConnectionString_AcceptsFractionalAndMultiUnitDurations pins
// the duration grammar the other SDKs share: fractional seconds and
// sequences of units, all accepted in connection-string options.
func TestParseConnectionString_AcceptsFractionalAndMultiUnitDurations(t *testing.T) {
	for _, options := range []string{
		"reconnection_interval=500ms",
		"reconnection_interval=0.5s",
		"reconnection_interval=1.005s",
		"reconnection_interval=2m",
		"reconnection_interval=1h 1m 1s",
		"reconnection_interval=1h30m",
		"reconnection_interval=5sec",
		"reconnection_interval=5msec",
		"heartbeat_interval=10s",
		"reestablish_after=10s",
	} {
		t.Run(options, func(t *testing.T) {
			opts, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?" + options)
			require.NoError(t, err)
			assert.NotEmpty(t, opts)
		})
	}
}

// TestParseConnectionString_AcceptsUnlimitedRetries pins the u32 ceiling
// rule: unlimited is the keyword, and any u32 value parses.
func TestParseConnectionString_AcceptsUnlimitedRetries(t *testing.T) {
	for _, options := range []string{
		"reconnection_retries=unlimited",
		"reconnection_retries=3",
		"reconnection_retries=4294967295",
	} {
		t.Run(options, func(t *testing.T) {
			opts, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?" + options)
			require.NoError(t, err)
			assert.NotEmpty(t, opts)
		})
	}
}

// TestParseConnectionString_RejectsUnsupportedTransports pins the scheme
// rule: the default scheme and the explicit tcp spelling parse, other
// iggy+<transport> spellings are rejected with an error naming the
// transport.
func TestParseConnectionString_RejectsUnsupportedTransports(t *testing.T) {
	for _, value := range []string{
		"iggy+quic://iggy:secret@localhost:8090",
		"iggy+ws://iggy:secret@localhost:8090",
		"http://iggy:secret@localhost:8090",
	} {
		opts, err := ParseConnectionString(value)
		require.Error(t, err)
		assert.Empty(t, opts)
	}
}

// TestParseConnectionString_ErrorsNeverLeakTheSecret checks that no error
// message contains the password or the token from the rejected string.
func TestParseConnectionString_ErrorsNeverLeakTheSecret(t *testing.T) {
	secrets := []string{"hunter2", "iggypat-1234567890abcdef"}
	for _, value := range []string{
		"iggy://iggy:hunter2@localhost",
		"iggy+tcp://iggypat-1234567890abcdef@localhost",
		"iggy://iggy:hunter2@localhost:8090?unknown=value",
		"iggy://iggy:hunter2@localhost:8090?tls=maybe",
		"iggy://iggy:hunter2@localhost:8090?reconnection_retries=three",
		"iggy://iggy:hunter2@localhost:70000",
	} {
		opts, err := ParseConnectionString(value)
		require.Error(t, err)
		assert.Empty(t, opts)
		for _, secret := range secrets {
			assert.NotContains(t, err.Error(), secret,
				"error message leaked a secret")
		}
	}
}
