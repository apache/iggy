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

// Package fakevsr provides the fake VSR server the connection-string
// tests drive a full connect-and-login flow against, without a real
// server, and the shared certificate helper the transport tests mint
// their throwaway TLS certificates with. It is an internal package, so
// it never reaches SDK users.
package fakevsr

import (
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

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/require"
)

// Frame offsets the fake server reads and writes. They mirror the
// unexported tables of the codec package, which no test can reach.
const (
	FrameOffsetSize      = 48
	FrameOffsetCommand   = 60
	FrameOffsetRequest   = 168
	FrameOffsetOperation = 176
	FrameOffsetSession   = 184
	FrameOffsetReserved  = 196

	ReplyFrameOffsetRequest   = 200
	ReplyFrameOffsetOperation = 208
	ReplyFrameOffsetStatus    = 216
)

// Request is one frame the fake server read off the wire.
type Request struct {
	Header  [vsr.HeaderSize]byte
	Payload []byte
}

// Operation reads the request operation code.
func (r Request) Operation() vsr.Operation {
	return vsr.Operation(r.Header[FrameOffsetOperation])
}

// Code reads the request command code.
func (r Request) Code() uint32 {
	return binary.LittleEndian.Uint32(r.Header[FrameOffsetReserved:])
}

// SessionID reads the logical session the request carries.
func (r Request) SessionID() uint64 {
	return binary.LittleEndian.Uint64(r.Header[FrameOffsetSession:])
}

// ReadRequest reads one complete frame from conn.
func ReadRequest(conn net.Conn) (Request, error) {
	var read Request
	if _, err := io.ReadFull(conn, read.Header[:]); err != nil {
		return read, err
	}
	size := binary.LittleEndian.Uint32(read.Header[FrameOffsetSize:])
	if size > vsr.HeaderSize {
		read.Payload = make([]byte, int(size)-vsr.HeaderSize)
		if _, err := io.ReadFull(conn, read.Payload); err != nil {
			return read, err
		}
	}
	return read, nil
}

// ReplyFrame builds a committed reply carrying body.
func ReplyFrame(operation vsr.Operation, body []byte) []byte {
	return StatusReplyFrame(operation, 0, body)
}

// StatusReplyFrame builds a reply whose header carries a pre-commit denial.
func StatusReplyFrame(operation vsr.Operation, status uint32, body []byte) []byte {
	frame := make([]byte, vsr.HeaderSize+len(body))
	binary.LittleEndian.PutUint32(frame[FrameOffsetSize:], uint32(len(frame)))
	frame[FrameOffsetCommand] = 8
	binary.LittleEndian.PutUint32(frame[ReplyFrameOffsetStatus:], status)
	frame[ReplyFrameOffsetOperation] = byte(operation)
	copy(frame[vsr.HeaderSize:], body)
	return frame
}

// ResultSection builds the committed result prefix of a metadata reply.
func ResultSection(codes ...uint32) []byte {
	section := binary.LittleEndian.AppendUint32(nil, uint32(len(codes)))
	for index, code := range codes {
		section = binary.LittleEndian.AppendUint32(section, uint32(index))
		section = binary.LittleEndian.AppendUint32(section, code)
	}
	return section
}

// RegisterReplyFrame builds the reply of a successful sign-in.
func RegisterReplyFrame(userID uint32, session uint64) []byte {
	body := binary.LittleEndian.AppendUint32(ResultSection(), userID)
	body = binary.LittleEndian.AppendUint64(body, session)
	body = binary.LittleEndian.AppendUint32(body, vsr.ProtocolVersion)
	body = append(body, byte(len("0.11.0-edge.1")))
	body = append(body, "0.11.0-edge.1"...)
	return ReplyFrame(vsr.OperationRegister, body)
}

// BindReplyFrame builds the reply of a successful session bind.
func BindReplyFrame(userID uint32, session uint64) []byte {
	registered := RegisterReplyFrame(userID, session)
	return ReplyFrame(vsr.OperationNonReplicated, registered[vsr.HeaderSize+len(ResultSection()):])
}

// ClusterMetadataFrame builds a cluster roster where the node at
// leaderIndex is the healthy leader.
func ClusterMetadataFrame(t *testing.T, leaderIndex int, addresses ...string) []byte {
	t.Helper()

	metadata := iggcon.ClusterMetadata{Name: "test-cluster"}
	for index, address := range addresses {
		host, port, err := net.SplitHostPort(address)
		require.NoError(t, err)
		tcpPort, err := net.LookupPort("tcp", port)
		require.NoError(t, err)

		role := iggcon.RoleFollower
		if index == leaderIndex {
			role = iggcon.RoleLeader
		}
		metadata.Nodes = append(metadata.Nodes, iggcon.ClusterNode{
			ReplicaID: uint8(index),
			Name:      host,
			IP:        host,
			Role:      role,
			Status:    iggcon.Healthy,
			Endpoints: iggcon.TransportEndpoints{Tcp: uint16(tcpPort)},
		})
	}

	body, err := metadata.MarshalBinary()
	require.NoError(t, err)
	return ReplyFrame(vsr.OperationNonReplicated, body)
}

// EchoReplyRequest stamps the request id into a reply frame the way the
// server echoes it, so handlers do not repeat the correlation plumbing. An
// eviction frame passes through untouched.
func EchoReplyRequest(answer []byte, read Request) {
	if len(answer) < vsr.HeaderSize || answer[FrameOffsetCommand] != 8 {
		return
	}
	copy(answer[ReplyFrameOffsetRequest:ReplyFrameOffsetRequest+8],
		read.Header[FrameOffsetRequest:FrameOffsetRequest+8])
}

// Server answers frames from a handler and records what it was asked.
type Server struct {
	mtx      sync.Mutex
	requests []Request
	listener net.Listener
}

// Listen answers frames the way a single-node Iggy server does, so a test
// can drive a full connect-and-login flow without a real server: the
// bind-session and cluster-metadata requests get their canned replies, a
// sign-in gets a successful register, and anything else gets an empty
// non-replicated reply. The wrap hook lets a test put TLS in front of the
// listener.
func Listen(t *testing.T, wrap func(net.Conn) net.Conn) *Server {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	server := &Server{listener: listener}
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
					read, err := ReadRequest(conn)
					if err != nil {
						return
					}
					server.mtx.Lock()
					server.requests = append(server.requests, read)
					server.mtx.Unlock()

					var answer []byte
					switch {
					case read.Code() == uint32(command.BindSessionCode):
						answer = BindReplyFrame(7, read.SessionID())
					case read.Code() == uint32(command.GetClusterMetadataCode):
						answer = ClusterMetadataFrame(t, 0, server.listener.Addr().String())
					case read.Operation() == vsr.OperationRegister:
						answer = RegisterReplyFrame(7, 128)
					default:
						answer = ReplyFrame(vsr.OperationNonReplicated, nil)
					}
					EchoReplyRequest(answer, read)
					if _, err := conn.Write(answer); err != nil {
						return
					}
				}
			}(conn)
		}
	}()
	return server
}

// Recorded returns the requests the server read off the wire.
func (s *Server) Recorded() []Request {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return append([]Request(nil), s.requests...)
}

// Listener returns the loopback listener, so a test can read the address
// the fake server bound to before the client connects.
func (s *Server) Listener() net.Listener {
	return s.listener
}

// SelfSignedCert mints a throwaway server certificate for the loopback
// listener, so a TLS test carries no expiry or checkout-layout dependency.
// The connection-string and TCP suites share it.
func SelfSignedCert(t *testing.T) (tls.Certificate, string) {
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
