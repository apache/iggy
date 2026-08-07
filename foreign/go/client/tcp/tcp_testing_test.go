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
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/require"
)

// Frame offsets the fake server reads and writes. They mirror the unexported
// tables of the codec package, which the transport tests cannot reach.
const (
	frameOffsetSize      = 48
	frameOffsetCommand   = 60
	frameOffsetClient    = 128
	frameOffsetRequest   = 168
	frameOffsetOperation = 176
	frameOffsetNamespace = 184
	frameOffsetSession   = 192
	frameOffsetReserved  = 204

	replyFrameOffsetOperation = 208
	replyFrameOffsetStatus    = 224

	evictionFrameOffsetVersion    = 144
	evictionFrameOffsetVersionMin = 148
	evictionFrameOffsetReason     = 255
)

// request is one frame the fake server read off the wire.
type request struct {
	header  [vsr.HeaderSize]byte
	payload []byte
}

func (r request) operation() vsr.Operation { return vsr.Operation(r.header[frameOffsetOperation]) }
func (r request) code() uint32 {
	return binary.LittleEndian.Uint32(r.header[frameOffsetReserved:])
}
func (r request) requestID() uint64 {
	return binary.LittleEndian.Uint64(r.header[frameOffsetRequest:])
}
func (r request) sessionID() uint64 {
	return binary.LittleEndian.Uint64(r.header[frameOffsetSession:])
}
func (r request) namespace() uint64 {
	return binary.LittleEndian.Uint64(r.header[frameOffsetNamespace:])
}
func (r request) clientID() vsr.ClientID {
	return vsr.ClientID{
		Lo: binary.LittleEndian.Uint64(r.header[frameOffsetClient:]),
		Hi: binary.LittleEndian.Uint64(r.header[frameOffsetClient+8:]),
	}
}

// replyFrame builds a committed reply carrying body.
func replyFrame(operation vsr.Operation, body []byte) []byte {
	return statusReplyFrame(operation, 0, body)
}

// statusReplyFrame builds a reply whose header carries a pre-commit denial.
func statusReplyFrame(operation vsr.Operation, status uint32, body []byte) []byte {
	frame := make([]byte, vsr.HeaderSize+len(body))
	binary.LittleEndian.PutUint32(frame[frameOffsetSize:], uint32(len(frame)))
	frame[frameOffsetCommand] = 8
	frame[replyFrameOffsetOperation] = byte(operation)
	binary.LittleEndian.PutUint32(frame[replyFrameOffsetStatus:], status)
	copy(frame[vsr.HeaderSize:], body)
	return frame
}

// evictionFrame builds a header-only session-terminal frame.
func evictionFrame(reason vsr.EvictionReason, version, versionMin uint32) []byte {
	frame := make([]byte, vsr.HeaderSize)
	binary.LittleEndian.PutUint32(frame[frameOffsetSize:], vsr.HeaderSize)
	frame[frameOffsetCommand] = 13
	binary.LittleEndian.PutUint32(frame[evictionFrameOffsetVersion:], version)
	binary.LittleEndian.PutUint32(frame[evictionFrameOffsetVersionMin:], versionMin)
	frame[evictionFrameOffsetReason] = byte(reason)
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

// registerReplyFrame builds the reply of a successful sign-in.
func registerReplyFrame(userID uint32, session uint64) []byte {
	body := binary.LittleEndian.AppendUint32(resultSection(), userID)
	body = binary.LittleEndian.AppendUint64(body, session)
	body = binary.LittleEndian.AppendUint32(body, vsr.ProtocolVersion)
	body = append(body, byte(len("0.11.0-edge.1")))
	body = append(body, "0.11.0-edge.1"...)
	return replyFrame(vsr.OperationRegister, body)
}

// clusterMetadataFrame builds a cluster roster where the node at leaderIndex
// is the healthy leader.
func clusterMetadataFrame(t *testing.T, leaderIndex int, addresses ...string) []byte {
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
			Name:      host,
			IP:        host,
			Role:      role,
			Status:    iggcon.Healthy,
			Endpoints: iggcon.TransportEndpoints{Tcp: uint16(tcpPort)},
		})
	}

	body, err := metadata.MarshalBinary()
	require.NoError(t, err)
	return replyFrame(vsr.OperationNonReplicated, body)
}

// readRequest reads one complete frame from conn.
func readRequest(conn net.Conn) (request, error) {
	var read request
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

// fakeServer answers frames from a handler and records what it was asked.
type fakeServer struct {
	mtx      sync.Mutex
	requests []request
	done     chan struct{}
}

// serve reads frames off conn until it closes, answering each with the frame
// the handler returns. A nil answer leaves the request unanswered.
func serve(conn net.Conn, handler func(index int, read request) []byte) *fakeServer {
	server := &fakeServer{done: make(chan struct{})}
	go func() {
		defer close(server.done)
		for index := 0; ; index++ {
			read, err := readRequest(conn)
			if err != nil {
				return
			}
			server.mtx.Lock()
			server.requests = append(server.requests, read)
			server.mtx.Unlock()

			answer := handler(index, read)
			if answer == nil {
				continue
			}
			if _, err := conn.Write(answer); err != nil {
				return
			}
		}
	}()
	return server
}

func (s *fakeServer) recorded() []request {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return append([]request(nil), s.requests...)
}

// newPipeClient builds a client wired to one end of an in-memory pipe, with a
// session already bound so replicated commands encode.
func newPipeClient(t *testing.T) (*IggyTcpClient, net.Conn) {
	t.Helper()

	serverConn, clientConn := net.Pipe()
	client := newTestClient(t, clientConn)
	client.session.BeginRegister()
	require.NoError(t, client.session.Bind(100))
	client.sessionState = iggcon.SessionStateAuthenticated

	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	return client, serverConn
}

// newTestClient builds a connected client over an established connection.
func newTestClient(t *testing.T, conn net.Conn) *IggyTcpClient {
	t.Helper()
	return &IggyTcpClient{
		conn:           conn,
		transportState: iggcon.TransportStateConnected,
		sessionState:   iggcon.SessionStateUnauthenticated,
		logger:         slog.New(slog.DiscardHandler),
		session:        vsr.NewSession(),
		config:         defaultTcpClientConfig(),
	}
}
