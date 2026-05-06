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
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
)

// newTestClient creates an IggyTcpClient backed by an in-memory net.Pipe connection.
// Returns the client and the server-side end of the pipe; caller must close the server conn.
func newTestClient(t *testing.T) (*IggyTcpClient, net.Conn) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	c := &IggyTcpClient{
		conn:  clientConn,
		state: iggcon.StateConnected,
	}
	t.Cleanup(func() {
		err := clientConn.Close()
		if err != nil {
			t.Errorf("error closing client connection: %v", err)
		}
	})
	t.Cleanup(func() {
		err := serverConn.Close()
		if err != nil {
			t.Errorf("error closing server connection: %v", err)
		}
	})
	return c, serverConn
}

func TestSendAndFetchResponse_NilContext(t *testing.T) {
	c, _ := newTestClient(t)
	_, err := c.sendAndFetchResponse(nil, []byte{}, command.Code(0)) //nolint:staticcheck
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "nil context") {
		t.Errorf("got %q, want it to contain %q", err.Error(), "nil context")
	}
}

func TestSendAndFetchResponse_ContextErrors(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	expiredCtx, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expiredCancel()

	tests := []struct {
		name    string
		ctx     context.Context
		wantErr error
	}{
		{
			name:    "canceled",
			ctx:     canceledCtx,
			wantErr: context.Canceled,
		},
		{
			name:    "expired",
			ctx:     expiredCtx,
			wantErr: context.DeadlineExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, _ := newTestClient(t)

			// server does not respond, but it doesn't matter.
			// ctx.Err() should fire before any I/O is attempted.
			_, err := c.sendAndFetchResponse(tt.ctx, []byte{}, command.Code(0))
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("got %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestSendAndFetchResponse_DeadlineTimeout(t *testing.T) {
	c, _ := newTestClient(t)

	// server intentionally does not read or write, causing the client to block
	// until the context deadline fires and SetDeadline triggers a timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := c.sendAndFetchResponse(ctx, []byte{}, command.Code(0))
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want context.DeadlineExceeded", err)
	}
	// After a timeout, the connection should be invalidated.
	if c.state != iggcon.StateDisconnected {
		t.Errorf("expected state %v, got %v", iggcon.StateDisconnected, c.state)
	}
}

func TestSendAndFetchResponse_CancelDuringIO(t *testing.T) {
	c, _ := newTestClient(t)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel the context after a short delay to unblock the I/O.
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// Server does not respond, so the client blocks until the context is cancelled.
	_, err := c.sendAndFetchResponse(ctx, []byte{}, command.Code(0))
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
	// Connection should be invalidated after the I/O error.
	if c.state != iggcon.StateDisconnected {
		t.Errorf("expected state %v, got %v", iggcon.StateDisconnected, c.state)
	}
}

// serverRespond is a test helper that reads the full request from the pipe
// and writes back a response with the given status code and payload.
func serverRespond(t *testing.T, serverConn net.Conn, status uint32, payload []byte) {
	t.Helper()

	var lengthBuf [RequestInitialBytesLength]byte
	if _, err := serverConn.Read(lengthBuf[:]); err != nil {
		t.Errorf("server: read request length: %v", err)
		return
	}
	reqLen := int(binary.LittleEndian.Uint32(lengthBuf[:]))
	discard := make([]byte, reqLen)
	if _, err := serverConn.Read(discard); err != nil {
		t.Errorf("server: read request body: %v", err)
		return
	}

	resp := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(resp[0:4], status)
	binary.LittleEndian.PutUint32(resp[4:8], uint32(len(payload)))
	copy(resp[8:], payload)
	if _, err := serverConn.Write(resp); err != nil {
		t.Errorf("server: write response: %v", err)
	}
}

func TestSendAndFetchResponse_ErrorStatus(t *testing.T) {
	c, serverConn := newTestClient(t)

	go serverRespond(t, serverConn, uint32(ierror.UnauthenticatedCode), nil)

	_, err := c.sendAndFetchResponse(context.Background(), []byte{}, command.Code(0))
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should return the iggy error corresponding to the status code.
	if !errors.Is(err, ierror.ErrUnauthenticated) {
		t.Errorf("got %v, want %v", err, ierror.ErrUnauthenticated)
	}
	// Connection should remain healthy after an application-level error.
	if c.state != iggcon.StateConnected {
		t.Errorf("expected state %v, got %v", iggcon.StateConnected, c.state)
	}
}

func TestSendAndFetchResponse_SuccessEmptyBody(t *testing.T) {
	c, serverConn := newTestClient(t)

	go serverRespond(t, serverConn, 0, nil)

	result, err := c.sendAndFetchResponse(context.Background(), []byte{}, command.Code(0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d bytes", len(result))
	}
	if c.state != iggcon.StateConnected {
		t.Errorf("expected state %v, got %v", iggcon.StateConnected, c.state)
	}
}

func TestSendAndFetchResponse_SuccessWithBody(t *testing.T) {
	c, serverConn := newTestClient(t)

	body := []byte("hello iggy")
	go serverRespond(t, serverConn, 0, body)

	result, err := c.sendAndFetchResponse(context.Background(), []byte{}, command.Code(0))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != string(body) {
		t.Errorf("got %q, want %q", result, body)
	}
	if c.state != iggcon.StateConnected {
		t.Errorf("expected state %v, got %v", iggcon.StateConnected, c.state)
	}
}
