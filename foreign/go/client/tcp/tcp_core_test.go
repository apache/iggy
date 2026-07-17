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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// emptyWireReq is an 8-byte wire payload for a zero-code request with empty body:
// [length-le32=4][code-le32=0]. Reused across tests that don't care about the body.
var emptyWireReq = []byte{4, 0, 0, 0, 0, 0, 0, 0}

// newTestClient creates an IggyTcpClient backed by an in-memory net.Pipe connection.
// Returns the client and the server-side end of the pipe; caller must close the server conn.
func newTestClient(t *testing.T) (*IggyTcpClient, net.Conn) {
	t.Helper()
	serverConn, clientConn := net.Pipe()
	c := &IggyTcpClient{
		conn:           clientConn,
		transportState: iggcon.TransportStateConnected,
		sessionState:   iggcon.SessionStateUnauthenticated,
		logger:         slog.New(slog.DiscardHandler),
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
	_, err := c.sendWireAndFetchResponse(nil, emptyWireReq) //nolint:staticcheck
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ierror.ErrNilContext) {
		t.Errorf("got %v, want %v", err, ierror.ErrNilContext)
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
			_, err := c.sendWireAndFetchResponse(tt.ctx, emptyWireReq)
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

	_, err := c.sendWireAndFetchResponse(ctx, emptyWireReq)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want context.DeadlineExceeded", err)
	}
	// After a timeout, the connection should be invalidated.
	if c.transportState != iggcon.TransportStateDisconnected {
		t.Errorf("expected state %v, got %v", iggcon.TransportStateDisconnected, c.transportState)
	}

	// TODO: revisit after reconnect implementation
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
	_, err := c.sendWireAndFetchResponse(ctx, emptyWireReq)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want context.Canceled", err)
	}
	// Connection should be invalidated after the I/O error.
	if c.transportState != iggcon.TransportStateDisconnected {
		t.Errorf("expected state %v, got %v", iggcon.TransportStateDisconnected, c.transportState)
	}
}

// serverRespond is a test helper that reads the full request from the pipe
// and writes back a response with the given status code and payload.
func serverRespond(t *testing.T, serverConn net.Conn, status uint32, payload []byte) {
	t.Helper()
	_ = serverRespondCapture(t, serverConn, status, payload)
}

// serverRespondCapture is like serverRespond but returns the request bytes
// (4-byte command code followed by the marshaled body) it read off the pipe.
// Returns nil if the read or write fails.
func serverRespondCapture(t *testing.T, serverConn net.Conn, status uint32, payload []byte) []byte {
	t.Helper()

	var lengthBuf [RequestInitialBytesLength]byte
	if _, err := serverConn.Read(lengthBuf[:]); err != nil {
		t.Errorf("server: read request length: %v", err)
		return nil
	}
	reqLen := int(binary.LittleEndian.Uint32(lengthBuf[:]))
	req := make([]byte, reqLen)
	if _, err := serverConn.Read(req); err != nil {
		t.Errorf("server: read request body: %v", err)
		return nil
	}

	resp := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(resp[0:4], status)
	binary.LittleEndian.PutUint32(resp[4:8], uint32(len(payload)))
	copy(resp[8:], payload)
	if _, err := serverConn.Write(resp); err != nil {
		t.Errorf("server: write response: %v", err)
		return nil
	}
	return req
}

func TestSendAndFetchResponse_ErrorStatus(t *testing.T) {
	c, serverConn := newTestClient(t)

	go serverRespond(t, serverConn, uint32(ierror.UnauthenticatedCode), nil)

	_, err := c.sendWireAndFetchResponse(context.Background(), emptyWireReq)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	// Should return the iggy error corresponding to the status code.
	if !errors.Is(err, ierror.ErrUnauthenticated) {
		t.Errorf("got %v, want %v", err, ierror.ErrUnauthenticated)
	}
	// Connection should remain healthy after an application-level error.
	if c.transportState != iggcon.TransportStateConnected {
		t.Errorf("expected state %v, got %v", iggcon.TransportStateConnected, c.transportState)
	}
}

func TestSendAndFetchResponse_SuccessEmptyBody(t *testing.T) {
	c, serverConn := newTestClient(t)

	go serverRespond(t, serverConn, 0, nil)

	result, err := c.sendWireAndFetchResponse(context.Background(), emptyWireReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d bytes", len(result))
	}
	if c.transportState != iggcon.TransportStateConnected {
		t.Errorf("expected state %v, got %v", iggcon.TransportStateConnected, c.transportState)
	}
}

func TestSendAndFetchResponse_SuccessWithBody(t *testing.T) {
	c, serverConn := newTestClient(t)

	body := []byte("hello iggy")
	go serverRespond(t, serverConn, 0, body)

	result, err := c.sendWireAndFetchResponse(context.Background(), emptyWireReq)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(result) != string(body) {
		t.Errorf("got %q, want %q", result, body)
	}
	if c.transportState != iggcon.TransportStateConnected {
		t.Errorf("expected state %v, got %v", iggcon.TransportStateConnected, c.transportState)
	}
}

func TestNewIggyTcpClient_StoresProvidedLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	c := NewIggyTcpClient(logger)

	c.logger.Info("transport probe", slog.String("source", "tcp"))

	output := buf.String()
	if !strings.Contains(output, "transport probe") {
		t.Errorf("expected logger output to contain 'transport probe', got: %q", output)
	}
	if !strings.Contains(output, "source=tcp") {
		t.Errorf("expected logger output to contain 'source=tcp', got: %q", output)
	}
}

func TestLoginUser_LoginAndLogout(t *testing.T) {
	c, serverConn := newTestClient(t)

	identity := make([]byte, 4)
	binary.LittleEndian.PutUint32(identity, 42)

	go func() {
		serverRespond(t, serverConn, 0, identity)
		// login always probes for a leader afterwards; it must be answered or the call blocks.
		serverRespond(t, serverConn, uint32(ierror.FeatureUnavailableCode), nil)
		serverRespond(t, serverConn, 0, nil)
	}()

	ctx := context.Background()
	info, err := c.LoginUser(ctx, "iggy", "iggy")
	if err != nil {
		t.Fatalf("unexpected login error: %v", err)
	}
	if info.UserId != 42 {
		t.Errorf("got user id %d, want 42", info.UserId)
	}
	if c.sessionState != iggcon.SessionStateAuthenticated {
		t.Errorf("expected session %v after login, got %v", iggcon.SessionStateAuthenticated, c.sessionState)
	}

	if err := c.LogoutUser(ctx); err != nil {
		t.Fatalf("unexpected logout error: %v", err)
	}
	if c.sessionState != iggcon.SessionStateUnauthenticated {
		t.Errorf("expected session %v after logout, got %v", iggcon.SessionStateUnauthenticated, c.sessionState)
	}
}

func TestLoginUser_RejectedReloginKeepsExistingSession(t *testing.T) {
	c, serverConn := newTestClient(t)
	c.sessionState = iggcon.SessionStateAuthenticated

	go serverRespond(t, serverConn, uint32(ierror.InvalidCredentialsCode), nil)

	_, err := c.LoginUser(context.Background(), "other-user", "wrong-password")
	if err == nil {
		t.Fatal("expected login error, got nil")
	}
	if !errors.Is(err, ierror.ErrInvalidCredentials) {
		t.Errorf("got %v, want %v", err, ierror.ErrInvalidCredentials)
	}
	// The server rejects the login before touching the existing session,
	// so the client must keep reporting the session it still has.
	if c.sessionState != iggcon.SessionStateAuthenticated {
		t.Errorf("expected session to stay authenticated after rejected relogin, got %v", c.sessionState)
	}
}

var errCloseFailed = errors.New("close failed")

// failingCloseConn is a connection whose Close always fails, standing in for a
// socket whose teardown reports an error the client cannot act on.
type failingCloseConn struct {
	net.Conn
	closes int
}

func (f *failingCloseConn) Close() error {
	f.closes++
	return errCloseFailed
}

func TestShutdown_FailedCloseStillCompletesTeardown(t *testing.T) {
	c, _ := newTestClient(t)
	conn := &failingCloseConn{Conn: c.conn}
	c.conn = conn

	if err := c.shutdown(); !errors.Is(err, errCloseFailed) {
		t.Fatalf("got %v, want %v", err, errCloseFailed)
	}
	if c.transportState != iggcon.TransportStateShutdown {
		t.Errorf("expected state %v, got %v", iggcon.TransportStateShutdown, c.transportState)
	}
	if c.conn != nil {
		t.Error("expected the closed connection to be dropped")
	}

	if err := c.shutdown(); err != nil {
		t.Errorf("expected the second shutdown to be a no-op, got %v", err)
	}
	if conn.closes != 1 {
		t.Errorf("expected the connection to be closed once, got %d", conn.closes)
	}
}

func TestDisconnect_ShutdownClientIsNotResurrected(t *testing.T) {
	c, _ := newTestClient(t)

	if err := c.shutdown(); err != nil {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
	if err := c.disconnect(); err != nil {
		t.Fatalf("unexpected disconnect error: %v", err)
	}

	if c.transportState != iggcon.TransportStateShutdown {
		t.Errorf("expected state to stay %v, got %v", iggcon.TransportStateShutdown, c.transportState)
	}

	_, err := c.sendWireAndFetchResponse(context.Background(), emptyWireReq)
	if !errors.Is(err, ierror.ErrClientShutdown) {
		t.Errorf("got %v, want %v", err, ierror.ErrClientShutdown)
	}
}
