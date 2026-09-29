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
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func numericIdentifier(t *testing.T, id uint32) iggcon.Identifier {
	t.Helper()
	identifier, err := iggcon.NewIdentifier(id)
	require.NoError(t, err)
	return identifier
}

func TestExchange_RejectsANilContext(t *testing.T) {
	client, _ := newPipeClient(t)

	_, err := client.SendBinaryRequest(nil, uint32(command.PingCode), nil) //nolint:staticcheck
	assert.ErrorIs(t, err, ierror.ErrNilContext)
}

func TestExchange_ReportsAContextThatIsAlreadyDone(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expiredCancel()

	tests := []struct {
		name string
		ctx  context.Context
		want error
	}{
		{name: "cancelled", ctx: cancelled, want: context.Canceled},
		{name: "expired", ctx: expired, want: context.DeadlineExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _ := newPipeClient(t)
			_, err := client.SendBinaryRequest(test.ctx, uint32(command.PingCode), nil)
			assert.ErrorIs(t, err, test.want)
		})
	}
}

type cancelOnDeadlineConn struct {
	net.Conn
	cancel context.CancelFunc
}

func (c cancelOnDeadlineConn) SetReadDeadline(deadline time.Time) error {
	err := c.Conn.SetReadDeadline(deadline)
	if !deadline.IsZero() {
		c.cancel()
	}
	return err
}

func TestExchange_DoesNotSendWhenCancelledBeforeTheWrite(t *testing.T) {
	client, serverConn := newPipeClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.conn = cancelOnDeadlineConn{Conn: client.conn, cancel: cancel}
	client.reader = bufio.NewReaderSize(client.conn, connectionReadBufferSize)
	identity := client.session.ClientID()
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateStream, resultSection())
	})

	_, err := client.do(ctx, &command.CreateStream{Name: "orders"})
	require.ErrorIs(t, err, context.Canceled)
	_, err = client.do(context.Background(), &command.CreateStream{Name: "payments"})
	require.NoError(t, err)
	assert.Len(t, server.recorded(), 1, "the cancelled request must not reach the server")
	assert.Equal(t, identity, client.session.ClientID())
}

func TestExchange_RefusesToSendWhileNotConnected(t *testing.T) {
	tests := []struct {
		name  string
		state iggcon.TransportState
		want  error
	}{
		{name: "shutdown", state: iggcon.TransportStateShutdown, want: ierror.ErrClientShutdown},
		{name: "disconnected", state: iggcon.TransportStateDisconnected, want: ierror.ErrNotConnected},
		{name: "connecting", state: iggcon.TransportStateConnecting, want: ierror.ErrNotConnected},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, _ := newPipeClient(t)
			client.transportState = test.state
			client.config.reconnection.enabled = false

			_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
			assert.ErrorIs(t, err, test.want)
		})
	}
}

func TestExchange_SendsAFrameTheServerCanRead(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, []byte{1, 2, 3})
	})

	response, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3}, response)

	recorded := server.recorded()
	require.Len(t, recorded, 1)
	assert.Equal(t, vsr.OperationNonReplicated, recorded[0].operation())
	assert.Equal(t, uint32(command.PingCode), recorded[0].code())
	assert.Equal(t, uint64(100), recorded[0].sessionID())
	assert.False(t, recorded[0].clientID().IsZero())
}

func TestExchange_ForwardsAVendorCodeInTheReservedField(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	_, err := client.SendBinaryRequest(context.Background(), 60000, []byte{7, 7})
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 1)
	assert.Equal(t, uint32(60000), recorded[0].code())
	assert.Equal(t, []byte{7, 7}, recorded[0].payload)
}

func TestExchange_KeepsRepeatedVendorCodesFromGappingMetadataRequestIDs(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.operation() == vsr.OperationCreateStream {
			return replyFrame(vsr.OperationCreateStream, resultSection())
		}
		return replyFrame(vsr.OperationNonReplicated, nil)
	})

	for range 3 {
		_, err := client.SendBinaryRequest(context.Background(), 60000, nil)
		require.NoError(t, err)
	}
	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 4)
	for index := range 3 {
		assert.Equal(t, uint64(1), recorded[index].requestID(),
			"a non-replicated request reads the watermark without consuming it")
	}
	assert.Equal(t, uint64(1), recorded[3].requestID(),
		"the metadata request still takes the first id")
}

func TestSendBinaryRequest_RejectsSessionControlCodesWithoutWriting(t *testing.T) {
	codes := []command.Code{
		command.LoginUserCode,
		command.LogoutUserCode,
		command.LoginRegisterCode,
		command.LoginWithAccessTokenCode,
		command.LoginRegisterWithPATCode,
	}
	for _, code := range codes {
		client, serverConn := newPipeClient(t)
		server := serve(serverConn, func(_ int, _ request) []byte {
			return replyFrame(vsr.OperationNonReplicated, nil)
		})

		_, err := client.SendBinaryRequest(context.Background(), uint32(code), nil)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand, "code %d", code)
		assert.Empty(t, server.recorded(), "code %d reached the wire", code)
	}
}

func TestExchange_StripsTheResultSectionOfAMetadataReply(t *testing.T) {
	client, serverConn := newPipeClient(t)
	payload := []byte{9, 8, 7}
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateStream, append(resultSection(), payload...))
	})

	response, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	require.NoError(t, err)
	assert.Equal(t, payload, response)
}

func TestExchange_SurfacesACommittedRejection(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationCreateStream,
			resultSection(uint32(ierror.StreamNameAlreadyExistsCode)))
	})

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	assert.ErrorIs(t, err, ierror.FromCode(ierror.StreamNameAlreadyExistsCode))
}

func TestExchange_SurfacesAHeaderStatusAndIgnoresTheBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		// A denial ships no body, so a decoder that read one would misparse.
		return statusReplyFrame(vsr.OperationCreateStream,
			uint32(ierror.UnauthorizedCode), []byte{0xFF, 0xFF, 0xFF, 0xFF})
	})

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	assert.ErrorIs(t, err, ierror.ErrUnauthorized)
}

func TestExchange_ReplaysTheIdenticalFrameWhileTheServerAnswersNotCommitted(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(index int, _ request) []byte {
		if index < 2 {
			return statusReplyFrame(vsr.OperationCreateStream,
				uint32(ierror.TransientNotCommittedCode), nil)
		}
		return replyFrame(vsr.OperationCreateStream, resultSection())
	})

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 3)
	assert.Equal(t, recorded[0].header, recorded[1].header,
		"a replay must carry the same client and request id for the server to deduplicate it")
	assert.Equal(t, recorded[0].header, recorded[2].header)
	assert.Equal(t, recorded[0].payload, recorded[2].payload)
}

func TestExchange_GivesUpOnNotCommittedWhenTheBudgetExpires(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return statusReplyFrame(vsr.OperationCreateStream,
			uint32(ierror.TransientNotCommittedCode), nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := client.do(ctx, &command.CreateStream{Name: "orders"})
	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestExchange_EscalatesNotAcceptedToALeaderRecheck(t *testing.T) {
	client, serverConn := newPipeClient(t)
	// A single-node roster short-circuits the redirect, so the request keeps
	// replaying on this connection until the caller's budget runs out.
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.code() == uint32(command.GetClusterMetadataCode) {
			return clusterMetadataFrame(t, 0, "127.0.0.1:8090")
		}
		return statusReplyFrame(vsr.OperationCreateStream,
			uint32(ierror.TransientNotAcceptedCode), nil)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client.config.reconnection.enabled = false

	_, err := client.do(ctx, &command.CreateStream{Name: "orders"})
	require.Error(t, err)

	var sawMetadata bool
	for _, recorded := range server.recorded() {
		if recorded.code() == uint32(command.GetClusterMetadataCode) {
			sawMetadata = true
		}
	}
	assert.True(t, sawMetadata, "the client re-checked leadership")
}

func TestExchange_ResetsTheSessionOnAnEviction(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	before := client.session.ClientID()
	serve(serverConn, func(_ int, _ request) []byte {
		return evictionFrame(vsr.EvictionStaleClient, 0, 0)
	})

	_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
	assert.ErrorIs(t, err, ierror.ErrStaleClient)

	var eviction *vsr.EvictionError
	assert.ErrorAs(t, err, &eviction)
	assert.False(t, client.session.Bound(), "the fence no longer holds")
	assert.NotEqual(t, before, client.session.ClientID(), "a new identity is minted")
	assert.Equal(t, iggcon.SessionStateUnauthenticated, client.sessionState)
}

func TestExchange_MapsEveryEvictionReasonThatReachesTheCaller(t *testing.T) {
	tests := []struct {
		reason vsr.EvictionReason
		want   error
	}{
		{reason: vsr.EvictionInvalidCredentials, want: ierror.ErrInvalidCredentials},
		{reason: vsr.EvictionInvalidToken, want: ierror.ErrInvalidPersonalAccessToken},
		{reason: vsr.EvictionMalformedLogin, want: ierror.ErrInvalidFormat},
		{reason: vsr.EvictionInvalidRequestBody, want: ierror.ErrInvalidCommand},
	}
	for _, test := range tests {
		client, serverConn := newPipeClient(t)
		client.config.reconnection.enabled = false
		serve(serverConn, func(_ int, _ request) []byte {
			return evictionFrame(test.reason, 0, 0)
		})

		_, err := client.do(context.Background(), &command.CreateStream{Name: "orders"})
		assert.ErrorIs(t, err, test.want, "reason %d", test.reason)
	}
}

func TestExchange_InvalidatesTheConnectionOnAFrameShorterThanItsHeader(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	serve(serverConn, func(_ int, _ request) []byte {
		frame := replyFrame(vsr.OperationNonReplicated, nil)
		binary.LittleEndian.PutUint32(frame[frameOffsetSize:], vsr.HeaderSize-1)
		return frame
	})

	_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState,
		"the stream is at an unknown boundary, so the connection is dropped")
}

func TestExchange_RejectsAFrameAboveTheTransportLimitWithoutReadingItsBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	serve(serverConn, func(_ int, _ request) []byte {
		frame := replyFrame(vsr.OperationNonReplicated, nil)
		binary.LittleEndian.PutUint32(frame[frameOffsetSize:], vsr.MaxFrameSize+1)
		return frame
	})

	_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
}

func TestExchange_InvalidatesTheConnectionWhenTheReplyIsCutShort(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	serve(serverConn, func(_ int, _ request) []byte {
		_ = serverConn.Close()
		return nil
	})

	_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
	assert.ErrorIs(t, err, ierror.ErrDisconnected)
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
}

func TestExchange_KeepsTheConnectionWhenTheCallerGivesUpMidRead(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
		cancel  bool
		want    error
	}{
		{name: "cancelled", timeout: time.Hour, cancel: true, want: context.Canceled},
		{name: "deadline passed", timeout: 50 * time.Millisecond, want: context.DeadlineExceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, serverConn := newPipeClient(t)
				client.config.reconnection.enabled = false
				identity := client.session.ClientID()
				entered := make(chan struct{})
				release := make(chan struct{})
				server := serve(serverConn, func(index int, _ request) []byte {
					if index == 0 {
						close(entered)
						<-release
						return replyFrame(vsr.OperationNonReplicated, []byte("late"))
					}
					return replyFrame(vsr.OperationNonReplicated, []byte("next"))
				})
				ctx, cancel := context.WithTimeout(context.Background(), test.timeout)
				defer cancel()

				returned := make(chan error, 1)
				go func() {
					_, err := client.SendBinaryRequest(ctx, uint32(command.PingCode), nil)
					returned <- err
				}()
				<-entered
				if test.cancel {
					cancel()
				}
				select {
				case err := <-returned:
					require.ErrorIs(t, err, test.want)
				case <-time.After(time.Second):
					t.Fatal("the caller waited for the reply instead of giving up")
				}
				close(release)

				response, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
				require.NoError(t, err)
				assert.Equal(t, []byte("next"), response, "the late reply must never answer the next request")
				recorded := server.recorded()
				require.Len(t, recorded, 2)
				assert.Equal(t, recorded[0].clientID(), recorded[1].clientID())
				assert.Equal(t, recorded[0].sessionID(), recorded[1].sessionID())
				assert.Equal(t, identity, client.session.ClientID())
				assert.Equal(t, iggcon.TransportStateConnected, client.transportState)
			})
		})
	}
}

type observedReadConn struct {
	net.Conn
	maxRead     int
	readBytes   int
	cancelAfter int
	cancel      context.CancelFunc
}

func (c *observedReadConn) Read(buffer []byte) (int, error) {
	c.maxRead = max(c.maxRead, len(buffer))
	count, err := c.Conn.Read(buffer)
	c.readBytes += count
	if count > 0 && c.cancel != nil && c.readBytes >= c.cancelAfter {
		c.cancel()
	}
	return count, err
}

func TestExchange_ResumesPartialRepliesAfterCancellation(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		transport := "TCP"
		if encrypted {
			transport = "TLS"
		}
		for _, phase := range []string{"header", "body"} {
			t.Run(transport+"/"+phase, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					client, serverConn := newPipeClient(t)
					if encrypted {
						certificate, _ := selfSignedCert(t)
						leaf, err := x509.ParseCertificate(certificate.Certificate[0])
						require.NoError(t, err)
						roots := x509.NewCertPool()
						roots.AddCert(leaf)
						serverTLS := tls.Server(serverConn, &tls.Config{Certificates: []tls.Certificate{certificate}})
						clientTLS := tls.Client(client.conn, &tls.Config{RootCAs: roots, ServerName: "localhost"})
						handshake := make(chan error, 1)
						go func() { handshake <- serverTLS.Handshake() }()
						require.NoError(t, clientTLS.Handshake())
						require.NoError(t, <-handshake)
						client.conn = clientTLS
						serverConn = serverTLS
					}
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					const bodyPrefix = 3
					split := vsr.HeaderSize / 2
					if phase == "body" {
						split = vsr.HeaderSize + bodyPrefix
					}
					observed := &observedReadConn{Conn: client.conn, cancelAfter: split, cancel: cancel}
					client.conn = observed
					client.reader = bufio.NewReaderSize(observed, connectionReadBufferSize)
					identity := client.session.ClientID()
					release := make(chan struct{})
					server := serve(serverConn, func(index int, read request) []byte {
						if index != 0 {
							return replyFrame(vsr.OperationNonReplicated, []byte("next"))
						}
						answer := replyFrame(vsr.OperationNonReplicated, []byte("late reply"))
						echoReplyRequest(answer, read)
						if phase == "body" {
							_, _ = serverConn.Write(answer[:vsr.HeaderSize])
							_, _ = serverConn.Write(answer[vsr.HeaderSize:split])
						} else {
							_, _ = serverConn.Write(answer[:split])
						}
						<-release
						_, _ = serverConn.Write(answer[split:])
						return nil
					})

					_, err := client.SendBinaryRequest(ctx, uint32(command.PingCode), nil)
					require.ErrorIs(t, err, context.Canceled)
					close(release)
					response, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
					require.NoError(t, err)
					assert.Equal(t, []byte("next"), response)
					assert.Equal(t, identity, client.session.ClientID())
					assert.Len(t, server.recorded(), 2)
				})
			})
		}
	}
}

func TestExchange_DrainsAbandonedRepliesWithoutAllocatingTheBody(t *testing.T) {
	const bodySize = 128 * 1024
	const watermark = uint64(42)
	for _, outcome := range []string{"complete", "truncated", "wrong echo", "evicted"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, serverConn := newPipeClient(t)
				var logs bytes.Buffer
				client.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
				transport := &observedReadConn{Conn: client.conn}
				client.conn = transport
				client.reader = bufio.NewReaderSize(transport, connectionReadBufferSize)
				client.config.reconnection.enabled = false
				identity := client.session.ClientID()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				release := make(chan struct{})
				serve(serverConn, func(index int, read request) []byte {
					if index != 0 {
						return replyFrame(vsr.OperationNonReplicated, []byte("next"))
					}
					cancel()
					<-release
					answer := replyFrame(vsr.OperationCreateStream, make([]byte, bodySize))
					echoReplyRequest(answer, read)
					binary.LittleEndian.PutUint64(answer[testMetadataCommitOffset:], watermark)
					switch outcome {
					case "truncated":
						answer = answer[:vsr.HeaderSize+bodySize/2]
					case "wrong echo":
						binary.LittleEndian.PutUint64(answer[replyFrameOffsetRequest:], read.requestID()+1)
					case "evicted":
						answer = evictionFrame(vsr.EvictionNoSession, 0, 0)
					}
					_, _ = serverConn.Write(answer)
					if outcome == "truncated" {
						_ = serverConn.Close()
					}
					return nil
				})

				_, err := client.do(ctx, &command.CreateStream{Name: "orders"})
				require.ErrorIs(t, err, context.Canceled)
				close(release)
				require.NoError(t, client.acquireExchange(context.Background()))
				client.releaseExchange()
				assert.Contains(t, logs.String(), fmt.Sprintf("msg=\"Abandoned TCP reply drain finished\" code=%d", command.CreateStreamCode))
				if outcome != "complete" {
					assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
					assert.False(t, client.session.Bound())
					if outcome == "wrong echo" {
						assert.Zero(t, client.metadataWatermark.Load())
					}
					return
				}
				assert.LessOrEqual(t, transport.maxRead, connectionReadBufferSize,
					"draining must not allocate a read buffer for the entire reply body")
				assert.Equal(t, watermark, client.metadataWatermark.Load())
				assert.Contains(t, logs.String(), "reply_complete=true drain_error=<nil> reply_header_status=0")
				response, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
				require.NoError(t, err)
				assert.Equal(t, []byte("next"), response)
				assert.Equal(t, identity, client.session.ClientID())
			})
		})
	}
}

func TestLoginUser_KeepsABufferedSignInReplyAfterCancellation(t *testing.T) {
	client, serverConn := newUnboundPipeClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	transport := &observedReadConn{Conn: client.conn, cancel: cancel}
	client.conn = transport
	client.reader = bufio.NewReaderSize(transport, connectionReadBufferSize)
	serve(serverConn, func(_ int, _ request) []byte {
		return registerReplyFrame(7, 100)
	})

	identity, err := client.LoginUser(context.WithValue(ctx, skipLeaderSettlement{}, struct{}{}), "iggy", "iggy")
	require.NoError(t, err)
	assert.ErrorIs(t, ctx.Err(), context.Canceled)
	assert.Equal(t, uint32(7), identity.UserId)
	assert.True(t, client.session.Bound())
	assert.NotNil(t, client.pollSession.Load())
}

func TestExchange_DoesNotResendAfterTheCallerGaveUp(t *testing.T) {
	tests := []struct {
		name   string
		status uint32
	}{
		{name: "not committed", status: uint32(ierror.TransientNotCommittedCode)},
		{name: "not accepted", status: uint32(ierror.TransientNotAcceptedCode)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, serverConn := newPipeClient(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				answer := make(chan struct{})
				server := serve(serverConn, func(index int, _ request) []byte {
					if index == 0 {
						cancel()
						<-answer
						return statusReplyFrame(vsr.OperationCreateStream, test.status, nil)
					}
					return replyFrame(vsr.OperationCreateStream, resultSection())
				})

				results := make(chan error, 2)
				go func() {
					_, err := client.do(ctx, &command.CreateStream{Name: "orders"})
					results <- err
					_, err = client.do(context.Background(), &command.CreateStream{Name: "payments"})
					results <- err
				}()
				require.ErrorIs(t, <-results, context.Canceled)
				synctest.Wait()
				close(answer)
				require.NoError(t, <-results)

				recorded := server.recorded()
				require.Len(t, recorded, 2, "a request whose caller gave up was sent again")
				assert.Equal(t, recorded[0].clientID(), recorded[1].clientID())
				assert.Equal(t, recorded[0].sessionID(), recorded[1].sessionID())
				assert.Less(t, recorded[0].requestID(), recorded[1].requestID())
				assert.Equal(t, iggcon.TransportStateConnected, client.transportState)
			})
		})
	}
}

func TestExchange_FinishesTheWriteBeforeReturningCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, serverConn := newPipeClient(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		bp := acquireRequestBuf()
		frame, err := appendCommandFrame(*bp, &command.CreateStream{Name: "orders"})
		require.NoError(t, err)
		*bp = frame
		orders := slices.Clone(frame[vsr.HeaderSize:])

		gaveUp := make(chan error, 1)
		go func() {
			_, _, err := client.exchange(ctx, uint32(command.CreateStreamCode), bp)
			gaveUp <- err
		}()
		// A pipe write blocks until the other end reads, and nothing reads yet.
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case err := <-gaveUp:
			t.Fatalf("cancellation interrupted the write: %v", err)
		default:
		}
		release := make(chan struct{})
		server := serve(serverConn, func(index int, _ request) []byte {
			if index == 0 {
				<-release
			}
			return replyFrame(vsr.OperationCreateStream, resultSection())
		})
		require.ErrorIs(t, <-gaveUp, context.Canceled)
		assert.NotNil(t, *bp, "the completed write no longer needs the pooled frame")
		releaseRequestBuf(bp)

		payments := make(chan error, 1)
		go func() {
			_, err := client.do(context.Background(), &command.CreateStream{Name: "payments"})
			payments <- err
		}()
		synctest.Wait()
		close(release)
		require.NoError(t, <-payments)

		recorded := server.recorded()
		require.Len(t, recorded, 2)
		assert.Equal(t, orders, recorded[0].payload, "the pool handed out a frame in flight")
	})
}

func TestExchange_CancelledWriteCannotOutliveTheRequestBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, _ := newPipeClient(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()
		started := time.Now()
		_, err := client.do(ctx, &command.CreateStream{Name: "orders"})
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, responseReadTimeout, time.Since(started))
		assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
	})
}

func BenchmarkSendMessagesSmallBatch(b *testing.B) {
	for _, cancellable := range []bool{false, true} {
		name := "background"
		if cancellable {
			name = "cancellable"
		}
		b.Run(name, func(b *testing.B) {
			serverConn, clientConn := net.Pipe()
			client := newTestClient(b, clientConn)
			client.session.BeginRegister()
			require.NoError(b, client.session.Bind(100))
			client.sessionState = iggcon.SessionStateAuthenticated
			done := make(chan struct{})
			b.Cleanup(func() {
				_ = clientConn.Close()
				_ = serverConn.Close()
				<-done
			})
			go func() {
				defer close(done)
				answer := replyFrame(vsr.OperationSendMessages, zeroConfirmations())
				for {
					read, err := readRequest(serverConn)
					if err != nil {
						return
					}
					echoReplyRequest(answer, read)
					if _, err := serverConn.Write(answer); err != nil {
						return
					}
				}
			}()
			ctx := context.Background()
			if cancellable {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
			}
			identifier, err := iggcon.NewIdentifier(uint32(1))
			require.NoError(b, err)
			message, err := iggcon.NewIggyMessage([]byte("payload"))
			require.NoError(b, err)
			messages := []iggcon.IggyMessage{message}
			b.ReportAllocs()
			for b.Loop() {
				_, err := client.SendMessages(ctx, identifier, identifier, iggcon.PartitionId(0), messages)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestClose_DoesNotWaitForAnExchangeItsCallerGaveUpOn(t *testing.T) {
	client, serverConn := newPipeClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serve(serverConn, func(_ int, _ request) []byte {
		cancel()
		return nil
	})

	_, err := client.SendBinaryRequest(ctx, uint32(command.PingCode), nil)
	require.ErrorIs(t, err, context.Canceled)

	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close waited out the request budget of an exchange nobody waits for")
	}
}

func TestExchange_DropsTheConnectionWhenTheAbandonedReplyNeverArrives(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	abandoned := make(chan struct{})
	serve(serverConn, func(_ int, _ request) []byte {
		cancel()
		<-abandoned
		_ = serverConn.Close()
		return nil
	})

	_, err := client.SendBinaryRequest(ctx, uint32(command.PingCode), nil)
	require.ErrorIs(t, err, context.Canceled)
	close(abandoned)

	// The abandoned exchange holds the gate until its drain ends.
	require.NoError(t, client.acquireExchange(context.Background()))
	defer client.releaseExchange()
	client.mtx.Lock()
	defer client.mtx.Unlock()
	assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState,
		"the stream is at an unknown boundary, so the connection is dropped")
	assert.False(t, client.session.Bound())
}

func TestExchange_CancelledInlineExchangesDropTheConnection(t *testing.T) {
	tests := []struct {
		name    string
		client  func(*testing.T) (*IggyTcpClient, net.Conn)
		request func(context.Context, *IggyTcpClient) error
	}{
		{
			name:   "unbound session",
			client: newUnboundPipeClient,
			request: func(ctx context.Context, client *IggyTcpClient) error {
				_, err := client.SendBinaryRequest(ctx, uint32(command.PingCode), nil)
				return err
			},
		},
		{
			name: "data connection",
			client: func(t *testing.T) (*IggyTcpClient, net.Conn) {
				client, serverConn := newPipeClient(t)
				client.dataConnection = true
				return client, serverConn
			},
			request: func(ctx context.Context, client *IggyTcpClient) error {
				_, err := client.SendBinaryRequest(ctx, uint32(command.PingCode), nil)
				return err
			},
		},
		{
			name:   "logout",
			client: newPipeClient,
			request: func(ctx context.Context, client *IggyTcpClient) error {
				return client.LogoutUser(ctx)
			},
		},
		{
			name:   "sign-in roster read",
			client: newUnboundPipeClient,
			request: func(ctx context.Context, client *IggyTcpClient) error {
				_, err := client.LoginUser(ctx, "iggy", "iggy")
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, serverConn := test.client(t)
			client.config.reconnection.enabled = false
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			serve(serverConn, func(_ int, read request) []byte {
				if read.operation() == vsr.OperationRegister {
					return registerReplyFrame(7, 128)
				}
				cancel()
				return nil
			})

			assert.ErrorIs(t, test.request(ctx, client), context.Canceled)
			assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
		})
	}
}

// Real time, not synctest: a regression parks these calls on a mutex, which
// synctest does not count as blocked, and the test would hang instead of fail.
func TestExchange_AnAbandonedExchangeHoldsOnlyTheConnection(t *testing.T) {
	client, serverConn := newPipeClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	defer close(release)
	serve(serverConn, func(_ int, _ request) []byte {
		cancel()
		<-release
		return nil
	})
	_, err := client.SendBinaryRequest(ctx, uint32(command.PingCode), nil)
	require.ErrorIs(t, err, context.Canceled)

	const wait = 100 * time.Millisecond
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		assert.NotNil(t, client.GetConnectionInfo())
		assert.NoError(t, client.Connect(context.Background()), "the client is still connected")

		redirectCtx, cancelRedirect := context.WithTimeout(context.Background(), wait)
		defer cancelRedirect()
		_, err := client.HandleLeaderRedirection(redirectCtx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		loginCtx, cancelLogin := context.WithTimeout(context.Background(), wait)
		defer cancelLogin()
		_, err = client.LoginUser(loginCtx, "iggy", "iggy")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("a call waited for an exchange whose caller gave up")
	}
}

func TestClose_FailsAnInFlightExchangeAtOnce(t *testing.T) {
	client, serverConn := newPipeClient(t)
	entered := make(chan struct{})
	serve(serverConn, func(_ int, _ request) []byte {
		close(entered)
		return nil
	})

	returned := make(chan error, 1)
	go func() {
		_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
		returned <- err
	}()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close waited for the exchange in flight")
	}
	select {
	case err := <-returned:
		assert.ErrorIs(t, err, ierror.ErrClientShutdown)
	case <-time.After(time.Second):
		t.Fatal("the request waited out its budget after Close")
	}
}

func TestDisconnect_WaitsForTheExchangeOnTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, serverConn := newPipeClient(t)
		generation := client.connGeneration
		entered := make(chan struct{})
		release := make(chan struct{})
		serve(serverConn, func(_ int, _ request) []byte {
			close(entered)
			<-release
			return replyFrame(vsr.OperationNonReplicated, nil)
		})

		returned := make(chan error, 1)
		go func() {
			_, err := client.SendBinaryRequest(context.Background(), uint32(command.PingCode), nil)
			returned <- err
		}()
		<-entered

		shortCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		torn, err := client.disconnectGeneration(shortCtx, generation)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.False(t, torn)

		close(release)
		require.NoError(t, <-returned, "the teardown cut the exchange it waits for")
		torn, err = client.disconnectGeneration(context.Background(), generation)
		require.NoError(t, err)
		assert.True(t, torn)
		assert.Equal(t, iggcon.TransportStateDisconnected, client.transportState)
	})
}

func TestSendMessages_DecodesTheConfirmations(t *testing.T) {
	client, serverConn := newPipeClient(t)
	confirmations := binary.LittleEndian.AppendUint32(nil, 1)
	confirmations = binary.LittleEndian.AppendUint32(confirmations, 3)
	confirmations = binary.LittleEndian.AppendUint32(confirmations, 2)
	confirmations = binary.LittleEndian.AppendUint32(confirmations, 1)
	confirmations = binary.LittleEndian.AppendUint64(confirmations, 42)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationSendMessages, confirmations)
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	response, err := client.SendMessages(context.Background(),
		numericIdentifier(t, 3), numericIdentifier(t, 2),
		iggcon.PartitionId(1), []iggcon.IggyMessage{message})
	require.NoError(t, err)

	assert.Equal(t, []iggcon.SendMessagesConfirmation{
		{StreamId: 3, TopicId: 2, PartitionId: 1, BaseOffset: 42},
	}, response.Confirmations)

	recorded := server.recorded()
	require.Len(t, recorded, 1)
	assert.Equal(t, vsr.OperationSendMessages, recorded[0].operation())
	assert.Equal(t, uint32(1), recorded[0].partitionID(t))
	assert.Equal(t, uint64(1), recorded[0].requestID(),
		"a partition request reads the watermark without consuming it")
}

func TestSendMessages_RejectsAnEmptyReplyBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	client.config.reconnection.enabled = false
	serve(serverConn, func(_ int, _ request) []byte {
		// The server answers a replicated request on a dead session with an
		// empty status-0 reply. Nothing was written, so reading it as a
		// successful send would silently drop the batch.
		return replyFrame(vsr.OperationSendMessages, nil)
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	_, err = client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1),
		iggcon.PartitionId(0), []iggcon.IggyMessage{message})
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
}

func TestSendMessages_AcceptsAZeroCountConfirmationBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationSendMessages, zeroConfirmations())
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	response, err := client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1),
		iggcon.PartitionId(0), []iggcon.IggyMessage{message})
	require.NoError(t, err)
	assert.Empty(t, response.Confirmations)
}

// zeroConfirmations builds a confirmation section with a zero entry count.
func zeroConfirmations() []byte {
	return binary.LittleEndian.AppendUint32(nil, 0)
}

func TestSendMessages_DegradesAnUnreadableConfirmationBody(t *testing.T) {
	client, serverConn := newPipeClient(t)
	serve(serverConn, func(_ int, _ request) []byte {
		// The batch already committed, so a decode failure must not surface as
		// an error a caller would retry into a duplicate write.
		return replyFrame(vsr.OperationSendMessages, []byte{1, 0, 0, 0, 0xFF})
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	response, err := client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1),
		iggcon.PartitionId(0), []iggcon.IggyMessage{message})
	require.NoError(t, err)
	assert.Empty(t, response.Confirmations)
}

func TestSendMessages_ResolvesKeyPartitioningToAnExplicitPartition(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.operation() == vsr.OperationSendMessages {
			return replyFrame(vsr.OperationSendMessages, zeroConfirmations())
		}
		return replyFrame(vsr.OperationNonReplicated, topicDetailsBody(t, 4))
	})

	key, err := iggcon.EntityIdString("order-key-1")
	require.NoError(t, err)
	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)

	_, err = client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1), key, []iggcon.IggyMessage{message})
	require.NoError(t, err)

	recorded := server.recorded()
	require.Len(t, recorded, 2)
	assert.Equal(t, uint32(command.GetTopicCode), recorded[0].code())
	// 0x0D3FE0E1 modulo four partitions.
	assert.Equal(t, uint32(1), recorded[1].partitionID(t))
}

func TestSendMessages_RoundRobinsBalancedPartitioning(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, read request) []byte {
		if read.operation() == vsr.OperationSendMessages {
			return replyFrame(vsr.OperationSendMessages, zeroConfirmations())
		}
		return replyFrame(vsr.OperationNonReplicated, topicDetailsBody(t, 3))
	})

	message, err := iggcon.NewIggyMessage([]byte("payload"))
	require.NoError(t, err)
	for range 4 {
		_, err = client.SendMessages(context.Background(),
			numericIdentifier(t, 1), numericIdentifier(t, 1),
			iggcon.None(), []iggcon.IggyMessage{message})
		require.NoError(t, err)
	}

	var partitions []uint32
	for _, recorded := range server.recorded() {
		if recorded.operation() == vsr.OperationSendMessages {
			partitions = append(partitions, recorded.partitionID(t))
		}
	}
	assert.Equal(t, []uint32{0, 1, 2, 0}, partitions)

	metadataRequests := 0
	for _, recorded := range server.recorded() {
		if recorded.code() == uint32(command.GetTopicCode) {
			metadataRequests++
		}
	}
	assert.Equal(t, 1, metadataRequests, "the partition count is cached after the first read")
}

func TestSendMessages_RejectsAnEmptyBatch(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationSendMessages, nil)
	})

	_, err := client.SendMessages(context.Background(),
		numericIdentifier(t, 1), numericIdentifier(t, 1), iggcon.PartitionId(0), nil)
	assert.ErrorIs(t, err, ierror.ErrInvalidMessagesCount)
	assert.Empty(t, server.recorded())
}

func TestCanReplay_RefusesOnlyReplicatedRequestsWithAnUnknownOutcome(t *testing.T) {
	tests := []struct {
		name string
		code uint32
		err  error
		want bool
	}{
		{name: "register replays by design",
			code: uint32(command.LoginRegisterCode), err: ierror.ErrDisconnected, want: true},
		{name: "non-replicated read replays",
			code: uint32(command.PingCode), err: ierror.ErrDisconnected, want: true},
		{name: "replicated write never sent replays",
			code: uint32(command.CreateStreamCode), err: ierror.ErrNotConnected, want: true},
		{name: "replicated write refused by the server replays",
			code: uint32(command.CreateStreamCode), err: ierror.ErrUnauthenticated, want: true},
		{name: "replicated write on a stale client replays",
			code: uint32(command.CreateStreamCode), err: ierror.ErrStaleClient, want: true},
		{name: "replicated write with a lost reply is refused",
			code: uint32(command.CreateStreamCode), err: ierror.ErrDisconnected, want: false},
		{name: "send with a lost reply is refused",
			code: uint32(command.SendMessagesCode), err: ierror.ErrDisconnected, want: false},
		{name: "token mint with a lost reply is refused",
			code: uint32(command.CreateAccessTokenCode), err: ierror.ErrDisconnected, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, canReplay(test.code, nil, test.err))
		})
	}
}

func TestCanReplay_ReadsThePollFlagBeforeTrailingBytes(t *testing.T) {
	stream, err := iggcon.NewIdentifier("orders")
	require.NoError(t, err)
	for _, autoCommit := range []bool{false, true} {
		poll := command.PollMessages{
			StreamId: stream, TopicId: numericIdentifier(t, 2), Consumer: iggcon.DefaultConsumer(),
			Strategy: iggcon.NextPollingStrategy(), Count: 10, AutoCommit: autoCommit,
		}
		payload, err := poll.MarshalBinary()
		require.NoError(t, err)
		code := uint32(command.PollMessagesCode)
		assert.Equal(t, !autoCommit, canReplay(code, payload, ierror.ErrDisconnected))
		for _, err := range []error{ierror.ErrNotConnected, ierror.ErrCannotEstablishConnection, ierror.ErrUnauthenticated, ierror.ErrStaleClient} {
			assert.True(t, canReplay(code, payload, err), "a poll that was never applied can reconnect: %v", err)
		}
		for size := range len(payload) {
			assert.False(t, canReplay(code, payload[:size], ierror.ErrDisconnected),
				"truncated poll of length %d must not replay", size)
		}
		trailing := byte(0)
		if !autoCommit {
			trailing = 1
		}
		assert.Equal(t, !autoCommit, canReplay(code, append(payload, trailing), ierror.ErrDisconnected))
	}
}

// topicDetailsBody builds the reply body of GetTopic for a topic with the
// given partition count and no partitions listed. The trailing 8 zero bytes
// are the u32 length prefixes of the empty explicit and derived options
// blocks.
func topicDetailsBody(t *testing.T, partitionsCount uint32) []byte {
	t.Helper()

	const nameLenOffset = 49
	name := "orders"
	body := make([]byte, nameLenOffset+1+len(name)+4+4)
	binary.LittleEndian.PutUint32(body[0:4], 1)
	binary.LittleEndian.PutUint32(body[12:16], partitionsCount)
	body[nameLenOffset] = byte(len(name))
	copy(body[nameLenOffset+1:], name)
	return body
}
