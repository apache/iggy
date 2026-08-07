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
	"crypto/tls"
	"crypto/x509"
	"encoding"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/avast/retry-go/v5"
)

type Option func(config *Options)

type Options struct {
	config config
}

func GetDefaultOptions() Options {
	return Options{
		config: defaultTcpClientConfig(),
	}
}

type IggyTcpClient struct {
	conn                   net.Conn
	mtx                    sync.Mutex
	config                 config
	logger                 *slog.Logger
	MessageCompression     iggcon.IggyMessageCompression
	leaderRedirectionState iggcon.LeaderRedirectionState
	clientAddress          string
	currentServerAddress   string
	knownServerAddresses   []string
	connectedAt            time.Time
	transportState         iggcon.TransportState
	sessionState           iggcon.SessionState
	// session carries the consensus client identity and request watermark;
	// guarded by c.mtx.
	session *vsr.Session
	// skipAutoLoginOnce suppresses the next automatic sign-in so a replayed
	// login is not preempted by one the reconnect issues; guarded by c.mtx.
	skipAutoLoginOnce bool
	// groups caches the consumer-group assignments this client polls with.
	groups groupAssignmentCache
	// topics caches what a send needs to resolve a partition locally.
	topics topicCache
	// respHeader is the reused reply-header read buffer; guarded by c.mtx.
	respHeader [vsr.HeaderSize]byte
}

type config struct {
	// serverAddress is the address of the Iggy server
	serverAddress string
	// tlsEnabled indicates whether to use TLS when connecting to the server
	tlsEnabled bool
	tls        tlsConfig
	// autoLogin indicates whether to automatically login user after establishing connection.
	autoLogin AutoLogin
	// reconnection indicates whether to automatically reconnect when disconnected
	reconnection tcpClientReconnectionConfig
	// noDelay disable Nagle's algorithm for the TCP connection
	noDelay bool
}

func defaultTcpClientConfig() config {
	return config{
		serverAddress: "127.0.0.1:8090",
		tlsEnabled:    false,
		tls:           defaultTLSConfig(),
		autoLogin:     AutoLogin{},
		reconnection:  defaultTcpClientReconnectionConfig(),
		noDelay:       false,
	}
}

type tcpClientReconnectionConfig struct {
	enabled          bool
	maxRetries       uint32
	interval         time.Duration
	reestablishAfter time.Duration
}

func defaultTcpClientReconnectionConfig() tcpClientReconnectionConfig {
	return tcpClientReconnectionConfig{
		enabled:          true,
		maxRetries:       0, //infinity retry
		interval:         2 * time.Second,
		reestablishAfter: 0,
	}
}

type tlsConfig struct {
	// tlsDomain is the domain to use for TLS when connecting to the server
	// If empty, automatically extracts the hostname/IP from serverAddress
	tlsDomain string
	// tlsCAFile is the path to the CA file to use for TLS
	tlsCAFile string
	// tlsValidateCertificate indicates whether to validate the server's TLS certificate
	tlsValidateCertificate bool
}

func defaultTLSConfig() tlsConfig {
	return tlsConfig{
		tlsDomain:              "",
		tlsCAFile:              "",
		tlsValidateCertificate: true,
	}
}

type AutoLogin struct {
	enabled     bool
	credentials Credentials
}

func NewAutoLogin(credentials Credentials) AutoLogin {
	return AutoLogin{
		enabled:     true,
		credentials: credentials,
	}
}

type Credentials struct {
	username            string
	password            string
	personalAccessToken string
}

func NewUsernamePasswordCredentials(username, password string) Credentials {
	return Credentials{
		username: username,
		password: password,
	}
}

func NewPersonalAccessTokenCredentials(token string) Credentials {
	return Credentials{
		personalAccessToken: token,
	}
}

// WithServerAddress Sets the server address for the TCP client.
func WithServerAddress(address string) Option {
	return func(opts *Options) {
		opts.config.serverAddress = address
	}
}

// WithAutoLogin signs the client in with the given credentials on every
// connection, including the ones a reconnect establishes. Without it a
// reconnect cannot restore the session, so a request that hits a dropped
// connection fails instead of replaying.
func WithAutoLogin(credentials Credentials) Option {
	return func(opts *Options) {
		opts.config.autoLogin = NewAutoLogin(credentials)
	}
}

// TLSOption is a functional option for configuring TLS settings.
type TLSOption func(cfg *tlsConfig)

// WithTLS enables TLS for the TCP client and applies the given TLS options.
func WithTLS(tlsOpts ...TLSOption) Option {
	return func(opts *Options) {
		opts.config.tlsEnabled = true
		for _, tlsOpt := range tlsOpts {
			if tlsOpt != nil {
				tlsOpt(&opts.config.tls)
			}
		}
	}
}

// WithTLSDomain sets the TLS domain for server name indication (SNI).
// If not provided, the domain will be automatically extracted from the server address.
func WithTLSDomain(domain string) TLSOption {
	return func(cfg *tlsConfig) {
		cfg.tlsDomain = domain
	}
}

// WithTLSCAFile sets the path to the CA certificate file for TLS verification.
func WithTLSCAFile(path string) TLSOption {
	return func(cfg *tlsConfig) {
		cfg.tlsCAFile = path
	}
}

// WithTLSValidateCertificate enables or disables TLS certificate validation.
func WithTLSValidateCertificate(validate bool) TLSOption {
	return func(cfg *tlsConfig) {
		cfg.tlsValidateCertificate = validate
	}
}

// NewIggyTcpClient creates a new Iggy TCP client with the given options.
// warning: don't use this function directly, use iggycli.NewIggyClient with iggycli.WithTcp instead.
func NewIggyTcpClient(logger *slog.Logger, options ...Option) *IggyTcpClient {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	opts := GetDefaultOptions()
	for _, opt := range options {
		if opt != nil {
			opt(&opts)
		}
	}

	return &IggyTcpClient{
		config:                 opts.config,
		logger:                 logger,
		clientAddress:          "",
		conn:                   nil,
		transportState:         iggcon.TransportStateDisconnected,
		sessionState:           iggcon.SessionStateUnauthenticated,
		connectedAt:            time.Time{},
		leaderRedirectionState: iggcon.LeaderRedirectionState{},
		currentServerAddress:   opts.config.serverAddress,
		session:                vsr.NewSession(),
	}
}

const (
	MaxStringLength   = 255
	MaxPartitionCount = 1000
)

// Timings of the lockstep consensus exchange, matching the Rust SDK.
const (
	// responseReadTimeout bounds one request across every replay and failover.
	// It is far beyond any healthy round trip and only trips when the server
	// loses the reply, which would otherwise hold the connection forever.
	responseReadTimeout = 30 * time.Second
	// replayInterval paces the resend of a transiently rejected request.
	replayInterval = 50 * time.Millisecond
	// failoverCheckInterval is how long a request that was never admitted
	// replays on the same connection before the client re-checks leadership.
	// A node that stopped being primary answers transient forever, so
	// replaying alone never recovers.
	failoverCheckInterval = 2 * time.Second
)

// requestPrologue is the zeroed space a frame reserves ahead of its payload.
// The header is stamped into it once the payload length is known.
var requestPrologue [vsr.HeaderSize]byte

// requestBufPool reuses wire-payload buffers across RPCs.
var requestBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 256)
		return &b
	},
}

func acquireRequestBuf() *[]byte {
	return requestBufPool.Get().(*[]byte)
}

func releaseRequestBuf(bp *[]byte) {
	const maxPooled = 64 * 1024
	if cap(*bp) > maxPooled {
		return
	}
	*bp = (*bp)[:0]
	requestBufPool.Put(bp)
}

func (c *IggyTcpClient) read(expectedSize int) (int, []byte, error) {
	buffer := make([]byte, expectedSize)
	n, err := c.readInto(buffer)
	if err != nil {
		return n, buffer[:n], err
	}
	return n, buffer, nil
}

// readInto reads exactly len(buf) bytes from the connection into buf.
func (c *IggyTcpClient) readInto(buf []byte) (int, error) {
	var totalRead int
	expected := len(buf)
	for totalRead < expected {
		n, err := c.conn.Read(buf[totalRead:])
		if err != nil {
			return totalRead, err
		}
		if n == 0 {
			return totalRead, io.ErrNoProgress
		}
		totalRead += n
	}
	return totalRead, nil
}

func (c *IggyTcpClient) write(payload []byte) (int, error) {
	var totalWritten int
	for totalWritten < len(payload) {
		n, err := c.conn.Write(payload[totalWritten:])
		if err != nil {
			return totalWritten, err
		}
		if n == 0 {
			return totalWritten, io.ErrNoProgress
		}
		totalWritten += n
	}

	return totalWritten, nil
}

// do sends the command and returns the response body. Commands implementing
// the appender interface encode directly into a pooled buffer.
func (c *IggyTcpClient) do(ctx context.Context, cmd command.Command) ([]byte, error) {
	bp := acquireRequestBuf()
	defer releaseRequestBuf(bp)

	frame, err := appendCommandFrame(*bp, cmd)
	if err != nil {
		return nil, err
	}
	*bp = frame

	return c.exchange(ctx, uint32(cmd.Code()), frame)
}

// SendBinaryRequest sends a command code and payload and returns the raw response body.
// Session-control codes return ierror.ErrInvalidCommand without writing to the connection.
func (c *IggyTcpClient) SendBinaryRequest(ctx context.Context, code uint32, payload []byte) ([]byte, error) {
	if isSessionControlCode(code) {
		return nil, ierror.ErrInvalidCommand
	}

	bp := acquireRequestBuf()
	defer releaseRequestBuf(bp)

	frame := append(append((*bp)[:0], requestPrologue[:]...), payload...)
	*bp = frame

	return c.exchange(ctx, code, frame)
}

func isSessionControlCode(code uint32) bool {
	switch code {
	case uint32(command.LoginUserCode),
		uint32(command.LogoutUserCode),
		uint32(command.LoginRegisterCode),
		uint32(command.LoginWithAccessTokenCode),
		uint32(command.LoginRegisterWithPATCode):
		return true
	default:
		return false
	}
}

// isRegisterCode reports whether the code carries the sign-in handshake.
func isRegisterCode(code uint32) bool {
	return code == uint32(command.LoginRegisterCode) ||
		code == uint32(command.LoginRegisterWithPATCode)
}

// appendCommandFrame reserves the request prologue in buf and appends the
// encoded command payload after it, so the frame is a single allocation the
// header can be stamped into once the payload length is known.
func appendCommandFrame(buf []byte, cmd command.Command) ([]byte, error) {
	buf = append(buf[:0], requestPrologue[:]...)
	if appender, ok := cmd.(encoding.BinaryAppender); ok {
		return appender.AppendBinary(buf)
	}
	body, err := cmd.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append(buf, body...), nil
}

// connectScoped marks the context of a request the connect flow itself
// issues. exchange must not reconnect such a request: Connect is already on
// the stack, and re-entering it has no depth bound.
type connectScoped struct{}

// exchange runs one request to completion, reconnecting and replaying it when
// the failure is one a fresh connection recovers from.
func (c *IggyTcpClient) exchange(ctx context.Context, code uint32, frame []byte) ([]byte, error) {
	response, err := c.sendFrame(ctx, code, frame)
	if err == nil || !isReconnectable(err) {
		return response, err
	}
	if ctx.Value(connectScoped{}) != nil {
		return nil, err
	}
	if !c.config.reconnection.enabled {
		c.logger.Warn("Automatic reconnection is disabled.")
		return nil, err
	}

	// Without auto-login a reconnect cannot restore the session, so anything
	// but a sign-in fails here instead of replaying unauthenticated. The
	// sign-in itself is the exception: the server stays silent on a transient
	// register failure and expects the client to replay it.
	login := isRegisterCode(code)
	if !c.config.autoLogin.enabled && !login {
		return nil, err
	}
	if !canReplay(code, err) {
		c.logger.Warn("Not replaying a replicated request with an unknown outcome.",
			slog.Int("code", int(code)), slog.Any("error", err))
		return nil, err
	}

	if disconnectErr := c.disconnect(); disconnectErr != nil {
		return nil, disconnectErr
	}
	if login {
		c.mtx.Lock()
		c.skipAutoLoginOnce = true
		c.mtx.Unlock()
	}

	c.logger.Info("Reconnecting to the server...",
		slog.String("server_address", c.currentServerAddress),
		slog.Any("error", err))

	if reconnectErr := c.Connect(ctx); reconnectErr != nil {
		if login {
			c.mtx.Lock()
			c.skipAutoLoginOnce = false
			c.mtx.Unlock()
		}
		return nil, reconnectErr
	}
	return c.sendFrame(ctx, code, frame)
}

// canReplay reports whether re-issuing the request over a fresh connection
// cannot double-apply it. A reconnect registers a new client identity, so the
// server cannot deduplicate the replay against the original request. A
// sign-in is a deliberate exception the server expects, a non-replicated
// request is never deduplicated in the first place, and two failures leave a
// replicated request provably unapplied: one that struck before the frame was
// written, and a session refusal the server answered instead of applying.
// What remains is a replicated request whose reply was lost in transit, and
// its unknown outcome makes the replay unsafe.
func canReplay(code uint32, err error) bool {
	if isRegisterCode(code) {
		return true
	}
	if _, replicated := vsr.ReplicatedOperation(code); !replicated {
		return true
	}
	neverApplied := []error{
		ierror.ErrNotConnected,
		ierror.ErrCannotEstablishConnection,
		ierror.ErrUnauthenticated,
		ierror.ErrStaleClient,
	}
	for _, target := range neverApplied {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// isReconnectable reports whether a fresh connection can recover the failure.
func isReconnectable(err error) bool {
	reconnectable := []error{
		ierror.ErrDisconnected,
		ierror.ErrEmptyResponse,
		ierror.ErrUnauthenticated,
		ierror.ErrStaleClient,
		ierror.ErrNotConnected,
		ierror.ErrCannotEstablishConnection,
		ierror.ErrTcpError,
	}
	for _, target := range reconnectable {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}

// sendFrame runs the request against the current connection. One deadline
// bounds it across every same-connection replay and every leader failover.
func (c *IggyTcpClient) sendFrame(ctx context.Context, code uint32, frame []byte) ([]byte, error) {
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(responseReadTimeout)
	stamped := false
	for {
		// A sign-in owns the whole budget on this connection. The connect flow
		// already pointed it at the leader, and failing over from under the
		// handshake would recurse back into it.
		transientDeadline := deadline
		if !isRegisterCode(code) {
			if failover := time.Now().Add(failoverCheckInterval); failover.Before(deadline) {
				transientDeadline = failover
			}
		}

		response, attemptStamped, err := c.attempt(
			ctx, code, frame, stamped, transientDeadline, deadline)
		stamped = attemptStamped

		switch {
		case err == nil:
			return response, nil
		case errors.Is(err, ierror.ErrTransientNotAccepted) &&
			!isRegisterCode(code) && time.Now().Before(deadline):
			// The server never admitted the request, so re-issuing it cannot
			// double-apply. A same-connection replay keeps the stamped request
			// id; a redirect registers again, so the frame is stamped afresh.
			redirect, redirectErr := c.HandleLeaderRedirection(ctx)
			if redirectErr != nil {
				return nil, redirectErr
			}
			if redirect {
				if connectErr := c.Connect(ctx); connectErr != nil {
					return nil, connectErr
				}
				stamped = false
			}
		default:
			return nil, err
		}
	}
}

// attempt stamps the frame if it is not stamped yet and exchanges it once,
// replaying in place while the server answers transiently.
func (c *IggyTcpClient) attempt(
	ctx context.Context,
	code uint32,
	frame []byte,
	stamped bool,
	transientDeadline, readDeadline time.Time,
) ([]byte, bool, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	switch c.transportState {
	case iggcon.TransportStateShutdown:
		c.logger.Debug("Cannot send data. Client is shutdown.")
		return nil, stamped, ierror.ErrClientShutdown
	case iggcon.TransportStateDisconnected:
		c.logger.Debug("Cannot send data. Client is not connected.")
		return nil, stamped, ierror.ErrNotConnected
	case iggcon.TransportStateConnecting:
		c.logger.Debug("Cannot send data. Client is still connecting.")
		return nil, stamped, ierror.ErrNotConnected
	}
	if c.conn == nil {
		return nil, stamped, ierror.ErrNotConnected
	}

	if !stamped {
		// Stamp once per session: the header consumes a request id, and a
		// replay must carry the same one for the server to deduplicate it.
		if err := vsr.StampRequestHeader(c.session, code, frame); err != nil {
			return nil, false, err
		}
		stamped = true
	}

	conn := c.conn
	var deadlineMu sync.Mutex
	cleared := false
	if ctx.Done() != nil {
		stop := context.AfterFunc(ctx, func() {
			deadlineMu.Lock()
			defer deadlineMu.Unlock()
			if !cleared {
				// A deadline in the past unblocks any read or write in
				// progress. This uses the snapshotted conn, not c.conn, so a
				// reconnect cannot receive the deadline of a cancelled call.
				_ = conn.SetDeadline(time.Now())
			}
		})
		defer stop()
	}

	deadlineMu.Lock()
	_ = conn.SetDeadline(readDeadline)
	deadlineMu.Unlock()

	response, err := c.exchangeLocked(ctx, code, frame, transientDeadline, readDeadline)

	deadlineMu.Lock()
	cleared = true
	_ = conn.SetDeadline(time.Time{})
	deadlineMu.Unlock()

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, stamped, ctxErr
		}
	}
	return response, stamped, err
}

// exchangeLocked writes the frame and reads its reply, resending the identical
// bytes while the server answers with a transient rejection.
func (c *IggyTcpClient) exchangeLocked(
	ctx context.Context,
	code uint32,
	frame []byte,
	transientDeadline, readDeadline time.Time,
) ([]byte, error) {
	for {
		c.logger.Debug("Sending a TCP request",
			slog.Int("frame_length", len(frame)), slog.Int("code", int(code)))
		if _, err := c.write(frame); err != nil {
			c.invalidateConnLocked()
			return nil, err
		}

		body, err := c.readReplyLocked(code)
		if err != nil {
			return nil, err
		}

		response, err := vsr.DecodeReply(&c.respHeader, body)
		switch {
		case errors.Is(err, ierror.ErrTransientNotCommitted) && time.Now().Before(readDeadline):
			// The outcome is unknown, so only a replay of the same request id
			// on this session is safe. The server's client table answers from
			// its reply cache if the request did commit.
			if waitErr := waitBeforeReplay(ctx, readDeadline); waitErr != nil {
				return nil, waitErr
			}
		case errors.Is(err, ierror.ErrTransientNotAccepted) && time.Now().Before(transientDeadline):
			if waitErr := waitBeforeReplay(ctx, transientDeadline); waitErr != nil {
				return nil, waitErr
			}
		default:
			if err != nil {
				c.handleReplyFailureLocked(err)
				return nil, err
			}
			return response, nil
		}
	}
}

// readReplyLocked reads the fixed header and then exactly the body bytes it
// declares. A read that cannot complete leaves the stream at an unknown frame
// boundary, so the connection is dropped rather than reused.
func (c *IggyTcpClient) readReplyLocked(code uint32) ([]byte, error) {
	if _, err := c.readInto(c.respHeader[:]); err != nil {
		c.logger.Error("Failed to read the reply header",
			slog.Int("code", int(code)), slog.Any("error", err))
		c.invalidateConnLocked()
		return nil, ierror.ErrDisconnected
	}

	size := vsr.ReadSize(&c.respHeader)
	if size < vsr.HeaderSize || size > vsr.MaxFrameSize {
		c.logger.Error("The reply declares an invalid frame size",
			slog.Int("code", int(code)), slog.Int("size", int(size)))
		c.invalidateConnLocked()
		return nil, ierror.ErrInvalidCommand
	}

	bodyLength := int(size) - vsr.HeaderSize
	if bodyLength == 0 {
		return nil, nil
	}

	_, body, err := c.read(bodyLength)
	if err != nil {
		c.logger.Error("Failed to read the reply body",
			slog.Int("code", int(code)), slog.Any("error", err))
		c.invalidateConnLocked()
		return nil, ierror.ErrDisconnected
	}
	return body, nil
}

// handleReplyFailureLocked reacts to a failed reply. An eviction is
// session-terminal, so the local session is dropped and the next sign-in
// registers a fresh client identity.
func (c *IggyTcpClient) handleReplyFailureLocked(err error) {
	var eviction *vsr.EvictionError
	if !errors.As(err, &eviction) {
		return
	}
	c.logger.Warn("The server evicted the session",
		slog.Int("reason", int(eviction.Reason)),
		slog.Any("error", eviction.Unwrap()))
	c.invalidateConnLocked()
}

// waitBeforeReplay pauses before resending a transiently rejected request,
// never past the deadline and never past the caller's cancellation.
func waitBeforeReplay(ctx context.Context, deadline time.Time) error {
	interval := replayInterval
	if remaining := time.Until(deadline); remaining < interval {
		interval = remaining
	}
	if interval <= 0 {
		return nil
	}

	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// invalidateConnLocked closes the connection and marks it as disconnected
func (c *IggyTcpClient) invalidateConnLocked() {
	_ = c.closeConnLocked()
	c.transportState = iggcon.TransportStateDisconnected
	c.sessionState = iggcon.SessionStateUnauthenticated
	c.session.Reset()
	c.groups.clear()
	c.topics.clear()
}

// closeConnLocked closes and drops the current connection.
func (c *IggyTcpClient) closeConnLocked() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *IggyTcpClient) GetConnectionInfo() *iggcon.ConnectionInfo {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return &iggcon.ConnectionInfo{
		Protocol:      iggcon.Tcp,
		ServerAddress: c.currentServerAddress,
	}
}

// Connect establishes the TCP connection to the server.
func (c *IggyTcpClient) Connect(ctx context.Context) error {
	c.mtx.Lock()
	switch c.transportState {
	case iggcon.TransportStateShutdown:
		c.mtx.Unlock()
		c.logger.Debug("Cannot connect. Client is shutdown.")
		return ierror.ErrClientShutdown
	case iggcon.TransportStateConnected:
		clientAddress := c.clientAddress
		c.mtx.Unlock()
		c.logger.Debug("Client is already connected.", slog.String("client_address", clientAddress))
		return nil
	case iggcon.TransportStateConnecting:
		c.mtx.Unlock()
		c.logger.Debug("Client is already connecting.")
		return nil
	default:
		c.transportState = iggcon.TransportStateConnecting
	}
	connectedAt := c.connectedAt
	c.mtx.Unlock()

	// handle reestablish interval
	if !connectedAt.IsZero() {
		now := time.Now()
		elapsed := now.Sub(connectedAt)
		reestablishAfter := c.config.reconnection.reestablishAfter

		c.logger.Debug("Elapsed time since last connection", slog.Duration("elapsed", elapsed))
		if elapsed < reestablishAfter {
			remaining := reestablishAfter - elapsed
			c.logger.Info("Trying to connect to the server", slog.Duration("remaining", remaining))
			time.Sleep(remaining)
		}
	}
	attempts := uint(1)
	interval := time.Duration(0)
	if c.config.reconnection.enabled {
		attempts = uint(c.config.reconnection.maxRetries)
		interval = c.config.reconnection.interval
	}

	candidates := c.connectionCandidates()
	var conn net.Conn
	var candidateIndex int
	if err := retry.New(
		retry.Context(ctx),
		retry.Attempts(attempts),
		retry.Delay(interval),
		retry.DelayType(retry.FixedDelay),
		retry.OnRetry(func(n uint, err error) {
			c.logger.Info("Retrying to connect to server...", slog.Int("retry_count", int(n+1)), slog.Int("max_retries", int(attempts)), slog.Any("error", err))
		}),
	).Do(
		func() error {
			address := candidates[candidateIndex%len(candidates)]
			candidateIndex++
			c.logger.Info("Iggy client is connecting to server...", slog.String("server_address", address))
			connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
			if err != nil {
				c.logger.Error("Failed to establish TCP connection to the server", slog.Any("error", err))
				return ierror.ErrCannotEstablishConnection
			}

			tc := connection.(*net.TCPConn)
			if err := tc.SetNoDelay(c.config.noDelay); err != nil {
				c.logger.Error("Failed to set the nodelay option on the client, continuing...", slog.Any("error", err))
			}

			c.mtx.Lock()
			c.clientAddress = tc.LocalAddr().String()
			c.currentServerAddress = address
			c.mtx.Unlock()

			if !c.config.tlsEnabled {
				conn = connection
				return nil
			}

			// TLS logic
			tlsConfig, err := c.createTLSConfig()
			if err != nil {
				_ = connection.Close()
				return err
			}

			tlsConn := tls.Client(connection, tlsConfig)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				c.logger.Error("Failed to establish a TLS connection to the server", slog.Any("error", err))
				_ = connection.Close()
				return fmt.Errorf("TLS handshake failed: %w", err)
			}

			conn = tlsConn
			return nil
		}); err != nil {
		c.mtx.Lock()
		c.transportState = iggcon.TransportStateDisconnected
		c.mtx.Unlock()
		if !c.config.reconnection.enabled {
			c.logger.Warn("Automatic reconnection is disabled.")
		}
		// TODO publish event disconnected
		return err
	}

	c.mtx.Lock()
	c.conn = conn
	c.transportState = iggcon.TransportStateConnected
	c.connectedAt = time.Now()
	// The server fence does not survive the old socket, so the new connection
	// starts from a fresh client identity.
	c.session.Reset()
	skipAutoLogin := c.skipAutoLoginOnce
	c.skipAutoLoginOnce = false
	c.logger.Info("Iggy client has connected to the Iggy server", slog.String("client_address", c.clientAddress), slog.String("server_address", c.currentServerAddress))
	c.mtx.Unlock()

	if err := c.establishSession(ctx, skipAutoLogin); err != nil {
		_ = c.disconnect()
		return err
	}
	return nil
}

func (c *IggyTcpClient) connectionCandidates() []string {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	addresses := make([]string, 0, 2+len(c.knownServerAddresses))
	seen := make(map[string]struct{}, cap(addresses))
	for _, address := range append(
		[]string{c.currentServerAddress, c.config.serverAddress},
		c.knownServerAddresses...,
	) {
		if address == "" {
			continue
		}
		if _, ok := seen[address]; ok {
			continue
		}
		seen[address] = struct{}{}
		addresses = append(addresses, address)
	}
	return addresses
}

// establishSession points the connection at the leader and signs in when
// auto-login is configured.
//
// Leadership is settled before the sign-in, and it is settled even when
// auto-login is off: register is a consensus operation that a backup answers
// transiently, so signing in against a follower replays for the whole request
// budget instead of failing over. Cluster metadata is sessionless and works on
// the unauthenticated connection.
func (c *IggyTcpClient) establishSession(ctx context.Context, skipAutoLogin bool) error {
	// The metadata request runs while Connect is on the stack. Reconnecting
	// it would re-enter Connect and recurse without a bound, so its failure
	// unwinds to this Connect instead. The sign-in below keeps the reconnect
	// path: its replay is the documented recovery for a transient register
	// failure, and the replayed sign-in suppresses the automatic one, so the
	// depth is bounded at one nested Connect.
	redirect, err := c.HandleLeaderRedirection(context.WithValue(ctx, connectScoped{}, struct{}{}))
	if err != nil {
		return err
	}
	if redirect {
		return c.Connect(ctx)
	}

	if !c.config.autoLogin.enabled {
		c.logger.Info("Automatic sign-in is disabled.")
		return nil
	}
	if skipAutoLogin {
		c.logger.Info("Skipping the automatic sign-in for a replayed login.")
		return nil
	}

	credentials := c.config.autoLogin.credentials
	if credentials.personalAccessToken != "" {
		_, err = c.LoginWithPersonalAccessToken(ctx, credentials.personalAccessToken)
		return err
	}
	_, err = c.LoginUser(ctx, credentials.username, credentials.password)
	return err
}

func (c *IggyTcpClient) createTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: !c.config.tls.tlsValidateCertificate,
	}

	// Set server name for SNI
	serverName := c.config.tls.tlsDomain
	if serverName == "" {
		host, _, err := net.SplitHostPort(c.currentServerAddress)
		if err != nil {
			host = c.currentServerAddress
		}
		serverName = host
	}

	if serverName == "" {
		c.logger.Error("Failed to create a server name from the domain.", slog.Any("error", ierror.ErrInvalidTlsDomain))
		return nil, ierror.ErrInvalidTlsDomain
	}
	tlsConfig.ServerName = serverName

	// Load CA certificate if provided
	if c.config.tls.tlsCAFile != "" {
		caCert, err := os.ReadFile(c.config.tls.tlsCAFile)
		if err != nil {
			c.logger.Error("Failed to read the CA file", slog.String("certificate_path", c.config.tls.tlsCAFile), slog.Any("error", err))
			return nil, ierror.ErrInvalidTlsCertificatePath
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			c.logger.Error(
				"Failed to parse the CA certificate.",
				slog.String("certificate_path", c.config.tls.tlsCAFile),
			)
			return nil, ierror.ErrInvalidTlsCertificate
		}

		tlsConfig.RootCAs = caCertPool
	}

	return tlsConfig, nil
}

func (c *IggyTcpClient) disconnect() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.transportState == iggcon.TransportStateDisconnected || c.transportState == iggcon.TransportStateShutdown {
		return nil
	}

	c.logger.Info("Iggy client is disconnecting from server...", slog.String("client_address", c.clientAddress))
	c.transportState = iggcon.TransportStateDisconnected
	c.sessionState = iggcon.SessionStateUnauthenticated
	c.session.Reset()
	c.groups.clear()
	c.topics.clear()

	err := c.closeConnLocked()

	c.logger.Info("Iggy client has disconnected from server.", slog.String("client_address", c.clientAddress))
	// TODO event pushing logic
	return err
}

func (c *IggyTcpClient) shutdown() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.transportState == iggcon.TransportStateShutdown {
		return nil
	}

	c.logger.Info("Shutting down the Iggy TCP client...", slog.String("client_address", c.clientAddress))

	err := c.closeConnLocked()

	c.transportState = iggcon.TransportStateShutdown
	c.sessionState = iggcon.SessionStateUnauthenticated
	c.session.Reset()
	c.groups.clear()
	c.topics.clear()
	c.logger.Info("Iggy TCP client has been shutdown.", slog.String("client_address", c.clientAddress))
	// TODO push shutdown event
	return err
}

func (c *IggyTcpClient) Close() error {
	return c.shutdown()
}
