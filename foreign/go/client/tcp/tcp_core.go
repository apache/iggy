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
	"slices"
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

// connectAttempt is one run of Connect, shared with the callers waiting on it.
//
// The outcome is kept per attempt rather than in a field on the client: a
// waiter that read a shared field would read whatever the attempt after the one
// it waited on had written there, and a fresh attempt has no outcome yet.
type connectAttempt struct {
	// done is closed once the attempt settles, whichever way it ends.
	done chan struct{}
	// err is the attempt's outcome, written before done is closed.
	err error
	// suppressesLogin records that the owner does not sign in, which is what
	// makes the attempt safe to wait on from inside the sign-in transaction.
	suppressesLogin bool
}

type IggyTcpClient struct {
	conn net.Conn
	// reader buffers reads off conn, so a reply costs one syscall instead of
	// one for the header and one for the body; guarded by c.mtx.
	reader *bufio.Reader
	mtx    sync.Mutex
	// registerMtx single-flights the sign-in transaction: BeginRegister and
	// Bind live in different c.mtx critical sections, and two interleaved
	// sign-ins would commit one Register whose session is never bound.
	registerMtx sync.Mutex
	// closed unblocks a replay wait when Close is called, which cannot go
	// through c.mtx because the replay loop holds it.
	closed                 chan struct{}
	closeOnce              sync.Once
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
	// loggedOut records an explicit sign-out, so a reconnect's automatic
	// sign-in does not silently reverse it; guarded by c.mtx.
	loggedOut bool
	// connectAttempt is the attempt a Connect is running, shared with every
	// caller that arrives while it is in progress. Guarded by c.mtx.
	connectAttempt *connectAttempt
	// rememberedLogin holds the credentials a manual sign-in succeeded with,
	// so a reconnect -- on this node or, after a failover, another one -- can
	// re-establish the session instead of surfacing an unauthenticated error.
	// A caller that signs in by hand is otherwise less reconnectable than one
	// that configures auto-login, which is a surprising difference between
	// two ways of doing the same thing. Cleared on sign-out; guarded by
	// c.mtx.
	rememberedLogin AutoLogin
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
		// The lockstep request model stalls a multi-segment frame behind
		// Nagle's algorithm, so coalescing is off by default.
		noDelay: true,
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

// WithNoDelay controls TCP_NODELAY. It defaults to true because the lockstep
// request model stalls behind Nagle's algorithm; pass false to re-enable
// segment coalescing for bandwidth-bound workloads.
func WithNoDelay(noDelay bool) Option {
	return func(opts *Options) {
		opts.config.noDelay = noDelay
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
		closed:                 make(chan struct{}),
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
	// failoverDialTimeout bounds one endpoint's dial and handshake while other
	// endpoints are queued behind it. Neither step has a deadline of its own,
	// so a node whose syns are dropped would hold the sweep for the whole
	// kernel connect timeout while a survivor goes untried.
	failoverDialTimeout = 2 * time.Second
)

// requestBufPool reuses wire-payload buffers across RPCs. A fresh buffer
// already holds the header plus room for a small payload, so a metadata
// command does not reallocate on its first byte.
var requestBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, vsr.HeaderSize+512)
		return &b
	},
}

func acquireRequestBuf() *[]byte {
	return requestBufPool.Get().(*[]byte)
}

func releaseRequestBuf(bp *[]byte) {
	// Producer batches routinely grow to megabytes and reusing them is the
	// point of the pool; the ceiling only stops a pathological frame from
	// pinning memory for the process lifetime.
	const maxPooled = 4 * 1024 * 1024
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

// readInto reads exactly len(buf) bytes from the connection into buf, through
// the buffered reader when the connect flow installed one.
func (c *IggyTcpClient) readInto(buf []byte) (int, error) {
	if c.reader != nil {
		return io.ReadFull(c.reader, buf)
	}
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
	if frame != nil {
		// Keep the grown buffer even when the encode failed, so the pool gets
		// it back at its new capacity.
		*bp = frame
	}
	if err != nil {
		return nil, err
	}

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

	frame := append(reserveHeader(*bp), payload...)
	*bp = frame

	return c.exchange(ctx, code, frame)
}

// reserveHeader returns buf truncated to exactly the header prologue, growing
// it as needed. The prologue bytes are not zeroed here: EncodeRequestHeader
// clears the full header when the frame is stamped, and zeroing twice would
// write four extra cache lines per request.
func reserveHeader(buf []byte) []byte {
	return slices.Grow(buf[:0], vsr.HeaderSize)[:vsr.HeaderSize]
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
// encoded command payload after it. A command implementing
// encoding.BinaryAppender encodes straight into the buffer, keeping the frame
// a single allocation; the MarshalBinary fallback pays one extra body
// allocation and copy.
func appendCommandFrame(buf []byte, cmd command.Command) ([]byte, error) {
	buf = reserveHeader(buf)
	if appender, ok := cmd.(encoding.BinaryAppender); ok {
		return appender.AppendBinary(buf)
	}
	body, err := cmd.MarshalBinary()
	if err != nil {
		return buf, err
	}
	return append(buf, body...), nil
}

// connectScoped marks the context of a request that must not enter the
// reconnect path: the sign-in flow holding the register lock is on the stack
// (possibly under Connect), and the reconnect's automatic sign-in would
// deadlock on that lock or recurse Connect without a bound.
type connectScoped struct{}

// skipAutoLogin marks the context of a Connect whose caller owns the sign-in:
// a replayed login, or a redirect inside the sign-in transaction. Carried on
// the context rather than on the client, so it cannot outlive the call that
// meant it -- a client-wide flag leaks when Connect returns early on the
// already-connected gate, and then suppresses somebody else's auto-login.
type skipAutoLogin struct{}

// skipLeaderSettlement keeps the sign-in owned by one roster-walk Connect on
// the endpoint it dialed. Context scope prevents a failed or cancelled Connect
// from suppressing an unrelated later sign-in.
type skipLeaderSettlement struct{}

// suppressAutoLogin returns ctx marked so the Connect it drives does not sign
// in by itself.
func suppressAutoLogin(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipAutoLogin{}, struct{}{})
}

func suppressLeaderSettlement(ctx context.Context) context.Context {
	return context.WithValue(ctx, skipLeaderSettlement{}, struct{}{})
}

// localPreconditionError marks a request that failed before its frame was
// written. The connection is healthy, so exchange must not tear it down and
// re-dial over what is purely local state.
type localPreconditionError struct{ err error }

func (e *localPreconditionError) Error() string { return e.err.Error() }
func (e *localPreconditionError) Unwrap() error { return e.err }

// exchange runs one request to completion, reconnecting and replaying it when
// the failure is one a fresh connection recovers from.
func (c *IggyTcpClient) exchange(ctx context.Context, code uint32, frame []byte) ([]byte, error) {
	response, err := c.sendFrame(ctx, code, frame)
	if err == nil || !isReconnectable(err) {
		return response, err
	}

	// A stale-client eviction is not caller intent: the heartbeat verifier
	// sends it after a gc pause or a laptop sleep, so the remembered sign-in
	// survives it and the reconnect re-establishes the session. Only an
	// explicit sign-out ends it. Same rule in every SDK.
	var precondition *localPreconditionError
	if errors.As(err, &precondition) {
		return nil, err
	}
	if ctx.Value(connectScoped{}) != nil {
		return nil, err
	}
	if !c.config.reconnection.enabled {
		c.logger.Warn("Automatic reconnection is disabled.")
		return nil, err
	}

	// With no credentials -- neither configured nor remembered from a
	// sign-in -- a reconnect cannot restore the session, so anything but a
	// sign-in fails here instead of replaying unauthenticated. The sign-in
	// itself is the exception: the server stays silent on a transient
	// register failure and expects the client to replay it.
	login := isRegisterCode(code)
	if _, ok := c.signInCredentials(); !ok && !login {
		return nil, err
	}
	c.mtx.Lock()
	loggedOut := c.loggedOut
	c.mtx.Unlock()
	if loggedOut && !login {
		// An explicit sign-out stays signed out: the reconnect's automatic
		// sign-in would silently reverse it.
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
	reconnectCtx := ctx
	if login {
		// The caller replays the login itself, so the reconnect must not.
		reconnectCtx = suppressAutoLogin(ctx)
	}

	c.mtx.Lock()
	serverAddress := c.currentServerAddress
	c.mtx.Unlock()
	c.logger.Info("Reconnecting to the server...",
		slog.String("server_address", serverAddress),
		slog.Any("error", err))

	if reconnectErr := c.Connect(reconnectCtx); reconnectErr != nil {
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
	// Once this request starts walking the roster it keeps walking: a leader
	// recheck between hops would put it straight back on the metadata leader
	// whose partition replica refused it, and the walk would bounce between
	// two nodes without ever reaching the rest of the roster.
	walkingRoster := false
	visitedRosterEndpoints := make(map[string]struct{})
	for {
		// A sign-in owns the whole budget on this connection: any node
		// completes it (a backup forwards the register to the primary), and
		// failing over from under the handshake would recurse back into the
		// sign-in flow.
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
			redirect := false
			walked := false
			if !walkingRoster {
				var redirectErr error
				redirect, redirectErr = c.HandleLeaderRedirection(ctx)
				if redirectErr != nil {
					return nil, redirectErr
				}
			}
			if !redirect {
				// The roster names this node as the metadata leader (or said
				// nothing usable), yet it keeps refusing to admit the
				// request: its replica of the target partition group is not
				// that group's primary, because metadata and partition
				// consensus groups elect independently. Walk the roster
				// instead of replaying into the same refusal until the whole
				// request budget burns.
				var walkErr error
				walked, walkErr = c.settleOnNextEndpoint(visitedRosterEndpoints)
				if walkErr != nil {
					return nil, walkErr
				}
				if walked {
					walkingRoster = true
					redirect = true
				}
			}
			if redirect {
				redirectCtx := ctx
				if walked {
					redirectCtx = suppressLeaderSettlement(redirectCtx)
				}
				if ctx.Value(connectScoped{}) != nil {
					// Issued from inside the sign-in transaction, which holds
					// registerMtx: the automatic sign-in on the reconnect path
					// would wait on that lock forever. The transaction signs in
					// itself on the node it lands on, so the reconnect must not.
					redirectCtx = suppressAutoLogin(ctx)
				}
				if connectErr := c.Connect(redirectCtx); connectErr != nil {
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
		// A stamp failure is local and pre-write, so it is marked as such:
		// the connection is healthy and must not be torn down for it.
		if err := vsr.StampRequestHeader(c.session, code, frame); err != nil {
			return nil, false, &localPreconditionError{err}
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
			// Normalized like the read failures below, so callers can match
			// a dropped connection with one sentinel in both directions.
			c.logger.Error("Failed to write the request frame",
				slog.Int("code", int(code)), slog.Any("error", err))
			c.invalidateConnLocked()
			return nil, ierror.ErrDisconnected
		}

		body, err := c.readReplyLocked(code)
		if err != nil {
			return nil, err
		}
		if vsr.PeekCommand(&c.respHeader) == vsr.FrameReply {
			// A reply must answer the request in flight. An unexpected echo
			// means the stream is delivering some other request's answer, and
			// every later reply would pair off by one.
			expected := vsr.StampedRequestID(frame)
			if echoed := vsr.ReadReplyRequestID(&c.respHeader); echoed != expected {
				c.logger.Error("The reply answers a different request",
					slog.Uint64("expected_request", expected),
					slog.Uint64("echoed_request", echoed))
				c.invalidateConnLocked()
				return nil, ierror.ErrDisconnected
			}
		}

		response, err := vsr.DecodeReply(&c.respHeader, body)
		switch {
		case errors.Is(err, ierror.ErrTransientNotCommitted) && time.Now().Before(readDeadline):
			// The outcome is unknown, so only a replay of the same request id
			// on this session is safe. On the metadata plane the client table
			// answers a committed request from its reply cache; the partition
			// plane keeps no client table, so its replay is at-least-once.
			if waitErr := c.waitBeforeReplay(ctx, readDeadline); waitErr != nil {
				return nil, waitErr
			}
		case errors.Is(err, ierror.ErrTransientNotAccepted) && time.Now().Before(transientDeadline):
			if waitErr := c.waitBeforeReplay(ctx, transientDeadline); waitErr != nil {
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
// never past the deadline, the caller's cancellation, or a client shutdown.
// The shutdown channel matters because this wait runs with c.mtx held, which
// is the lock Close needs; without it Close would block for the rest of the
// request budget.
func (c *IggyTcpClient) waitBeforeReplay(ctx context.Context, deadline time.Time) error {
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
	case <-c.closed:
		return ierror.ErrClientShutdown
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
	c.topics.clearCounts()
}

// closeConnLocked closes and drops the current connection.
func (c *IggyTcpClient) closeConnLocked() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	c.reader = nil
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
//
// Single-flighted: one attempt dials, and every caller that arrives while it
// runs waits for it and shares its outcome. Reporting success to those callers
// instead would hand them a client with no connection yet, and their next
// request would fail ErrNotConnected for no reason of its own.
func (c *IggyTcpClient) Connect(ctx context.Context) (err error) {
	suppressesLogin := ctx.Value(skipAutoLogin{}) != nil
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
		attempt := c.connectAttempt
		c.mtx.Unlock()
		if attempt == nil {
			return nil
		}
		if suppressesLogin && !attempt.suppressesLogin {
			// Only the sign-in transaction suppresses the automatic sign-in,
			// and it holds registerMtx while it does. The attempt in flight
			// ends in a sign-in that needs that same lock, so waiting here
			// would close a cycle: the owner blocked on registerMtx, this
			// goroutine blocked on the owner, and neither context cancelled.
			c.logger.Debug("Another connect is signing in; not waiting for it.")
			return ierror.ErrCannotEstablishConnection
		}
		c.logger.Debug("Client is already connecting; waiting for that attempt.")
		select {
		case <-attempt.done:
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closed:
			return ierror.ErrClientShutdown
		}
		return attempt.err
	}
	attempt := &connectAttempt{
		done:            make(chan struct{}),
		suppressesLogin: suppressesLogin,
	}
	c.transportState = iggcon.TransportStateConnecting
	c.connectAttempt = attempt
	connectedAt := c.connectedAt
	c.mtx.Unlock()

	// Settles the attempt for whoever is waiting on it, whichever way it ends.
	defer func() {
		attempt.err = err
		c.mtx.Lock()
		if c.connectAttempt == attempt {
			c.connectAttempt = nil
		}
		c.mtx.Unlock()
		close(attempt.done)
	}()

	candidates := c.connectionCandidates()
	if len(candidates) == 0 {
		// Nowhere to dial: a client configured with an empty server address
		// and no roster. Reporting success here would leave every request
		// answering ErrNotConnected while Connect keeps saying it is
		// connected.
		c.mtx.Lock()
		c.transportState = iggcon.TransportStateDisconnected
		c.mtx.Unlock()
		c.logger.Error("No server address to connect to.")
		return ierror.ErrCannotEstablishConnection
	}

	// reestablishAfter paces reconnects to the endpoint this client was last
	// on, and to that one only: the other endpoints owe it no cooldown, and
	// pausing before dialing them would push the failover past the window the
	// caller is willing to wait. So when there is somewhere else to go, the
	// paced endpoint goes last -- by which time its window has usually
	// elapsed anyway -- instead of the wait being skipped outright.
	pacedEndpoint := candidates[0]
	if !connectedAt.IsZero() && len(candidates) > 1 &&
		time.Since(connectedAt) < c.config.reconnection.reestablishAfter {
		candidates = append(candidates[1:], pacedEndpoint)
	}
	attempts := uint(1)
	interval := time.Duration(0)
	if c.config.reconnection.enabled {
		attempts = uint(c.config.reconnection.maxRetries)
		interval = c.config.reconnection.interval
	}
	var conn net.Conn
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
			// Every endpoint gets its turn inside one attempt, so a full pass
			// over the cluster costs one retry rather than one per endpoint:
			// a pass that stopped at the first refusal would never reach the
			// survivors of a client configured for a single retry.
			var lastErr error
			// A fault no retry can fix, kept aside rather than returned at
			// once: it belongs to the endpoint that raised it, and the
			// endpoints behind that one may be perfectly usable.
			var configFault error
			for _, address := range candidates {
				if address == pacedEndpoint {
					c.awaitReestablish(ctx, connectedAt)
				}
				connection, err := c.dialCandidate(ctx, address, len(candidates) > 1)
				if err != nil {
					lastErr = err
					if isTLSConfigFault(err) {
						configFault = err
					}
					continue
				}

				conn = connection
				return nil
			}

			// An unreadable CA file, an unparsable domain, a certificate this
			// client will never accept: no endpoint answered, and at least one
			// said why in a way no retry changes. Reported as unrecoverable so
			// the default unlimited retries do not redial it every interval
			// forever and bury it.
			if configFault != nil {
				return retry.Unrecoverable(configFault)
			}

			return lastErr
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
	if state := c.transportState; state != iggcon.TransportStateConnecting {
		// Superseded while this attempt was dialing. The connection it just
		// made is surplus either way, but what to report differs: a client
		// another attempt already connected is connected, and saying
		// otherwise would fail a caller whose client is up.
		c.mtx.Unlock()
		_ = conn.Close()
		c.logger.Debug("The connect was superseded while dialing; dropping the connection.",
			slog.Any("transport_state", state))
		switch state {
		case iggcon.TransportStateShutdown:
			return ierror.ErrClientShutdown
		case iggcon.TransportStateConnected:
			return nil
		default:
			return ierror.ErrNotConnected
		}
	}
	c.conn = conn
	c.reader = bufio.NewReaderSize(conn, 64*1024)
	c.transportState = iggcon.TransportStateConnected
	c.connectedAt = time.Now()
	// The server fence does not survive the old socket, so the new connection
	// starts from a fresh client identity.
	c.session.Reset()
	clientAddress := c.clientAddress
	serverAddress := c.currentServerAddress
	c.mtx.Unlock()
	c.logger.Info("Iggy client has connected to the Iggy server",
		slog.String("client_address", clientAddress),
		slog.String("server_address", serverAddress))

	if err := c.establishSession(ctx, ctx.Value(skipAutoLogin{}) != nil); err != nil {
		_ = c.disconnect()
		return err
	}
	return nil
}

// isTLSConfigFault reports whether a dial failed for a reason that says the
// client's own TLS configuration is wrong -- an unreadable or unparsable CA
// file, a domain that yields no server name, or a certificate this client will
// never accept. None of those change on a retry.
//
// A peer that answered the ClientHello in plaintext is not one of them: that
// says something about the endpoint, not about this client, and the endpoints
// behind it in the roster may be speaking TLS perfectly well.
func isTLSConfigFault(err error) bool {
	if errors.Is(err, ierror.ErrInvalidTlsCertificatePath) ||
		errors.Is(err, ierror.ErrInvalidTlsCertificate) ||
		errors.Is(err, ierror.ErrInvalidTlsDomain) {
		return true
	}

	var certificateError *tls.CertificateVerificationError
	return errors.As(err, &certificateError)
}

// awaitReestablish waits out what is left of the reestablishAfter window since
// the last successful connection, if any.
//
// The wait ends early on the caller's context or on Close: a sweep that found
// every other endpoint refused reaches the paced one in milliseconds, and
// sleeping the rest of the window regardless would hold the client in
// Connecting long past the deadline the caller gave it.
func (c *IggyTcpClient) awaitReestablish(ctx context.Context, connectedAt time.Time) {
	if connectedAt.IsZero() {
		return
	}

	elapsed := time.Since(connectedAt)
	c.logger.Debug("Elapsed time since last connection", slog.Duration("elapsed", elapsed))
	remaining := c.config.reconnection.reestablishAfter - elapsed
	if remaining <= 0 {
		return
	}

	c.logger.Info("Trying to connect to the server", slog.Duration("remaining", remaining))
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	case <-c.closed:
	}
}

// dialCandidate brings one endpoint all the way up, wrapping it in TLS when
// configured, and records the endpoint that answered: the leader check
// compares against it and the next reconnect starts from it.
//
// bounded caps the whole attempt at failoverDialTimeout, for when other
// endpoints are queued behind this one. Neither the dial nor the handshake has
// a deadline of its own, and a node whose syns are dropped -- or one that
// accepts TCP and then never answers the ClientHello -- would hold the sweep
// for minutes while a survivor goes untried.
func (c *IggyTcpClient) dialCandidate(ctx context.Context, address string, bounded bool) (net.Conn, error) {
	c.logger.Info("Iggy client is connecting to server...", slog.String("server_address", address))
	if bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, failoverDialTimeout)
		defer cancel()
	}

	connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		c.logger.Error("Failed to establish TCP connection to the server", slog.Any("error", err))
		return nil, ierror.ErrCannotEstablishConnection
	}

	tc := connection.(*net.TCPConn)
	if err := tc.SetNoDelay(c.config.noDelay); err != nil {
		c.logger.Error("Failed to set the nodelay option on the client, continuing...", slog.Any("error", err))
	}

	established := connection
	if c.config.tlsEnabled {
		tlsConfig, err := c.createTLSConfig(address)
		if err != nil {
			_ = connection.Close()
			return nil, err
		}

		tlsConn := tls.Client(connection, tlsConfig)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			c.logger.Error("Failed to establish a TLS connection to the server", slog.Any("error", err))
			_ = connection.Close()
			return nil, fmt.Errorf("TLS handshake failed: %w", err)
		}
		established = tlsConn
	}

	// Recorded only once the connection is usable: an endpoint that accepts
	// TCP but fails the handshake is not where this client lives, and leading
	// the next pass with it would shadow every endpoint behind it.
	c.mtx.Lock()
	c.clientAddress = tc.LocalAddr().String()
	c.currentServerAddress = address
	c.mtx.Unlock()

	return established, nil
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

// establishSession signs in when auto-login is configured.
//
// Leader settlement runs after the sign-in (see settleOnLeader): the roster
// read is auth-gated, so it only works once a login binds a session, and a
// login dialed at a backup still succeeds because the server forwards the
// register to the primary. Without auto-login the connection can stay on a
// backup: once the caller signs in, the first replicated request fails over
// through the transient-deny path.
func (c *IggyTcpClient) establishSession(ctx context.Context, skipAutoLogin bool) error {
	credentials, ok := c.signInCredentials()
	if !ok {
		c.logger.Info("No credentials to sign in with.")
		return nil
	}
	if skipAutoLogin {
		c.logger.Info("Skipping the automatic sign-in for a replayed login.")
		return nil
	}

	if credentials.personalAccessToken != "" {
		_, err := c.LoginWithPersonalAccessToken(ctx, credentials.personalAccessToken)
		return err
	}
	_, err := c.LoginUser(ctx, credentials.username, credentials.password)
	return err
}

// signInCredentials reports the credentials a reconnect signs in with: the
// configured ones, or else the ones a manual sign-in succeeded with.
func (c *IggyTcpClient) signInCredentials() (Credentials, bool) {
	if c.config.autoLogin.enabled {
		return c.config.autoLogin.credentials, true
	}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return c.rememberedLogin.credentials, c.rememberedLogin.enabled
}

// rememberLogin keeps the credentials a sign-in succeeded with. Call it from
// under registerMtx (register does): remembered outside that lock, two
// concurrent sign-ins can leave A remembered while the session is B.
func (c *IggyTcpClient) rememberLogin(credentials Credentials) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.rememberedLogin = NewAutoLogin(credentials)
}

// forgetLogin drops them: after an explicit sign-out there is no session to
// restore, and a reconnect must not resurrect one.
func (c *IggyTcpClient) forgetLogin() {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.rememberedLogin = AutoLogin{}
}

// createTLSConfig builds the client config for one dial.
//
// address is the candidate being dialed, which is where the SNI comes from
// when no domain is configured. Taking it from currentServerAddress instead
// would name the endpoint the client just lost: with validation on, a failover
// to a node with another name or address then fails the handshake against a
// certificate that never covered the old one.
func (c *IggyTcpClient) createTLSConfig(address string) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: !c.config.tls.tlsValidateCertificate,
	}

	// Set server name for SNI
	serverName := c.config.tls.tlsDomain
	if serverName == "" {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			host = address
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
	if c.transportState == iggcon.TransportStateConnecting {
		// An attempt is already dialing. Every caller here is tearing the
		// connection down to reconnect, which that attempt is doing anyway:
		// resetting the state under it would let the next Connect start a
		// second attempt, and the two would fight over which socket ends up
		// installed and which error the waiters are told about.
		c.logger.Debug("Not disconnecting; a connect is already in flight.")
		return nil
	}

	c.logger.Info("Iggy client is disconnecting from server...", slog.String("client_address", c.clientAddress))
	c.transportState = iggcon.TransportStateDisconnected
	c.sessionState = iggcon.SessionStateUnauthenticated
	c.session.Reset()
	c.groups.clear()
	c.topics.clearCounts()

	err := c.closeConnLocked()

	c.logger.Info("Iggy client has disconnected from server.", slog.String("client_address", c.clientAddress))
	// TODO event pushing logic
	return err
}

func (c *IggyTcpClient) shutdown() error {
	// Unblock any in-flight replay wait before taking the lock it holds,
	// otherwise this call queues behind the rest of that request's budget.
	c.closeOnce.Do(func() {
		if c.closed != nil {
			close(c.closed)
		}
	})

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
	c.topics.clearCounts()
	c.logger.Info("Iggy TCP client has been shutdown.", slog.String("client_address", c.clientAddress))
	// TODO push shutdown event
	return err
}

func (c *IggyTcpClient) Close() error {
	return c.shutdown()
}
