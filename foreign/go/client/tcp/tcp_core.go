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
	"log/slog"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
)

type Option func(config *Options)
type Options struct{ config config }

func GetDefaultOptions() Options { return Options{config: defaultTcpClientConfig()} }

type IggyTcpClient struct {
	mtx                sync.Mutex
	connectMu          sync.Mutex
	connectionAttempt  *connectAttempt
	recovery           context.Context
	recoveryCancel     context.CancelFunc
	registerGate       chan struct{}
	closed             chan struct{}
	closeOnce          sync.Once
	config             config
	logger             *slog.Logger
	MessageCompression iggcon.IggyMessageCompression
	session            *vsr.Session
	sessionFence       uint64
	sessionUserID      uint32
	loggedOut          bool
	autoLoginBlocked   bool
	rememberedLogin    AutoLogin
	epoch              atomic.Pointer[signInEpoch]
	metadataWatermark  atomic.Uint64
	operations         *operationRegistry
	links              *replicaLinks
	polls              pollRouter
	groups             groupAssignmentCache
	topics             topicCache
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
	noDelay              bool
	maxPendingOperations int
	maxPendingBytes      int
	heartbeatInterval    time.Duration
}

func defaultTcpClientConfig() config {
	return config{
		serverAddress: "127.0.0.1:8090",
		tlsEnabled:    false,
		tls:           defaultTLSConfig(),
		autoLogin:     AutoLogin{},
		reconnection:  defaultTcpClientReconnectionConfig(),
		// Small control frames must not wait for delayed acknowledgments.
		noDelay:              true,
		maxPendingOperations: DefaultMaxPendingOperations,
		maxPendingBytes:      DefaultMaxPendingOperationBytes,
		heartbeatInterval:    5 * time.Second,
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

// WithServerAddress Sets the server address for the TCP client. The link to
// the node of a one-node cluster always dials this address.
func WithServerAddress(address string) Option {
	return func(opts *Options) {
		opts.config.serverAddress = address
	}
}

// WithAutoLogin supplies credentials for the initial session. Replacement
// links bind the existing identity. A terminal session recovery signs in with
// these credentials until the caller signs in with LoginUser or
// LoginWithPersonalAccessToken, and as that sign-in after it. After
// LogoutUser, only such a sign-in by the caller or a Connect after Disconnect
// signs in again. Disconnect forgets the sign-in of the caller, so the next
// Connect signs in with these credentials. A failed explicit sign-in or a
// credential refusal blocks automatic sign-in until another explicit attempt.
// A committed password change or rename updates stored credentials, also
// when it is sent raw or proved by ResolveOperation.
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

// WithNoDelay controls TCP_NODELAY; it defaults to true.
func WithNoDelay(noDelay bool) Option {
	return func(opts *Options) {
		opts.config.noDelay = noDelay
	}
}

// WithPendingOperations bounds active and retained mutation frames, including headers.
func WithPendingOperations(maxOperations, maxBytes int) Option {
	return func(opts *Options) {
		opts.config.maxPendingOperations = maxOperations
		opts.config.maxPendingBytes = maxBytes
	}
}

// WithHeartbeatInterval controls managed activity on every ready replica link.
// A nonpositive interval disables the task.
func WithHeartbeatInterval(interval time.Duration) Option {
	return func(opts *Options) { opts.config.heartbeatInterval = interval }
}

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
	client := &IggyTcpClient{config: opts.config, logger: logger, session: vsr.NewSession(), sessionFence: 1, registerGate: make(chan struct{}, 1), closed: make(chan struct{})}
	client.operations = newOperationRegistry(opts.config.maxPendingOperations, opts.config.maxPendingBytes)
	client.links = newReplicaLinks(client)
	client.epoch.Store(newSignInEpoch())
	return client
}

const (
	MaxStringLength           = 255
	MaxPartitionCount         = 1000
	responseReadTimeout       = 30 * time.Second
	replayInterval            = 50 * time.Millisecond
	lifecycleRetryMaxInterval = time.Second
	failoverDialTimeout       = 2 * time.Second
)

// operationScope marks the context of a call. Its value is the sign-in epoch
// that the call started in, which every request inside the call keeps.
type operationScope struct{}

func (c *IggyTcpClient) operationContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Value(operationScope{}) != nil {
		return ctx, func() {}
	}
	timed, cancel := context.WithTimeout(ctx, responseReadTimeout)
	return context.WithValue(timed, operationScope{}, c.epoch.Load()), cancel
}

// callEpoch is the sign-in epoch that the call of ctx started in.
func (c *IggyTcpClient) callEpoch(ctx context.Context) *signInEpoch {
	if epoch, ok := ctx.Value(operationScope{}).(*signInEpoch); ok {
		return epoch
	}
	return c.epoch.Load()
}

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

// do sends the command and returns the response body. Commands implementing
// the appender interface encode directly into a pooled buffer.
func (c *IggyTcpClient) do(ctx context.Context, cmd command.Command) ([]byte, error) {
	return c.doCaptured(ctx, cmd, nil)
}

// doCaptured is do for a partition command that carries captured, a context
// the caller holds, instead of the context its route names.
func (c *IggyTcpClient) doCaptured(ctx context.Context, cmd command.Command, captured *iggcon.PartitionContext) ([]byte, error) {
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	bp := acquireRequestBuf()
	defer releaseRequestBuf(bp)
	frame, err := appendCommandFrame(*bp, cmd)
	if frame != nil {
		*bp = frame
	}
	if err != nil {
		return nil, err
	}
	if isPartitionCommand(cmd.Code()) {
		query, err := partitionQuery(cmd.Code(), frame[vsr.HeaderSize:])
		if err != nil {
			return nil, err
		}
		if poll, ok := cmd.(*command.PollMessages); ok && poll.Strategy.Context != nil {
			captured = poll.Strategy.Context
		}
		return c.drive(ctx, cmd.Code(), query, frame, captured)
	}
	return c.drive(ctx, cmd.Code(), nil, frame, nil)
}

// rawRequest carries a SendBinaryRequest through do, so a raw partition
// command captures and keeps its context exactly as a typed one does.
type rawRequest struct {
	code    command.Code
	payload []byte
}

func (r rawRequest) Code() command.Code { return r.code }

func (r rawRequest) MarshalBinary() ([]byte, error) { return r.payload, nil }

func (r rawRequest) AppendBinary(b []byte) ([]byte, error) { return append(b, r.payload...), nil }

// SendBinaryRequest sends a command code and payload and returns the raw response body.
// Session-control codes return ierror.ErrInvalidCommand without writing to the connection.
// Message sends, polls and consumer offset writes are stamped with the partition context
// their typed methods would capture, and keep it across retries. A send must name an
// explicit partition id: a balanced or key-partitioned send returns
// ierror.ErrFeatureUnavailable without writing, so use SendMessages for those.
func (c *IggyTcpClient) SendBinaryRequest(ctx context.Context, code uint32, payload []byte) ([]byte, error) {
	if isSessionControlCode(code) {
		return nil, ierror.ErrInvalidCommand
	}
	return c.do(ctx, rawRequest{code: command.Code(code), payload: payload})
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

func (c *IggyTcpClient) Connect(ctx context.Context) error {
	err := c.connect(ctx)
	var refusal *signInRefusal
	if errors.As(err, &refusal) {
		return refusal.err
	}
	return err
}

// connect is Connect, except that a refused sign-in stays a signInRefusal, so
// the recovery and the calls that wait for the attempt can tell it apart.
func (c *IggyTcpClient) connect(ctx context.Context) error {
	if ctx == nil {
		return ierror.ErrNilContext
	}
	if c.config.maxPendingOperations <= 0 || c.config.maxPendingBytes <= 0 {
		return ierror.ErrInvalidConfiguration
	}
	epoch := c.callEpoch(ctx)
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(epoch.ctx, cancel)
	defer cancel()
	defer stop()
	ctx = context.WithValue(ctx, operationScope{}, epoch)
	c.connectMu.Lock()
	if attempt := c.connectionAttempt; attempt != nil {
		c.connectMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-attempt.done:
			return attempt.err
		}
	}
	attempt := &connectAttempt{done: make(chan struct{})}
	c.connectionAttempt = attempt
	c.connectMu.Unlock()
	link, err := c.links.connect(ctx)
	if err == nil {
		c.links.mu.Lock()
		lifetime := c.links.ctx
		c.links.mu.Unlock()
		if lifetime == nil {
			err = ierror.ErrNotConnected
		} else {
			sessionCtx, endSignIn := signInContext(ctx, epoch)
			stop := context.AfterFunc(lifetime, endSignIn)
			err = c.establishSession(sessionCtx, link)
			stop()
			endSignIn()
		}
	}
	var refusal *signInRefusal
	if ended := epoch.err(); ended != nil && !errors.As(err, &refusal) {
		err = ended
	}
	c.connectMu.Lock()
	attempt.link, attempt.err = link, err
	c.connectionAttempt = nil
	close(attempt.done)
	c.connectMu.Unlock()
	return err
}

func (c *IggyTcpClient) GetConnectionInfo() *iggcon.ConnectionInfo {
	return &iggcon.ConnectionInfo{Protocol: iggcon.Tcp, ServerAddress: c.links.address()}
}

// ResolveOperation replays the retained bytes under their original identity.
// A committed business rejection is returned unchanged and releases the lane.
// For an operation that is no longer retained, it returns what a replay proved.
func (c *IggyTcpClient) ResolveOperation(ctx context.Context, operationID uint64) error {
	if ctx == nil {
		return ierror.ErrNilContext
	}
	ctx, cancel := c.operationContext(ctx)
	defer cancel()
	ticket, err := c.operations.resume(ctx, operationID)
	if errors.Is(err, ierror.ErrOperationNotRetained) {
		return c.operations.recordedOutcome(operationID)
	}
	if err != nil {
		return err
	}
	_, err = c.driveRetained(ctx, ticket)
	return err
}

// AbandonOperation releases local recovery ownership without proving non-execution.
// A running ResolveOperation keeps the lane and the outcome that it proves.
func (c *IggyTcpClient) AbandonOperation(ctx context.Context, operationID uint64) error {
	if ctx == nil {
		return ierror.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.operations.abandon(operationID)
}

func (c *IggyTcpClient) Disconnect() error {
	epoch := c.resetSession(false)
	c.mtx.Lock()
	if c.epoch.Load() != epoch {
		c.mtx.Unlock()
		return nil
	}
	// Registration publication is fenced by the reset. Forget credentials
	// before waiting, so completion cannot erase a later explicit sign-in.
	c.rememberedLogin = AutoLogin{}
	c.loggedOut = c.autoLoginBlocked
	stopped := c.links.detach()
	c.mtx.Unlock()
	stopped(true)
	c.connectMu.Lock()
	attempt := c.connectionAttempt
	c.connectMu.Unlock()
	if attempt != nil {
		<-attempt.done
	}
	return nil
}

func (c *IggyTcpClient) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return c.Disconnect()
}

func (c *IggyTcpClient) pause(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return ctx.Err()
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

func isTLSConfigFault(err error) bool {
	var certificateError *tls.CertificateVerificationError
	return errors.Is(err, ierror.ErrInvalidTlsCertificatePath) || errors.Is(err, ierror.ErrInvalidTlsCertificate) || errors.Is(err, ierror.ErrInvalidTlsDomain) || errors.As(err, &certificateError)
}

func (c *IggyTcpClient) dialCandidate(ctx context.Context, address string) (net.Conn, error) {
	c.logger.Info("Iggy client is connecting to server...", slog.String("server_address", address))
	ctx, cancel := context.WithTimeout(ctx, failoverDialTimeout)
	defer cancel()

	var tlsConfiguration *tls.Config
	if c.config.tlsEnabled {
		var err error
		tlsConfiguration, err = c.createTLSConfig(address)
		if err != nil {
			return nil, err
		}
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

		tlsConn := tls.Client(connection, tlsConfiguration)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			c.logger.Error("Failed to establish a TLS connection to the server", slog.Any("error", err))
			_ = connection.Close()
			return nil, fmt.Errorf("TLS handshake failed: %w", err)
		}
		established = tlsConn
	}

	return established, nil
}

func (c *IggyTcpClient) createTLSConfig(address string) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: !c.config.tls.tlsValidateCertificate,
	}

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
