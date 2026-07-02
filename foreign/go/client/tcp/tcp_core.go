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
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
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
	connectedAt            time.Time
	state                  iggcon.State
	// respHeader is the reused response-status read buffer; guarded by c.mtx.
	respHeader [ResponseInitialBytesLength]byte
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
		state:                  iggcon.StateDisconnected,
		connectedAt:            time.Time{},
		leaderRedirectionState: iggcon.LeaderRedirectionState{},
		currentServerAddress:   opts.config.serverAddress,
	}
}

const (
	RequestInitialBytesLength  = 4
	ResponseInitialBytesLength = 8
	MaxStringLength            = 255
	MaxPartitionCount          = 1000
)

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
		totalWritten += n
	}

	return totalWritten, nil
}

// do sends the command and returns the response body. Commands implementing
// the appender interface encode directly into a pooled buffer.
func (c *IggyTcpClient) do(ctx context.Context, cmd command.Command) ([]byte, error) {
	bp := acquireRequestBuf()
	buf, err := encodeWireRequest(*bp, cmd)
	if err != nil {
		releaseRequestBuf(bp)
		return nil, err
	}
	*bp = buf

	resp, err := c.sendWireAndFetchResponse(ctx, buf)
	releaseRequestBuf(bp)
	return resp, err
}

// encodeWireRequest writes the wire-format request (4-byte length, 4-byte
// code, then body) into buf, growing it as needed. The length prefix is
// written from the realized body length, so a buggy or unimplemented
// AppendBinary can never corrupt the wire frame (which would desync the
// persistent TCP stream — there is no per-request resync).
func encodeWireRequest(buf []byte, cmd command.Command) ([]byte, error) {
	buf = append(buf[:0], 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(cmd.Code()))
	if a, ok := cmd.(encoding.BinaryAppender); ok {
		out, err := a.AppendBinary(buf)
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(out[0:4], uint32(len(out)-4))
		return out, nil
	}
	body, err := cmd.MarshalBinary()
	if err != nil {
		return nil, err
	}
	buf = append(buf, body...)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(buf)-4))
	return buf, nil
}

// sendWireAndFetchResponse sends a pre-built wire payload (length header,
// command code, body) and returns the response body.
func (c *IggyTcpClient) sendWireAndFetchResponse(ctx context.Context, wirePayload []byte) ([]byte, error) {
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	switch c.state {
	case iggcon.StateShutdown:
		c.logger.Debug("Cannot send data. Client is shutdown.")
		return nil, ierror.ErrClientShutdown
	case iggcon.StateDisconnected:
		c.logger.Debug("Cannot send data. Client is not connected.")
		return nil, ierror.ErrNotConnected
	case iggcon.StateConnecting:
		c.logger.Debug("Cannot send data. Client is still connecting.")
		return nil, ierror.ErrNotConnected
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// fast path for non-cancellable ctx.
	if ctx.Done() == nil {
		return c.sendLocked(wirePayload)
	}

	conn := c.conn
	var deadlineMu sync.Mutex
	cleared := false

	stop := context.AfterFunc(ctx, func() {
		deadlineMu.Lock()
		defer deadlineMu.Unlock()
		if !cleared {
			// Set a deadline in the past to unblock any ongoing read/write operations on the connection.
			// This must use the snapshotted conn, not c.conn, to avoid setting a deadline on a
			// new connection if Connect() reestablishes the connection after the context is cancelled.
			_ = conn.SetDeadline(time.Now())
		}
	})
	defer stop()

	result, err := c.sendLocked(wirePayload)

	// clear the deadline of connection.
	deadlineMu.Lock()
	cleared = true
	_ = conn.SetDeadline(time.Time{})
	deadlineMu.Unlock()

	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, err
	}
	return result, nil
}

func (c *IggyTcpClient) sendLocked(wirePayload []byte) ([]byte, error) {
	commandCode := command.Code(binary.LittleEndian.Uint32(wirePayload[4:8]))
	c.logger.Debug("Sending a TCP request", slog.Int("payload_length", len(wirePayload)), slog.Int("code", int(commandCode)))
	if _, err := c.write(wirePayload); err != nil {
		c.invalidateConnLocked()
		return nil, err
	}

	c.logger.Debug("Sent a TCP request, waiting for a response...", slog.Int("code", int(commandCode)))

	if _, err := c.readInto(c.respHeader[:]); err != nil {
		c.logger.Error("Failed to read response for TCP request", slog.Int("code", int(commandCode)), slog.Any("error", err))
		c.invalidateConnLocked()
		return nil, err
	}

	if status := ierror.Code(binary.LittleEndian.Uint32(c.respHeader[0:4])); status != 0 {
		return nil, ierror.FromCode(status)
	}

	length := int(binary.LittleEndian.Uint32(c.respHeader[4:]))
	c.logger.Debug("Status: OK.", slog.Int("response_length", length))

	if length <= 1 {
		return []byte{}, nil
	}

	_, buffer, err := c.read(length)
	if err != nil {
		c.invalidateConnLocked()
		return nil, err
	}

	return buffer, nil
}

// invalidateConnLocked closes the connection and marks it as disconnected
func (c *IggyTcpClient) invalidateConnLocked() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.state = iggcon.StateDisconnected
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
	switch c.state {
	case iggcon.StateShutdown:
		c.mtx.Unlock()
		c.logger.Debug("Cannot connect. Client is shutdown.")
		return ierror.ErrClientShutdown
	case iggcon.StateConnected,
		iggcon.StateAuthenticating,
		iggcon.StateAuthenticated:
		clientAddress := c.clientAddress
		c.mtx.Unlock()
		c.logger.Debug("Client is already connected.", slog.String("client_address", clientAddress))
		return nil
	case iggcon.StateConnecting:
		c.mtx.Unlock()
		c.logger.Debug("Client is already connecting.")
		return nil
	default:
		c.state = iggcon.StateConnecting
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
			c.logger.Info("Iggy client is connecting to server...", slog.String("server_address", c.currentServerAddress))
			connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.currentServerAddress)
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
			if err := tlsConn.Handshake(); err != nil {
				c.logger.Error("Failed to establish a TLS connection to the server", slog.Any("error", err))
				_ = connection.Close()
				return fmt.Errorf("TLS handshake failed: %w", err)
			}

			conn = tlsConn
			return nil
		}); err != nil {
		c.mtx.Lock()
		c.state = iggcon.StateDisconnected
		c.mtx.Unlock()
		if !c.config.reconnection.enabled {
			c.logger.Warn("Automatic reconnection is disabled.")
		}
		// TODO publish event disconnected
		return err
	}

	c.mtx.Lock()
	c.conn = conn
	c.state = iggcon.StateConnected
	c.connectedAt = time.Now()
	c.logger.Info("Iggy client has connected to the Iggy server", slog.String("client_address", c.clientAddress), slog.String("server_address", c.currentServerAddress))
	c.mtx.Unlock()
	return nil
}

func (c *IggyTcpClient) createTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: !c.config.tls.tlsValidateCertificate,
	}

	// Set server name for SNI
	serverName := c.config.tls.tlsDomain
	if serverName == "" {
		// Extract hostname from server address (format: "host:port")
		host := c.currentServerAddress
		if colonIdx := strings.LastIndex(host, ":"); colonIdx != -1 {
			host = host[:colonIdx]
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

	if c.state == iggcon.StateDisconnected {
		return nil
	}

	c.logger.Info("Iggy client is disconnecting from server...", slog.String("client_address", c.clientAddress))
	c.state = iggcon.StateDisconnected

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return err
		}
	}

	c.logger.Info("Iggy client has disconnected from server.", slog.String("client_address", c.clientAddress))
	// TODO event pushing logic
	return nil
}

func (c *IggyTcpClient) shutdown() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.state == iggcon.StateShutdown {
		return nil
	}

	c.logger.Info("Shutting down the Iggy TCP client...", slog.String("client_address", c.clientAddress))

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return err
		}
	}

	c.state = iggcon.StateShutdown
	c.logger.Info("Iggy TCP client has been shutdown.", slog.String("client_address", c.clientAddress))
	// TODO push shutdown event
	return nil
}

func (c *IggyTcpClient) Close() error {
	return c.shutdown()
}
