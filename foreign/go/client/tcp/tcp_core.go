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

func (c *IggyTcpClient) read(expectedSize int) (int, []byte, error) {
	var totalRead int
	buffer := make([]byte, expectedSize)

	for totalRead < expectedSize {
		readSize := expectedSize - totalRead
		n, err := c.conn.Read(buffer[totalRead : totalRead+readSize])
		if err != nil {
			return totalRead, buffer[:totalRead], err
		}
		totalRead += n
	}

	return totalRead, buffer, nil
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

// do sends the command to the Iggy server and returns the response.
func (c *IggyTcpClient) do(ctx context.Context, cmd command.Command) ([]byte, error) {
	data, err := cmd.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return c.sendAndFetchResponse(ctx, data, cmd.Code())
}

func (c *IggyTcpClient) sendAndFetchResponse(ctx context.Context, message []byte, command command.Code) ([]byte, error) {
	if ctx == nil {
		return nil, ierror.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// fast path for non-cancellable ctx.
	if ctx.Done() == nil {
		return c.sendLocked(message, command)
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

	result, err := c.sendLocked(message, command)

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

func (c *IggyTcpClient) sendLocked(message []byte, command command.Code) ([]byte, error) {
	payload := createPayload(message, command)
	c.logger.Debug("Sending a TCP request", "payload_length", len(payload), "code", command)
	if _, err := c.write(payload); err != nil {
		c.invalidateConnLocked()
		return nil, err
	}

	c.logger.Debug("Sent a TCP request, waiting for a response...", "code", command)

	readBytes, buffer, err := c.read(ResponseInitialBytesLength)
	if err != nil {
		c.invalidateConnLocked()
		c.logger.Error("Failed to read response for TCP request", "code", command, "error", err)
		return nil, err
	}

	if readBytes != ResponseInitialBytesLength {
		c.invalidateConnLocked()
		c.logger.Error("Received an invalid or empty response.")
		return nil, fmt.Errorf("received an invalid or empty response: %w", ierror.EmptyResponse{})
	}

	if status := ierror.Code(binary.LittleEndian.Uint32(buffer[0:4])); status != 0 {
		return nil, ierror.FromCode(status)
	}

	length := int(binary.LittleEndian.Uint32(buffer[4:]))
	c.logger.Debug("Status: OK.", "response_length", length)

	if length <= 1 {
		return []byte{}, nil
	}

	_, buffer, err = c.read(length)
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

func createPayload(message []byte, command command.Code) []byte {
	messageLength := len(message) + 4
	messageBytes := make([]byte, RequestInitialBytesLength+messageLength)
	binary.LittleEndian.PutUint32(messageBytes[:4], uint32(messageLength))
	binary.LittleEndian.PutUint32(messageBytes[4:8], uint32(command))
	copy(messageBytes[8:], message)
	return messageBytes
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
		c.logger.Debug("Client is already connected.", "client_address", clientAddress)
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
		interval := c.config.reconnection.reestablishAfter

		c.logger.Debug("Elapsed time since last connection", "elapsed", elapsed)
		if elapsed < interval {
			remaining := interval - elapsed
			c.logger.Info("Trying to connect to the server", "remaining", remaining)
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
			c.logger.Info("Retrying to connect to server...", "retry_count", n+1, "max_retries", attempts, "error", err)
		}),
	).Do(
		func() error {
			c.logger.Info("Iggy client is connecting to server...", "server_address", c.currentServerAddress)
			connection, err := (&net.Dialer{}).DialContext(ctx, "tcp", c.currentServerAddress)
			if err != nil {
				c.logger.Error("Failed to establish TCP connection to the server", "error", err)
				return ierror.ErrCannotEstablishConnection
			}

			tc := connection.(*net.TCPConn)
			if err := tc.SetNoDelay(c.config.noDelay); err != nil {
				c.logger.Error("Failed to set the nodelay option on the client, continuing...", "error", err)
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
				c.logger.Error("Failed to establish a TLS connection to the server", "error", err)
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
	c.logger.Info("Iggy client has connected to the Iggy server", "client_address", c.clientAddress, "server_address", c.currentServerAddress)
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
		c.logger.Error("Failed to create a server name from the domain.", "error", ierror.ErrInvalidTlsDomain)
		return nil, ierror.ErrInvalidTlsDomain
	}
	tlsConfig.ServerName = serverName

	// Load CA certificate if provided
	if c.config.tls.tlsCAFile != "" {
		caCert, err := os.ReadFile(c.config.tls.tlsCAFile)
		if err != nil {
			c.logger.Error("Failed to read the CA file", "certificate_path", c.config.tls.tlsCAFile, "error", err)
			return nil, ierror.ErrInvalidTlsCertificatePath
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			c.logger.Error(
				"Failed to parse the CA certificate.",
				"certificate_path", c.config.tls.tlsCAFile,
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

	c.logger.Info("Iggy client is disconnecting from server...", "client_address", c.clientAddress)
	c.state = iggcon.StateDisconnected

	if err := c.conn.Close(); err != nil {
		return err
	}

	c.logger.Info("Iggy client has disconnected from server.", "client_address", c.clientAddress)
	// TODO event pushing logic
	return nil
}

func (c *IggyTcpClient) shutdown() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.state == iggcon.StateShutdown {
		return nil
	}

	c.logger.Info("Shutting down the Iggy TCP client...", "client_address", c.clientAddress)

	if err := c.conn.Close(); err != nil {
		return err
	}

	c.state = iggcon.StateShutdown
	c.logger.Info("Iggy TCP client has been shutdown.", "client_address", c.clientAddress)
	// TODO push shutdown event
	return nil
}

func (c *IggyTcpClient) Close() error {
	return c.shutdown()
}
