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

package client

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

type Options struct {
	protocol   iggcon.Protocol
	tcpOptions []tcp.Option
	logger     *slog.Logger

	heartbeatInterval time.Duration
}

func GetDefaultOptions() Options {
	return Options{
		protocol:          iggcon.Tcp,
		tcpOptions:        nil,
		logger:            slog.New(slog.DiscardHandler),
		heartbeatInterval: 5 * time.Second,
	}
}

type Option func(*Options)

// WithTcp sets the client protocol to TCP and applies custom TCP options.
func WithTcp(tcpOpts ...tcp.Option) Option {
	return func(opts *Options) {
		opts.protocol = iggcon.Tcp
		opts.tcpOptions = tcpOpts
	}
}

// WithLogger sets the logger for the Iggy client and its underlying transport.
// This logger is used by the heartbeat and forwarded to the transport.
// When no logger is provided, all internal log output is silently discarded.
func WithLogger(logger *slog.Logger) Option {
	return func(opts *Options) {
		if logger == nil {
			return
		}
		opts.logger = logger
	}
}

type IggyClient struct{ iggcon.Client }

// NewIggyClient creates the IggyClient instance without connecting.
// Call Connect to establish the connection to the server.
// If no Option is provided, NewIggyClient will create a default TCP client.
func NewIggyClient(options ...Option) (iggcon.Client, error) {
	opts := GetDefaultOptions()

	for _, opt := range options {
		opt(&opts)
	}

	var cli iggcon.Client
	switch opts.protocol {
	case iggcon.Tcp:
		opts.tcpOptions = append([]tcp.Option{tcp.WithHeartbeatInterval(opts.heartbeatInterval)}, opts.tcpOptions...)
		cli = tcp.NewIggyTcpClient(opts.logger, opts.tcpOptions...)
	default:
		return nil, fmt.Errorf("unknown protocol type: %v", opts.protocol)
	}
	return &IggyClient{Client: cli}, nil
}
