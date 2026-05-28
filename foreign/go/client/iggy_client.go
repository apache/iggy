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
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

type Options struct {
	protocol   iggcon.Protocol
	tcpOptions []tcp.Option

	heartbeatInterval time.Duration
}

func GetDefaultOptions() Options {
	return Options{
		protocol:          iggcon.Tcp,
		tcpOptions:        nil,
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

type IggyClient struct {
	iggcon.Client
	cancel             context.CancelFunc
	wg                 sync.WaitGroup
	heartbeatInterval  time.Duration
	heartbeatTriggered atomic.Bool
}

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
		cli = tcp.NewIggyTcpClient(opts.tcpOptions...)
	default:
		return nil, fmt.Errorf("unknown protocol type: %v", opts.protocol)
	}
	ic := &IggyClient{
		Client:            cli,
		cancel:            func() {},
		heartbeatInterval: opts.heartbeatInterval,
	}
	return ic, nil
}

// Connect establishes the connection to the server and starts the heartbeat loop if not started.
func (ic *IggyClient) Connect(ctx context.Context) error {
	if err := ic.Client.Connect(ctx); err != nil {
		return err
	}
	if ic.heartbeatTriggered.Swap(true) {
		return nil
	}
	if ic.heartbeatInterval > 0 {
		lifetimeCtx, cancel := context.WithCancel(context.Background())
		ic.cancel = cancel
		ic.wg.Add(1)
		go func() {
			defer ic.wg.Done()
			ticker := time.NewTicker(ic.heartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-lifetimeCtx.Done():
					return
				case <-ticker.C:
					pingCtx, pingCancel := context.WithTimeout(lifetimeCtx, ic.heartbeatInterval/2)
					if err := ic.Ping(pingCtx); err != nil {
						log.Printf("[WARN] heartbeat failed: %v", err)
					}
					pingCancel()
				}
			}
		}()
	}
	return nil
}

func (ic *IggyClient) Close() error {
	ic.cancel()
	ic.wg.Wait()
	return ic.Client.Close()
}
