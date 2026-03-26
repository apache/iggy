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
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestGetDefaultOptions(t *testing.T) {
	opts := GetDefaultOptions()
	if opts.protocol != iggcon.Tcp {
		t.Fatalf("expected protocol Tcp, got %v", opts.protocol)
	}
	if opts.heartbeatInterval != 5*time.Second {
		t.Fatalf("expected heartbeat 5s, got %v", opts.heartbeatInterval)
	}
	if opts.tcpOptions != nil {
		t.Fatalf("expected nil tcpOptions")
	}
}

func TestWithTcp(t *testing.T) {
	opts := GetDefaultOptions()
	opts.protocol = "quic"
	addrOpt := tcp.WithServerAddress("1.2.3.4:9090")
	WithTcp(addrOpt)(&opts)
	if opts.protocol != iggcon.Tcp {
		t.Fatalf("expected protocol Tcp, got %v", opts.protocol)
	}
	if len(opts.tcpOptions) != 1 {
		t.Fatalf("expected 1 tcp option, got %d", len(opts.tcpOptions))
	}
}

func TestNewIggyClient_UnknownProtocol(t *testing.T) {
	_, err := NewIggyClient(func(opts *Options) {
		opts.protocol = "unknown"
	})
	if err == nil {
		t.Fatalf("expected error for unknown protocol")
	}
}
