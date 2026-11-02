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
	"bytes"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestWithLogger_Nil(t *testing.T) {
	cli, err := NewIggyClient(WithLogger(nil))
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}
	_ = cli
	opts := GetDefaultOptions()
	WithLogger(nil)(&opts)
	if opts.logger.Handler() != slog.DiscardHandler {
		t.Errorf("expected slog.DiscardHandler, got %v", opts.logger.Handler())
	}
}

func TestWithLogger_SetsClientLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	cli, err := NewIggyClient(WithLogger(logger))
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	_ = cli
	opts := GetDefaultOptions()
	WithLogger(logger)(&opts)
	opts.logger.Info("probe message", "key", "value")

	output := buf.String()
	if !strings.Contains(output, "probe message") {
		t.Errorf("expected logger output to contain 'probe message', got: %q", output)
	}
	if !strings.Contains(output, "key=value") {
		t.Errorf("expected logger output to contain 'key=value', got: %q", output)
	}
}

func TestNewIggyClient_HeartbeatInterval(t *testing.T) {
	const documentedDefault = 5 * time.Second
	const custom = 250 * time.Millisecond
	tests := []struct {
		name    string
		options []Option
		want    time.Duration
	}{
		{name: "default", want: documentedDefault},
		{name: "tcp option", options: []Option{WithTcp(tcp.WithHeartbeatInterval(custom))}, want: custom},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cli, err := NewIggyClient(test.options...)
			if err != nil {
				t.Fatalf("unexpected error creating client: %v", err)
			}
			if got := tcpHeartbeatInterval(t, cli); got != test.want {
				t.Errorf("expected heartbeat interval %v, got %v", test.want, got)
			}
		})
	}
}

// tcpHeartbeatInterval reads the interval through reflection, because the tcp
// package keeps its configuration unexported.
func tcpHeartbeatInterval(t *testing.T, cli iggcon.Client) time.Duration {
	t.Helper()
	wrapper, ok := cli.(*IggyClient)
	if !ok {
		t.Fatalf("expected *IggyClient, got %T", cli)
	}
	transport, ok := wrapper.Client.(*tcp.IggyTcpClient)
	if !ok {
		t.Fatalf("expected *tcp.IggyTcpClient, got %T", wrapper.Client)
	}
	interval := reflect.ValueOf(transport).Elem().FieldByName("config").FieldByName("heartbeatInterval")
	if !interval.IsValid() {
		t.Fatal("tcp client has no config.heartbeatInterval field")
	}
	return time.Duration(interval.Int())
}
