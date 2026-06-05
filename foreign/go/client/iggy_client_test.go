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
	"strings"
	"testing"
)

func TestWithLogger_Nil(t *testing.T) {
	cli, err := NewIggyClient(WithLogger(nil))
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}
	ic := cli.(*IggyClient)
	if ic.logger.Handler() != slog.DiscardHandler {
		t.Errorf("expected slog.DiscardHandler, get %v", ic.logger.Handler())
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

	ic := cli.(*IggyClient)

	ic.logger.Info("probe message", "key", "value")

	output := buf.String()
	if !strings.Contains(output, "probe message") {
		t.Errorf("expected logger output to contain 'probe message', got: %q", output)
	}
	if !strings.Contains(output, "key=value") {
		t.Errorf("expected logger output to contain 'key=value', got: %q", output)
	}
}
