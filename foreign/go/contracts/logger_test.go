// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package iggcon

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestNewLogger_WritesToProvidedWriter(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(slog.LevelInfo, &buf)

	logger.Info("hello world")

	if !strings.Contains(buf.String(), "hello world") {
		t.Errorf("expected output to contain 'hello world', got: %s", buf.String())
	}
}

func TestNewLogger_RespectsLogLevel(t *testing.T) {
	tests := []struct {
		name         string
		loggerLevel  slog.Level
		logAt        slog.Level
		expectOutput bool
	}{
		{"suppresses below level", slog.LevelInfo, slog.LevelDebug, false},
		{"passes at level", slog.LevelInfo, slog.LevelInfo, true},
		{"passes above level", slog.LevelInfo, slog.LevelWarn, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := NewLogger(tt.loggerLevel, &buf)

			logger.Log(context.Background(), tt.logAt, "test message")

			if (buf.Len() > 0) != tt.expectOutput {
				t.Errorf("expectOutput=%v, got output: %q", tt.expectOutput, buf.String())
			}
		})
	}
}

func TestNewStderrLogger_RespectsLevel(t *testing.T) {
	logger := NewStderrLogger(slog.LevelWarn)

	// Verify level filtering without capturing stderr
	if logger.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("info should be disabled on a warn-level logger")
	}
	if !logger.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("warn should be enabled on a warn-level logger")
	}
}

func TestNopLogger_DoesNotPanic(t *testing.T) {
	logger := NopLogger()

	if logger == nil {
		t.Fatal("expected non-nil logger")
	}

	logger.Debug("msg")
	logger.Info("msg")
	logger.Warn("msg")
	logger.Error("msg")
}
