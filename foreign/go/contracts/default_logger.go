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

package iggcon

import (
	"fmt"
	"io"
	"log/slog"
	"os"
)

type DefaultLogger struct {
	logger *slog.Logger
}

func NewDefaultLogger(level LogLevel, writer io.Writer) *DefaultLogger {
	handler := slog.NewTextHandler(writer, &slog.HandlerOptions{
		Level: toSlogLevel(level),
	})
	return &DefaultLogger{
		logger: slog.New(handler),
	}
}

func NewStderrLogger(level LogLevel) *DefaultLogger {
	return NewDefaultLogger(level, os.Stderr)
}

func (l *DefaultLogger) Debug(msg string, args ...any) {
	l.logger.Debug(fmt.Sprintf(msg, args...))
}

func (l *DefaultLogger) Info(msg string, args ...any) {
	l.logger.Info(fmt.Sprintf(msg, args...))
}

func (l *DefaultLogger) Warn(msg string, args ...any) {
	l.logger.Warn(fmt.Sprintf(msg, args...))
}

func (l *DefaultLogger) Error(msg string, args ...any) {
	l.logger.Error(fmt.Sprintf(msg, args...))
}

type NoopLogger struct{}

func (*NoopLogger) Debug(string, ...any) {}
func (*NoopLogger) Info(string, ...any)  {}
func (*NoopLogger) Warn(string, ...any)  {}
func (*NoopLogger) Error(string, ...any) {}

func NewNoopLogger() Logger {
	return &NoopLogger{}
}

func toSlogLevel(level LogLevel) slog.Level {
	switch level {
	case LogLevelDebug:
		return slog.LevelDebug
	case LogLevelInfo:
		return slog.LevelInfo
	case LogLevelWarn:
		return slog.LevelWarn
	case LogLevelError:
		return slog.LevelError
	default:
		return slog.LevelError + 1
	}
}
