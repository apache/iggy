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

package ierror

import (
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestIggyError_Error(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		expected string
	}{
		{name: "without message", err: New(InvalidCredentials), expected: "code=42: invalid_credentials"},
		{name: "with message", err: New(InvalidCredentials, WithMsg("msg")), expected: "code=42: msg"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.err.Error(); got != c.expected {
				t.Errorf("Error() = %v, want %v", got, c.expected)
			}
		})
	}
}

func TestIggyError_Is(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		target   error
		expected bool
	}{
		{
			name:     "different code",
			err:      New(InvalidCredentials),
			target:   New(InvalidStreamId),
			expected: false,
		}, {
			name:     "same code, different message",
			err:      New(InvalidCredentials, WithMsg("msg1")),
			target:   New(InvalidCredentials, WithMsg("msg2")),
			expected: true,
		},
		{
			name:     "wrapped error same code",
			err:      fmt.Errorf("wrap: %w", New(InvalidCredentials)),
			target:   New(InvalidCredentials),
			expected: true,
		},
		{
			name:     "compare with nil",
			err:      New(InvalidCredentials),
			target:   nil,
			expected: false,
		},
		{
			name:     "compare with different type",
			err:      New(InvalidCredentials),
			target:   io.EOF,
			expected: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := errors.Is(c.err, c.target); got != c.expected {
				t.Errorf("errors.Is(%v, %v) = %v, want %v", c.err, c.target, got, c.expected)
			}
		})
	}
}

func TestCode(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		expected ErrCode
	}{
		{
			name:     "error",
			err:      New(InvalidCredentials),
			expected: InvalidCredentials,
		}, {
			name:     "wrapped error",
			err:      fmt.Errorf("wrapped error: %w", New(InvalidCredentials)),
			expected: InvalidCredentials,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Code(c.err); got != c.expected {
				t.Errorf("Code(%v) = %v, want %v", c.err, got, c.expected)
			}
		})
	}
}
