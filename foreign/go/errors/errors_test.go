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
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestIggyError_Error(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		expected string
	}{
		{name: "with field", err: ErrInvalidTopicId, expected: "invalid topic id"},
		{name: "without field", err: TopicIdNotFound{1, 1}, expected: "topic with id: 1 for stream with id: 1 was not found."},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.err.Error(); got != c.expected {
				t.Errorf("Error() = %v, want %v", got, c.expected)
			}
		})
	}
}

func TestIggyError_ConsensusErrors(t *testing.T) {
	cases := []struct {
		err      IggyError
		sentinel error
		code     Code
		message  string
	}{
		{
			err:      TransientNotCommitted{},
			sentinel: ErrTransientNotCommitted,
			code:     TransientNotCommittedCode,
			message:  "request transiently not committed; retry",
		},
		{
			err:      TransientNotAccepted{},
			sentinel: ErrTransientNotAccepted,
			code:     TransientNotAcceptedCode,
			message:  "request transiently not accepted; retry, on any replica",
		},
		{
			err:      ConsumerGroupPartitionNotOwned{ClientId: 4, PartitionId: 9},
			sentinel: ErrConsumerGroupPartitionNotOwned,
			code:     ConsumerGroupPartitionNotOwnedCode,
			message:  "consumer group member with client id: 4 does not own partition: 9 at the current generation (rebalance in progress).",
		},
		{
			err:      IncompatibleProtocolVersion{ClientVersion: 1, ServerVersionMin: 2, ServerVersionMax: 3},
			sentinel: ErrIncompatibleProtocolVersion,
			code:     IncompatibleProtocolVersionCode,
			message:  "incompatible binary protocol version: client 1, server accepts [2, 3]",
		},
	}
	for _, c := range cases {
		t.Run(c.message, func(t *testing.T) {
			if c.err.Code() != c.code {
				t.Errorf("Code() = %v, want %v", c.err.Code(), c.code)
			}
			if c.err.Error() != c.message {
				t.Errorf("Error() = %q, want %q", c.err.Error(), c.message)
			}
			if !errors.Is(c.err, c.sentinel) {
				t.Errorf("errors.Is(%v, sentinel) = false, want true", c.err)
			}
			if resolved := FromCode(c.code); resolved.Code() != c.code {
				t.Errorf("FromCode(%d).Code() = %v, want %v", c.code, resolved.Code(), c.code)
			}
		})
	}
}

func TestIggyError_TooManyConsumerOffsets(t *testing.T) {
	err := TooManyConsumerOffsets{}
	if err.Code() != TooManyConsumerOffsetsCode {
		t.Errorf("Code() = %v, want %v", err.Code(), TooManyConsumerOffsetsCode)
	}
	if err.Error() != "consumer offset limit reached for partition" {
		t.Errorf("Error() = %q", err.Error())
	}
	if !errors.Is(err, ErrTooManyConsumerOffsets) {
		t.Errorf("errors.Is(%v, ErrTooManyConsumerOffsets) = false", err)
	}
	if errors.Is(err, ErrConsumerOffsetNotFound) {
		t.Errorf("errors.Is(%v, ErrConsumerOffsetNotFound) = true", err)
	}
	if resolved := FromCode(TooManyConsumerOffsetsCode); !errors.Is(resolved, ErrTooManyConsumerOffsets) {
		t.Errorf("FromCode(%d) = %v", TooManyConsumerOffsetsCode, resolved)
	}
}

func TestFromCode_FallsBackToTheGenericError(t *testing.T) {
	if resolved := FromCode(Code(0xFFFFFF)); !errors.Is(resolved, ErrError) {
		t.Errorf("FromCode(unknown) = %v, want ErrError", resolved)
	}
}

// TestFromCode_ResolvesEveryDeclaredCode drives FromCode from errors.yaml,
// the same source errors_gen.go is generated from, so a case that resolves to
// the wrong error fails without hand-listing two hundred codes.
func TestFromCode_ResolvesEveryDeclaredCode(t *testing.T) {
	raw, err := os.ReadFile("errors.yaml")
	if err != nil {
		t.Fatalf("reading errors.yaml: %v", err)
	}
	var declared []struct {
		Name string `yaml:"name"`
		Code Code   `yaml:"code"`
	}
	if err := yaml.Unmarshal(raw, &declared); err != nil {
		t.Fatalf("parsing errors.yaml: %v", err)
	}
	if len(declared) < 200 {
		t.Fatalf("only %d declared errors, the yaml did not parse fully", len(declared))
	}

	for _, entry := range declared {
		resolved := FromCode(entry.Code)
		if resolved.Code() != entry.Code {
			t.Errorf("FromCode(%d) resolves to code %d (%s)",
				entry.Code, resolved.Code(), entry.Name)
		}
		if resolved.Error() == "" {
			t.Errorf("FromCode(%d) has an empty message (%s)", entry.Code, entry.Name)
		}
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
			err:      InvalidCredentials{},
			target:   ErrInvalidStreamId,
			expected: false,
		}, {
			name:     "same type, different field",
			err:      ResourceNotFound{"key1"},
			target:   ResourceNotFound{"key2"},
			expected: true,
		},
		{
			name:     "wrapped error with same type",
			err:      fmt.Errorf("wrap: %w", ErrInvalidCredentials),
			target:   ErrInvalidCredentials,
			expected: true,
		},
		{
			name:     "compare with nil",
			err:      ErrInvalidCredentials,
			target:   nil,
			expected: false,
		},
		{
			name:     "compare with different type",
			err:      ErrInvalidCredentials,
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
