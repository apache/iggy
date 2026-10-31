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
	"errors"
	"testing"
	"time"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestDefaultDeferredPollOptions_MatchTheRustDefaults(t *testing.T) {
	options := DefaultDeferredPollOptions()

	want := DeferredPollOptions{MaxWait: time.Second, MinCount: 1, MaxBytes: 1 << 20, RequestTimeout: 11 * time.Second}
	if options != want {
		t.Fatalf("defaults = %+v, want %+v", options, want)
	}
	if err := options.Validate(1); err != nil {
		t.Fatalf("the defaults must be valid for a poll of one message: %v", err)
	}
}

// The server reads the smallest byte limit as the reply prefix, one batch
// header and one message header, 344 bytes.
func TestMinDeferredPollMaxBytes_MatchesTheServer(t *testing.T) {
	if MinDeferredPollMaxBytes != 344 {
		t.Fatalf("MinDeferredPollMaxBytes = %d, want 344", MinDeferredPollMaxBytes)
	}
}

func TestDeferredPollOptions_ValidateRefusesWhatTheServerRefuses(t *testing.T) {
	valid := DefaultDeferredPollOptions()
	with := func(change func(*DeferredPollOptions)) DeferredPollOptions {
		options := valid
		change(&options)
		return options
	}
	tests := []struct {
		name    string
		options DeferredPollOptions
		count   uint32
		want    error
	}{
		{"min count equal to the count", with(func(o *DeferredPollOptions) { o.MinCount = 10 }), 10, nil},
		{"zero min count", with(func(o *DeferredPollOptions) { o.MinCount = 0 }), 10, ierror.ErrInvalidMessagesCount},
		{"min count over the count", with(func(o *DeferredPollOptions) { o.MinCount = 11 }), 10, ierror.ErrInvalidMessagesCount},
		{"longest wait and timeout", with(func(o *DeferredPollOptions) {
			o.MaxWait, o.RequestTimeout = MaxDeferredPollWait, MaxDeferredPollTimeout
		}), 1, nil},
		{"wait over the longest", with(func(o *DeferredPollOptions) {
			o.MaxWait, o.RequestTimeout = MaxDeferredPollWait+time.Microsecond, MaxDeferredPollTimeout
		}), 1, ierror.ErrInvalidCommand},
		{"negative wait", with(func(o *DeferredPollOptions) { o.MaxWait = -time.Microsecond }), 1, ierror.ErrInvalidCommand},
		{"timeout under one microsecond", with(func(o *DeferredPollOptions) {
			o.MaxWait, o.RequestTimeout = 0, time.Microsecond-time.Nanosecond
		}), 1, ierror.ErrInvalidCommand},
		{"timeout over the longest", with(func(o *DeferredPollOptions) {
			o.RequestTimeout = MaxDeferredPollTimeout + time.Microsecond
		}), 1, ierror.ErrInvalidCommand},
		{"zero wait without a margin", with(func(o *DeferredPollOptions) {
			o.MaxWait, o.RequestTimeout = 0, time.Microsecond
		}), 1, nil},
		{"exact reply margin", with(func(o *DeferredPollOptions) {
			o.MaxWait, o.RequestTimeout = time.Second, 2*time.Second
		}), 1, nil},
		{"short reply margin", with(func(o *DeferredPollOptions) {
			o.MaxWait, o.RequestTimeout = time.Second, 2*time.Second-time.Microsecond
		}), 1, ierror.ErrInvalidCommand},
		// The wire carries microseconds, so the margin holds on what is sent.
		{"margin on the sent microseconds", with(func(o *DeferredPollOptions) {
			o.MaxWait, o.RequestTimeout = time.Second+999*time.Nanosecond, 2*time.Second
		}), 1, nil},
		{"byte limit at the least", with(func(o *DeferredPollOptions) { o.MaxBytes = MinDeferredPollMaxBytes }), 1, nil},
		{"byte limit under the least", with(func(o *DeferredPollOptions) { o.MaxBytes = MinDeferredPollMaxBytes - 1 }), 1, ierror.ErrInvalidSizeBytes},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.options.Validate(tt.count)
			if tt.want == nil && err != nil {
				t.Fatalf("Validate refused valid options: %v", err)
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("Validate = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDeferredPollOptions_RemainingSharesOneBudget(t *testing.T) {
	options := DeferredPollOptions{MaxWait: time.Second, MinCount: 1, MaxBytes: 1 << 20, RequestTimeout: 11 * time.Second}

	partly, err := options.Remaining(300 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if partly.MaxWait != 700*time.Millisecond || partly.RequestTimeout != 10700*time.Millisecond {
		t.Fatalf("after 300 ms: wait %v, timeout %v, want 700ms and 10.7s", partly.MaxWait, partly.RequestTimeout)
	}
	if partly.MinCount != options.MinCount || partly.MaxBytes != options.MaxBytes {
		t.Fatalf("Remaining must keep the count and byte limits, got %+v", partly)
	}
	if err := partly.ValidateWait(); err != nil {
		t.Fatalf("Remaining must keep the reply margin: %v", err)
	}

	waited, err := options.Remaining(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if waited.MaxWait != 0 || waited.RequestTimeout != 9*time.Second {
		t.Fatalf("after the full wait: wait %v, timeout %v, want 0 and 9s", waited.MaxWait, waited.RequestTimeout)
	}

	last, err := options.Remaining(11*time.Second - time.Microsecond)
	if err != nil || last.RequestTimeout != time.Microsecond {
		t.Fatalf("one microsecond before the deadline: %+v, %v", last, err)
	}
	if _, err := options.Remaining(11 * time.Second); !errors.Is(err, ierror.ErrTransientNotAccepted) {
		t.Fatalf("a spent budget = %v, want %v", err, ierror.ErrTransientNotAccepted)
	}
}

func TestDeferredPollOptions_ValidateFetchKeepsRoomForLocalSetup(t *testing.T) {
	for _, tt := range []struct {
		name    string
		wait    time.Duration
		timeout time.Duration
		want    error
	}{
		{"below the reply margin", 0, time.Second - time.Microsecond, ierror.ErrInvalidCommand},
		{"exactly the reply margin", 0, time.Second, ierror.ErrInvalidCommand},
		{"fraction rounded down to the reply margin", 0, time.Second + 999*time.Nanosecond, ierror.ErrInvalidCommand},
		{"one wire microsecond above the reply margin", 0, time.Second + time.Microsecond, nil},
		{"positive wait without its reply margin", time.Second, 1500 * time.Millisecond, ierror.ErrInvalidCommand},
		{"positive wait with its reply margin", time.Second, 2 * time.Second, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			options := DefaultDeferredPollOptions()
			options.MaxWait, options.RequestTimeout = tt.wait, tt.timeout
			if err := options.ValidateFetch(1); !errors.Is(err, tt.want) {
				t.Fatalf("ValidateFetch = %v, want %v", err, tt.want)
			}
			if tt.wait == 0 {
				if err := options.Validate(1); err != nil {
					t.Fatalf("an immediate deferred poll must keep accepting this timeout: %v", err)
				}
				if err := options.ValidateWait(); err != nil {
					t.Fatalf("the shared wait validation must keep accepting this timeout: %v", err)
				}
			}
		})
	}
}

// Valid limits that are not whole microseconds keep the reply margin that the
// wire carries after any elapsed time, so a later step of the call is not
// refused.
func TestDeferredPollOptions_RemainingKeepsTheReplyMarginOfTheSentMicroseconds(t *testing.T) {
	options := DeferredPollOptions{MaxWait: time.Second + 999*time.Nanosecond, MinCount: 1, MaxBytes: 1 << 20, RequestTimeout: 2 * time.Second}
	if err := options.Validate(1); err != nil {
		t.Fatalf("the options must be valid: %v", err)
	}

	for _, elapsed := range []time.Duration{0, time.Nanosecond, 500 * time.Nanosecond, time.Microsecond, 1500 * time.Nanosecond, time.Millisecond} {
		remaining, err := options.Remaining(elapsed)
		if err != nil {
			t.Fatalf("after %v: %v", elapsed, err)
		}
		if err := remaining.ValidateWait(); err != nil {
			t.Fatalf("after %v: wait %v and timeout %v lost the reply margin: %v",
				elapsed, remaining.MaxWait, remaining.RequestTimeout, err)
		}
		if spent := options.RequestTimeout - remaining.RequestTimeout; spent < elapsed || spent >= elapsed+time.Microsecond {
			t.Fatalf("after %v: the budget shrank by %v, want %v rounded up to whole microseconds", elapsed, spent, elapsed)
		}
	}
}
