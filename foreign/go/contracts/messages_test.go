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
	"bytes"
	"errors"
	"os"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

// The user-header wire format bounds every key and value to 1..=255 bytes and
// a fixed-size kind to its exact width. Every reader enforces that: this
// package's DeserializeHeaders, and the Rust SDK and server, which drop the
// whole header map of a message that breaks it. The server stores header bytes
// as sent (they may be client-side encrypted, so it cannot check them), so a
// message NewIggyMessage accepts must already be one every reader can decode.
func TestNewIggyMessage_HeadersItAcceptsAreDecodable(t *testing.T) {
	key := HeaderKey{Kind: String, Value: []byte("dedup-key")}
	tests := []struct {
		name  string
		entry HeaderEntry
		valid bool
	}{
		{"1-byte string value", HeaderEntry{key, HeaderValue{Kind: String, Value: []byte("a")}}, true},
		{"255-byte raw value", HeaderEntry{key, HeaderValue{Kind: Raw, Value: bytes.Repeat([]byte{7}, 255)}}, true},
		{"4-byte int32 value", HeaderEntry{key, HeaderValue{Kind: Int32, Value: []byte{1, 0, 0, 0}}}, true},
		{"empty string value", HeaderEntry{key, HeaderValue{Kind: String, Value: []byte{}}}, false},
		{"nil raw value", HeaderEntry{key, HeaderValue{Kind: Raw}}, false},
		{"256-byte raw value", HeaderEntry{key, HeaderValue{Kind: Raw, Value: bytes.Repeat([]byte{7}, 256)}}, false},
		{"3-byte int32 value", HeaderEntry{key, HeaderValue{Kind: Int32, Value: []byte{1, 0, 0}}}, false},
		{"empty key", HeaderEntry{HeaderKey{Kind: String}, HeaderValue{Kind: String, Value: []byte("a")}}, false},
		{"256-byte key", HeaderEntry{HeaderKey{Kind: Raw, Value: bytes.Repeat([]byte{7}, 256)}, HeaderValue{Kind: String, Value: []byte("a")}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message, err := NewIggyMessage([]byte("payload"), WithUserHeaders([]HeaderEntry{tt.entry}))
			if tt.valid {
				if err != nil {
					t.Fatalf("NewIggyMessage refused a well-formed header: %v", err)
				}
				decoded, err := DeserializeHeaders(message.UserHeaders)
				if err != nil || len(decoded) != 1 || !bytes.Equal(decoded[0].Value.Value, tt.entry.Value.Value) {
					t.Fatalf("header did not round-trip: %v %+v", err, decoded)
				}
				return
			}
			if os.Getenv("IGGY_RUN_KNOWN_FAILURES") == "" {
				t.Skip("#4471: NewIggyMessage does not validate header lengths yet; set IGGY_RUN_KNOWN_FAILURES=1 to run")
			}
			if err == nil {
				_, decodeErr := DeserializeHeaders(message.UserHeaders)
				t.Fatalf("NewIggyMessage accepted a header no reader can decode (DeserializeHeaders: %v)", decodeErr)
			}
			if !errors.Is(err, ierror.ErrInvalidHeaderValue) && !errors.Is(err, ierror.ErrInvalidHeaderKey) {
				t.Fatalf("want InvalidHeaderKey/InvalidHeaderValue, got %v", err)
			}
		})
	}
}
