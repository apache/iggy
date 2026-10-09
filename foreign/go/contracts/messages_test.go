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
	"strings"
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
		{"8-byte double value", HeaderEntry{key, HeaderValue{Kind: Double, Value: make([]byte, 8)}}, true},
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

func TestNewIggyMessage_ReportsWhichHeaderFieldIsInvalid(t *testing.T) {
	key := HeaderKey{Kind: String, Value: []byte("dedup-key")}
	value := HeaderValue{Kind: String, Value: []byte("a")}
	tests := []struct {
		name  string
		entry HeaderEntry
		want  error
	}{
		{"empty value", HeaderEntry{key, HeaderValue{Kind: String}}, ierror.ErrInvalidHeaderValue},
		{"3-byte int32 value", HeaderEntry{key, HeaderValue{Kind: Int32, Value: []byte{1, 0, 0}}}, ierror.ErrInvalidHeaderValue},
		{"zero value kind", HeaderEntry{key, HeaderValue{Value: []byte("a")}}, ierror.ErrInvalidHeaderValue},
		{"empty key", HeaderEntry{HeaderKey{Kind: String}, value}, ierror.ErrInvalidHeaderKey},
		{"2-byte uint32 key", HeaderEntry{HeaderKey{Kind: Uint32, Value: []byte{1, 0}}, value}, ierror.ErrInvalidHeaderKey},
		{"unknown key kind", HeaderEntry{HeaderKey{Kind: 16, Value: []byte("k")}, value}, ierror.ErrInvalidHeaderKey},
		{"kind 257 key", HeaderEntry{HeaderKey{Kind: 257, Value: []byte("k")}, value}, ierror.ErrInvalidHeaderKey},
		{"kind -1 value", HeaderEntry{key, HeaderValue{Kind: -1, Value: []byte("a")}}, ierror.ErrInvalidHeaderValue},
		{"kind -255 value", HeaderEntry{key, HeaderValue{Kind: -255, Value: []byte("a")}}, ierror.ErrInvalidHeaderValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewIggyMessage([]byte("payload"), WithUserHeaders([]HeaderEntry{tt.entry}))
			if !errors.Is(err, tt.want) {
				t.Fatalf("want %v, got %v", tt.want, err)
			}
		})
	}
}

func TestNewIggyMessage_RejectsMalformedRawHeaderBytes(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		want error
	}{
		{"short TLV", []byte{2, 1, 0}, ierror.ErrInvalidHeaderKey},
		{"4-byte tail", []byte{2, 1, 0, 0}, ierror.ErrInvalidHeaderKey},
		{"truncated data", []byte{2, 5, 0, 0, 0, 'k'}, ierror.ErrInvalidHeaderKey},
		{"key with no value", []byte{2, 1, 0, 0, 0, 'k'}, ierror.ErrInvalidHeaderValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewIggyMessage([]byte("payload"), func(m *IggyMessage) { m.UserHeaders = tt.raw })
			if !errors.Is(err, tt.want) {
				t.Fatalf("want %v, got %v", tt.want, err)
			}
		})
	}
}

func TestNewIggyMessage_NamesTheInvalidHeaderEntry(t *testing.T) {
	good := HeaderEntry{
		Key:   HeaderKey{Kind: String, Value: []byte("k")},
		Value: HeaderValue{Kind: String, Value: []byte("v")},
	}
	bad := HeaderEntry{Key: good.Key, Value: HeaderValue{Kind: String}}
	_, err := NewIggyMessage([]byte("payload"), WithUserHeaders([]HeaderEntry{good, good, bad}))
	if !errors.Is(err, ierror.ErrInvalidHeaderValue) {
		t.Fatalf("want %v, got %v", ierror.ErrInvalidHeaderValue, err)
	}
	if !strings.Contains(err.Error(), "header entry 2") {
		t.Fatalf("error %q does not name entry 2", err)
	}
	if !strings.Contains(err.Error(), headerFieldLengthOutOfRange) {
		t.Fatalf("error %q does not give the reason %q", err, headerFieldLengthOutOfRange)
	}
}

func TestNewHeaderKey_RejectsOutOfRangeLengthAsInvalidHeaderKey(t *testing.T) {
	if _, err := NewHeaderKeyString(""); !errors.Is(err, ierror.ErrInvalidHeaderKey) {
		t.Errorf("NewHeaderKeyString(\"\"): want %v, got %v", ierror.ErrInvalidHeaderKey, err)
	}
	if _, err := NewHeaderKeyRaw(make([]byte, maxHeaderFieldLength+1)); !errors.Is(err, ierror.ErrInvalidHeaderKey) {
		t.Errorf("NewHeaderKeyRaw(256 bytes): want %v, got %v", ierror.ErrInvalidHeaderKey, err)
	}
}
