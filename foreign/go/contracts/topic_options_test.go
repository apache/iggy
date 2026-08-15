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
	"testing"
)

// Keys and value bytes are spelled out rather than taken from the constants and
// binary.LittleEndian: an expectation built the same way as the code under test
// would agree with it even byte-swapped or misnamed. These literals are the wire
// contract each constructor is pinned to.
func TestTopicOptions_ConstructorsEncodeKeyAndValue(t *testing.T) {
	tests := []struct {
		name      string
		entry     HeaderEntry
		wantKey   string
		wantKind  HeaderKind
		wantValue []byte
	}{
		{
			name:      "segment size",
			entry:     SegmentSizeOption(1 << 30),
			wantKey:   "segment_size",
			wantKind:  Uint64,
			wantValue: []byte{0, 0, 0, 64, 0, 0, 0, 0},
		},
		{
			name:      "enforce fsync on",
			entry:     EnforceFsyncOption(true),
			wantKey:   "enforce_fsync",
			wantKind:  Bool,
			wantValue: []byte{1},
		},
		{
			name:      "enforce fsync off",
			entry:     EnforceFsyncOption(false),
			wantKey:   "enforce_fsync",
			wantKind:  Bool,
			wantValue: []byte{0},
		},
		{
			name:      "messages required to save",
			entry:     MessagesRequiredToSaveOption(1024),
			wantKey:   "messages_required_to_save",
			wantKind:  Uint32,
			wantValue: []byte{0, 4, 0, 0},
		},
		{
			name:      "size of messages required to save",
			entry:     SizeOfMessagesRequiredToSaveOption(1 << 20),
			wantKey:   "size_of_messages_required_to_save",
			wantKind:  Uint64,
			wantValue: []byte{0, 0, 16, 0, 0, 0, 0, 0},
		},
		{
			name:      "preallocate segments on",
			entry:     PreallocateSegmentsOption(true),
			wantKey:   "preallocate_segments",
			wantKind:  Bool,
			wantValue: []byte{1},
		},
		{
			name:      "preallocate segments off",
			entry:     PreallocateSegmentsOption(false),
			wantKey:   "preallocate_segments",
			wantKind:  Bool,
			wantValue: []byte{0},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.entry.Key.Kind != String {
				t.Errorf("key kind = %d, want String", test.entry.Key.Kind)
			}
			if got := string(test.entry.Key.Value); got != test.wantKey {
				t.Errorf("key = %q, want %q", got, test.wantKey)
			}
			if test.entry.Value.Kind != test.wantKind {
				t.Errorf("value kind = %d, want %d", test.entry.Value.Kind, test.wantKind)
			}
			if !bytes.Equal(test.entry.Value.Value, test.wantValue) {
				t.Errorf("value = %v, want %v", test.entry.Value.Value, test.wantValue)
			}
			if expected := test.wantKind.ExpectedSize(); len(test.entry.Value.Value) != expected {
				t.Errorf("value length = %d, want %d for kind %d",
					len(test.entry.Value.Value), expected, test.wantKind)
			}
		})
	}
}

func TestTopicOptions_ConstructorEntriesSurviveTheHeaderCodec(t *testing.T) {
	entries := []HeaderEntry{
		SegmentSizeOption(1 << 20),
		EnforceFsyncOption(true),
		MessagesRequiredToSaveOption(7),
		SizeOfMessagesRequiredToSaveOption(4096),
		PreallocateSegmentsOption(false),
	}

	decoded, err := DeserializeHeaders(GetHeadersBytes(entries))
	if err != nil {
		t.Fatalf("options block is not valid TLV: %v", err)
	}
	if len(decoded) != len(entries) {
		t.Fatalf("decoded %d entries, want %d", len(decoded), len(entries))
	}
	for index, entry := range entries {
		got := decoded[index]
		if !bytes.Equal(got.Key.Value, entry.Key.Value) || got.Key.Kind != entry.Key.Kind {
			t.Errorf("entry %d key = %+v, want %+v", index, got.Key, entry.Key)
		}
		if !bytes.Equal(got.Value.Value, entry.Value.Value) || got.Value.Kind != entry.Value.Kind {
			t.Errorf("entry %d value = %+v, want %+v", index, got.Value, entry.Value)
		}
	}
}
