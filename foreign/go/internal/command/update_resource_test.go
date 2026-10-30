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

package command

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestUpdateResource_RawPatches(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	topicId, _ := iggcon.NewIdentifier(uint32(2))

	t.Run("given_raw_set_key_and_create_only_reset_when_patching_should_send_it_for_the_server_to_decide", func(t *testing.T) {
		update := &UpdateTopic{
			StreamId: streamId,
			TopicId:  topicId,
			Reset:    []iggcon.TopicOptionKey{iggcon.TopicOptionDurability},
			Options:  []iggcon.HeaderEntry{rawEntry("retention_class", "cold")},
		}
		body, err := update.MarshalBinary()
		if err != nil {
			t.Fatalf("raw patch refused locally: %v", err)
		}
		if !bytes.Contains(body, []byte("\x0adurability")) || !bytes.Contains(body, []byte("retention_class")) {
			t.Fatalf("body lacks the reset or the raw key: %x", body)
		}
	})

	t.Run("given_raw_reset_when_patching_should_send_it_for_the_server_to_decide", func(t *testing.T) {
		update := &UpdateStream{StreamId: streamId, RawReset: []string{"retention_class", "retention_class"}}
		body, err := update.MarshalBinary()
		if err != nil {
			t.Fatalf("raw reset refused locally: %v", err)
		}
		want := append([]byte{byte(iggcon.OptionsScopeStream)}, mustIdentifier(t, streamId)...)
		want = append(want, 1, byte(len("retention_class")))
		want = append(want, "retention_class"...)
		want = append(want, iggcon.GetHeadersBytes(nil)...)
		if !bytes.Equal(body, want) {
			t.Fatalf("body %x, want %x", body, want)
		}
	})

	t.Run("given_raw_reset_also_set_when_patching_should_refuse_invalid_command", func(t *testing.T) {
		update := &UpdateTopic{
			StreamId: streamId,
			TopicId:  topicId,
			RawReset: []string{"retention_class"},
			Options:  []iggcon.HeaderEntry{rawEntry("retention_class", "cold")},
		}
		_, err := update.MarshalBinary()
		if !errors.Is(err, ierror.ErrInvalidCommand) {
			t.Fatalf("got %v, want invalid command", err)
		}
	})

	t.Run("given_more_resets_than_the_wire_carries_when_patching_should_refuse_unsupported_key", func(t *testing.T) {
		reset := make([]string, 0, 256)
		for index := range 256 {
			reset = append(reset, fmt.Sprintf("future_%03d", index))
		}
		_, err := (&UpdateStream{StreamId: streamId, RawReset: reset}).MarshalBinary()
		if !errors.Is(err, ierror.ErrUnsupportedOptionKey) {
			t.Fatalf("got %v, want unsupported option key", err)
		}
	})

	t.Run("given_reset_name_the_wire_cannot_carry_when_patching_should_refuse_with_a_bounded_preview", func(t *testing.T) {
		for _, name := range []string{"", strings.Repeat("k", 256)} {
			_, err := (&UpdateStream{StreamId: streamId, RawReset: []string{name}}).MarshalBinary()
			var unsupported ierror.UnsupportedOptionKey
			if !errors.As(err, &unsupported) || len(unsupported.Key) > keyPreviewLength {
				t.Fatalf("name of %d bytes: got %v", len(name), err)
			}
		}
	})
}

func TestUpdateResource_OptionsBlockBounds(t *testing.T) {
	t.Run("given_block_at_the_byte_limit_when_encoding_should_accept_it_and_refuse_one_byte_more", func(t *testing.T) {
		// 200 entries of 10 framing bytes, a 245-byte key and a 245-byte value.
		entries := make([]iggcon.HeaderEntry, 0, 200)
		for index := range 200 {
			key := fmt.Sprintf("%03d", index) + strings.Repeat("k", 242)
			entries = append(entries, rawEntry(key, strings.Repeat("v", 245)))
		}
		if _, err := encodeOptions(entries); err != nil {
			t.Fatalf("block of exactly %d bytes refused: %v", maxOptionsBytes, err)
		}
		entries[0].Value.Value = append(entries[0].Value.Value, 'v')
		if _, err := encodeOptions(entries); !errors.Is(err, ierror.ErrOptionsBlockTooLarge) {
			t.Fatalf("got %v, want options block too large", err)
		}
	})

	t.Run("given_more_entries_than_the_limit_when_encoding_should_refuse_block_too_large", func(t *testing.T) {
		entries := make([]iggcon.HeaderEntry, 0, maxOptions+1)
		for index := range maxOptions + 1 {
			entries = append(entries, rawEntry(fmt.Sprintf("key_%04d", index), "v"))
		}
		if _, err := encodeOptions(entries[:maxOptions]); err != nil {
			t.Fatalf("block of %d entries refused: %v", maxOptions, err)
		}
		if _, err := encodeOptions(entries); !errors.Is(err, ierror.ErrOptionsBlockTooLarge) {
			t.Fatalf("got %v, want options block too large", err)
		}
	})

	t.Run("given_malformed_entry_when_encoding_should_refuse_with_the_server_category", func(t *testing.T) {
		cases := []struct {
			name  string
			entry iggcon.HeaderEntry
			want  error
		}{
			{"empty_key", rawEntry("", "v"), ierror.ErrUnsupportedOptionKey},
			{"key_over_255_bytes", rawEntry(strings.Repeat("k", 256), "v"), ierror.ErrUnsupportedOptionKey},
			{"key_not_utf8", rawEntry("\xff\xfe", "v"), ierror.ErrUnsupportedOptionKey},
			{"empty_value", rawEntry("future", ""), ierror.ErrInvalidOptionValue},
			{"value_over_255_bytes", rawEntry("future", strings.Repeat("v", 256)), ierror.ErrInvalidOptionValue},
			{"fixed_kind_with_wrong_size", iggcon.HeaderEntry{
				Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("future")},
				Value: iggcon.HeaderValue{Kind: iggcon.Uint64, Value: []byte{1, 2, 3, 4}},
			}, ierror.ErrInvalidOptionValue},
		}
		for _, testCase := range cases {
			_, err := encodeOptions([]iggcon.HeaderEntry{testCase.entry})
			if !errors.Is(err, testCase.want) {
				t.Errorf("%s: got %v, want %v", testCase.name, err, testCase.want)
			}
			var unsupported ierror.UnsupportedOptionKey
			if errors.As(err, &unsupported) && len(unsupported.Key) > keyPreviewLength {
				t.Errorf("%s: error repeats %d bytes of the key", testCase.name, len(unsupported.Key))
			}
		}
	})

	t.Run("given_raw_entries_with_the_same_key_when_encoding_should_refuse_invalid_command", func(t *testing.T) {
		_, err := encodeOptions([]iggcon.HeaderEntry{rawEntry("future", "a"), rawEntry("future", "b")})
		if !errors.Is(err, ierror.ErrInvalidCommand) {
			t.Fatalf("got %v, want invalid command", err)
		}
	})
}

func rawEntry(key, value string) iggcon.HeaderEntry {
	return iggcon.HeaderEntry{
		Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(key)},
		Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
	}
}

func mustIdentifier(t *testing.T, identifier iggcon.Identifier) []byte {
	t.Helper()
	encoded, err := identifier.AppendBinary(nil)
	if err != nil {
		t.Fatalf("identifier: %v", err)
	}
	return encoded
}
