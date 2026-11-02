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

import "strings"

type CreateStreamRequest struct {
	StreamId int    `json:"streamId"`
	Name     string `json:"name"`
}

type Stream struct {
	Id            uint32 `json:"id"`
	Name          string `json:"name"`
	SizeBytes     uint64 `json:"sizeBytes"`
	CreatedAt     uint64 `json:"createdAt"`
	MessagesCount uint64 `json:"messagesCount"`
	TopicsCount   uint32 `json:"topicsCount"`
	// Options are the keys the client set.
	Options map[string]HeaderValue `json:"options,omitempty"`
	// DerivedOptions are the values the server filled or computes, such as the
	// encryption key projections.
	DerivedOptions map[string]HeaderValue `json:"derivedOptions,omitempty"`
}

type StreamDetails struct {
	Stream
	Topics []Topic `json:"topics,omitempty"`
}

type GetStreamRequest struct {
	StreamID Identifier
}

// EncryptionKeySource reports who chose the stream's at-rest key, false when
// the server reported no source.
func (s Stream) EncryptionKeySource() (StreamEncryptionSource, bool) {
	source, ok := s.textOption(StreamOptionEncryptionKeySource)
	return StreamEncryptionSource(source), ok
}

// EncryptionKeyFingerprint reports the fingerprint of the stream's key, false
// for a plaintext stream. EncryptionKeyFingerprint of a key computes the same.
func (s Stream) EncryptionKeyFingerprint() (string, bool) {
	return s.textOption(StreamOptionEncryptionKeyFingerprint)
}

// EncryptionKekFingerprints reports the fingerprints of the server keys that
// wrap the stream's key: one outside a key rotation, none for a plaintext
// stream.
func (s Stream) EncryptionKekFingerprints() []string {
	joined, ok := s.textOption(StreamOptionEncryptionKekFingerprint)
	if !ok || joined == "" {
		return nil
	}
	return strings.Split(joined, ",")
}

func (s Stream) textOption(key StreamOptionKey) (string, bool) {
	for _, options := range []map[string]HeaderValue{s.DerivedOptions, s.Options} {
		if value, ok := options[string(key)]; ok && value.Kind == String {
			return string(value.Value), true
		}
	}
	return "", false
}
