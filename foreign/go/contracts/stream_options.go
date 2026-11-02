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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

// StreamOptionKey names a stream option of the server catalog.
type StreamOptionKey string

const (
	// StreamOptionName is the stream's name, set by CreateStream and changed
	// by UpdateStream.
	StreamOptionName StreamOptionKey = "name"
	// StreamOptionEncryptionKey is the stream's own data key: create-only and
	// write-only.
	StreamOptionEncryptionKey StreamOptionKey = "encryption_key"
	// StreamOptionEncryptionKeySource is read-only: who chose the stream's key.
	StreamOptionEncryptionKeySource StreamOptionKey = "encryption_key_source"
	// StreamOptionEncryptionKeyFingerprint is read-only: the fingerprint of the
	// stream's key.
	StreamOptionEncryptionKeyFingerprint StreamOptionKey = "encryption_key_fingerprint"
	// StreamOptionEncryptionKekFingerprint is read-only: the comma-joined
	// fingerprints of the server keys that wrap the stream's key.
	StreamOptionEncryptionKekFingerprint StreamOptionKey = "encryption_kek_fingerprint"
)

// StreamEncryptionKeyLength is the size of a stream data key in bytes.
const StreamEncryptionKeyLength = 32

// encryptionKeyFingerprintLabel prefixes the key in the fingerprint digest.
const encryptionKeyFingerprintLabel = "iggy/stream-key-fingerprint/v1"

// encryptionKeyFingerprintBytes is how much of the digest a fingerprint keeps.
const encryptionKeyFingerprintBytes = 8

// StreamEncryptionSource says who chose a stream's at-rest key.
type StreamEncryptionSource string

const (
	// StreamEncryptionSourceStream is a key the client sent at create.
	StreamEncryptionSourceStream StreamEncryptionSource = "stream"
	// StreamEncryptionSourceServer is a key the server generated at create.
	StreamEncryptionSourceServer StreamEncryptionSource = "server"
	// StreamEncryptionSourceNone is a plaintext stream.
	StreamEncryptionSourceNone StreamEncryptionSource = "none"
)

// StreamOptions are the options CreateStream may carry. A nil *StreamOptions
// selects the defaults.
//
// Formatting a StreamOptions with any fmt verb, or encoding it as JSON,
// redacts the key and every Raw value.
type StreamOptions struct {
	// EncryptionKey is the stream's own data key, exactly 32 random bytes.
	// Nil follows the server: it generates a key when its encryption is
	// enabled and leaves the stream plaintext otherwise. The key travels inside
	// the create request over the client's transport, so TLS is the caller's
	// job. The server stores the key wrapped and never returns it. Reads
	// report only its source and fingerprints. Never derive it from a
	// password: the fingerprint would then be an offline guessing oracle.
	EncryptionKey []byte
	// Raw carries keys newer than this build. A typed field wins on collision.
	Raw []HeaderEntry
}

// StreamUpdate is a patch of a stream's options. Absent keys keep their
// stored values.
type StreamUpdate struct {
	// Name renames the stream when set.
	Name *string
	// Reset names keys to return to their defaults. No stream key is
	// resettable, so a non-empty list is refused before anything is sent.
	Reset []StreamOptionKey
	// RawReset names keys to reset that this build may not know. The server
	// decides whether each one may be reset, and a patch that carries any raw
	// name or Options entry is checked by the server alone.
	RawReset []string
	// Options carries updatable keys newer than this build. A typed field wins
	// on collision.
	Options []HeaderEntry
}

// EncryptionKeyOption builds the encryption_key entry for a raw options list.
// StreamOptions.EncryptionKey is the typed way to set it.
func EncryptionKeyOption(key []byte) (HeaderEntry, error) {
	if len(key) != StreamEncryptionKeyLength {
		return HeaderEntry{}, ierror.ErrInvalidEncryptionKey
	}
	return HeaderEntry{
		Key:   HeaderKey{Kind: String, Value: []byte(StreamOptionEncryptionKey)},
		Value: HeaderValue{Kind: Raw, Value: key},
	}, nil
}

// DecodeEncryptionKey decodes a stream key from canonical standard base64
// with padding. Any other spelling, whitespace included, and any length but
// 32 bytes is refused, so one key has exactly one text form.
func DecodeEncryptionKey(encoded string) ([]byte, error) {
	key, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(key) != StreamEncryptionKeyLength || base64.StdEncoding.EncodeToString(key) != encoded {
		return nil, ierror.ErrInvalidEncryptionKey
	}
	return key, nil
}

// EncryptionKeyFingerprint computes the fingerprint a stream created with key
// reports, 16 lowercase hex characters, so a caller can check which key a
// stream uses without the server returning key bytes.
func EncryptionKeyFingerprint(key []byte) (string, error) {
	if len(key) != StreamEncryptionKeyLength {
		return "", ierror.ErrInvalidEncryptionKey
	}
	digest := sha256.New()
	digest.Write([]byte(encryptionKeyFingerprintLabel))
	digest.Write(key)
	return hex.EncodeToString(digest.Sum(nil)[:encryptionKeyFingerprintBytes]), nil
}
