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
	"encoding/binary"
	"errors"
	"fmt"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

const maxHeaderFieldLength = 255

type HeaderValue struct {
	Kind  HeaderKind
	Value []byte
}

type HeaderKey struct {
	Kind  HeaderKind
	Value []byte
}

type HeaderEntry struct {
	Key   HeaderKey
	Value HeaderValue
}

func NewHeaderKeyString(val string) (HeaderKey, error) {
	if len(val) == 0 || len(val) > maxHeaderFieldLength {
		return HeaderKey{}, fmt.Errorf("%w: length must be between 1 and %d", ierror.ErrInvalidHeaderKey, maxHeaderFieldLength)
	}
	return HeaderKey{Kind: String, Value: []byte(val)}, nil
}

func NewHeaderKeyRaw(val []byte) (HeaderKey, error) {
	if len(val) == 0 || len(val) > maxHeaderFieldLength {
		return HeaderKey{}, fmt.Errorf("%w: length must be between 1 and %d", ierror.ErrInvalidHeaderKey, maxHeaderFieldLength)
	}
	return HeaderKey{Kind: Raw, Value: val}, nil
}

func NewHeaderKeyInt32(val int32) HeaderKey {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, uint32(val))
	return HeaderKey{Kind: Int32, Value: buf}
}

type HeaderKind int

const (
	Raw     HeaderKind = 1
	String  HeaderKind = 2
	Bool    HeaderKind = 3
	Int8    HeaderKind = 4
	Int16   HeaderKind = 5
	Int32   HeaderKind = 6
	Int64   HeaderKind = 7
	Int128  HeaderKind = 8
	Uint8   HeaderKind = 9
	Uint16  HeaderKind = 10
	Uint32  HeaderKind = 11
	Uint64  HeaderKind = 12
	Uint128 HeaderKind = 13
	Float   HeaderKind = 14
	Double  HeaderKind = 15
)

func (k HeaderKind) ExpectedSize() int {
	switch k {
	case Bool, Int8, Uint8:
		return 1
	case Int16, Uint16:
		return 2
	case Int32, Uint32, Float:
		return 4
	case Int64, Uint64, Double:
		return 8
	case Int128, Uint128:
		return 16
	default:
		return -1
	}
}

// The defined range must move together with WireHeaderKind::KNOWN_MAX in
// core/binary_protocol/src/primitives/user_headers.rs.
func (k HeaderKind) valid() bool {
	return k >= Raw && k <= Double
}

// Out-of-byte kinds would alias a defined kind once truncated, so they encode as the reserved 0 that the Rust reader and the server reject.
func (k HeaderKind) wireByte() byte {
	if k < 0 || k > 255 {
		return 0
	}
	return byte(k)
}

func GetHeadersBytes(headers []HeaderEntry) []byte {
	headersLength := 0
	for _, entry := range headers {
		headersLength += 1 + 4 + len(entry.Key.Value) + 1 + 4 + len(entry.Value.Value)
	}
	headersBytes := make([]byte, headersLength)
	position := 0
	for _, entry := range headers {
		headerBytes := getBytesFromHeader(entry.Key, entry.Value)
		copy(headersBytes[position:position+len(headerBytes)], headerBytes)
		position += len(headerBytes)
	}
	return headersBytes
}

func getBytesFromHeader(key HeaderKey, value HeaderValue) []byte {
	headerBytesLength := 1 + 4 + len(key.Value) + 1 + 4 + len(value.Value)
	headerBytes := make([]byte, headerBytesLength)
	pos := 0

	headerBytes[pos] = key.Kind.wireByte()
	pos++

	binary.LittleEndian.PutUint32(headerBytes[pos:pos+4], uint32(len(key.Value)))
	pos += 4
	copy(headerBytes[pos:pos+len(key.Value)], key.Value)
	pos += len(key.Value)

	headerBytes[pos] = value.Kind.wireByte()
	pos++

	binary.LittleEndian.PutUint32(headerBytes[pos:pos+4], uint32(len(value.Value)))
	pos += 4
	copy(headerBytes[pos:], value.Value)

	return headerBytes
}

func DeserializeHeaders(userHeadersBytes []byte) ([]HeaderEntry, error) {
	var headers []HeaderEntry
	position := 0

	for position < len(userHeadersBytes) {
		keyKind, err := deserializeHeaderKind(userHeadersBytes, position)
		if err != nil {
			return nil, err
		}
		position++

		if len(userHeadersBytes) <= position+4 {
			return nil, errors.New("invalid header key length")
		}

		keyLength := binary.LittleEndian.Uint32(userHeadersBytes[position : position+4])
		if keyLength == 0 || 255 < keyLength {
			return nil, errors.New("key has incorrect size, must be between 1 and 255")
		}
		position += 4

		if len(userHeadersBytes) < position+int(keyLength) {
			return nil, errors.New("invalid header key")
		}

		if expected := keyKind.ExpectedSize(); expected != -1 && int(keyLength) != expected {
			return nil, errors.New("invalid header key size for kind")
		}

		keyValue := make([]byte, keyLength)
		copy(keyValue, userHeadersBytes[position:position+int(keyLength)])
		position += int(keyLength)

		valueKind, err := deserializeHeaderKind(userHeadersBytes, position)
		if err != nil {
			return nil, err
		}
		position++

		if len(userHeadersBytes) <= position+4 {
			return nil, errors.New("invalid header value length")
		}

		valueLength := binary.LittleEndian.Uint32(userHeadersBytes[position : position+4])
		position += 4

		if valueLength == 0 || 255 < valueLength {
			return nil, errors.New("value has incorrect size, must be between 1 and 255")
		}

		if len(userHeadersBytes) < position+int(valueLength) {
			return nil, errors.New("invalid header value")
		}

		if expected := valueKind.ExpectedSize(); expected != -1 && int(valueLength) != expected {
			return nil, errors.New("invalid header value size for kind")
		}

		valueBytes := make([]byte, valueLength)
		copy(valueBytes, userHeadersBytes[position:position+int(valueLength)])
		position += int(valueLength)

		headers = append(headers, HeaderEntry{
			Key:   HeaderKey{Kind: keyKind, Value: keyValue},
			Value: HeaderValue{Kind: valueKind, Value: valueBytes},
		})
	}

	return headers, nil
}

const (
	headerFieldTruncated        = "truncated"
	headerFieldUnknownKind      = "unknown kind"
	headerFieldLengthOutOfRange = "length out of range"
	headerFieldWrongWidth       = "wrong width for kind"
)

// validateUserHeaders rejects any field some reader refuses to decode.
func validateUserHeaders(userHeadersBytes []byte) error {
	position := 0
	entry := 0
	isKey := true
	for position < len(userHeadersBytes) {
		next, reason := headerFieldEnd(userHeadersBytes, position)
		if reason != "" {
			if isKey {
				return fmt.Errorf("header entry %d: %w: %s", entry, ierror.ErrInvalidHeaderKey, reason)
			}
			return fmt.Errorf("header entry %d: %w: %s", entry, ierror.ErrInvalidHeaderValue, reason)
		}
		position = next
		if !isKey {
			entry++
		}
		isKey = !isKey
	}
	if !isKey {
		return fmt.Errorf("header entry %d: %w: %s", entry, ierror.ErrInvalidHeaderValue, headerFieldTruncated)
	}
	return nil
}

// headerFieldEnd returns the position after the kind-length-data field at position, or the reason a reader would reject it.
func headerFieldEnd(userHeadersBytes []byte, position int) (int, string) {
	if len(userHeadersBytes)-position < 5 {
		return 0, headerFieldTruncated
	}
	field := userHeadersBytes[position : position+5]
	kind := HeaderKind(field[0])
	fieldLength := binary.LittleEndian.Uint32(field[1:5])
	position += 5
	if !kind.valid() {
		return 0, headerFieldUnknownKind
	}
	if fieldLength == 0 || fieldLength > maxHeaderFieldLength {
		return 0, headerFieldLengthOutOfRange
	}
	length := int(fieldLength)
	if len(userHeadersBytes)-position < length {
		return 0, headerFieldTruncated
	}
	if expected := kind.ExpectedSize(); expected != -1 && length != expected {
		return 0, headerFieldWrongWidth
	}
	return position + length, ""
}

func deserializeHeaderKind(payload []byte, position int) (HeaderKind, error) {
	if position >= len(payload) {
		return 0, errors.New("invalid header kind position")
	}

	return HeaderKind(payload[position]), nil
}
