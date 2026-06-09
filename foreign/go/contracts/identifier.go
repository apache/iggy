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

	ierror "github.com/apache/iggy/foreign/go/errors"
)

type Identifier struct {
	Kind   IdKind
	Length int
	Value  []byte
	// encoded caches the wire bytes; populated by NewIdentifier, empty for
	// zero-value Identifiers (marshalers fall back to Kind/Length/Value).
	encoded []byte
}

type IdKind uint8

const (
	NumericId IdKind = 1
	StringId  IdKind = 2
)

// NewIdentifier create a new identifier
func NewIdentifier[T uint32 | string](value T) (Identifier, error) {
	switch v := any(value).(type) {
	case uint32:
		return newNumericIdentifier(v)
	case string:
		return newStringIdentifier(v)
	}
	return Identifier{}, ierror.ErrInvalidIdentifier
}

// newNumericIdentifier creates a new identifier from the given numeric value.
func newNumericIdentifier(value uint32) (Identifier, error) {
	val := make([]byte, 4)
	binary.LittleEndian.PutUint32(val, value)
	encoded := []byte{byte(NumericId), 4, val[0], val[1], val[2], val[3]}
	return Identifier{
		Kind:    NumericId,
		Length:  4,
		Value:   val,
		encoded: encoded,
	}, nil
}

// NewStringIdentifier creates a new identifier from the given string value.
func newStringIdentifier(value string) (Identifier, error) {
	length := len(value)
	if length == 0 || length > 255 {
		return Identifier{}, ierror.ErrInvalidIdentifier
	}
	val := []byte(value)
	encoded := make([]byte, 2+length)
	encoded[0] = byte(StringId)
	encoded[1] = byte(length)
	copy(encoded[2:], val)
	return Identifier{
		Kind:    StringId,
		Length:  length,
		Value:   val,
		encoded: encoded,
	}, nil
}

// Uint32 returns the numeric value of the identifier.
func (id Identifier) Uint32() (uint32, error) {
	if id.Kind != NumericId || id.Length != 4 {
		return 0, ierror.ErrResourceNotFound
	}

	return binary.LittleEndian.Uint32(id.Value), nil
}

// String returns the string value of the identifier.
func (id Identifier) String() (string, error) {
	if id.Kind != StringId {
		return "", ierror.ErrInvalidIdentifier
	}

	return string(id.Value), nil
}

func (id Identifier) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, id.Length+2)
	bytes[0] = byte(id.Kind)
	bytes[1] = byte(id.Length)
	copy(bytes[2:], id.Value)
	return bytes, nil
}

func (id Identifier) AppendBinary(b []byte) ([]byte, error) {
	if len(id.encoded) > 0 {
		return append(b, id.encoded...), nil
	}
	b = append(b, byte(id.Kind), byte(id.Length))
	b = append(b, id.Value...)
	return b, nil
}

func MarshalIdentifiers(identifiers ...Identifier) ([]byte, error) {
	size := 0
	for i := 0; i < len(identifiers); i++ {
		size += 2 + identifiers[i].Length
	}
	bytes := make([]byte, 0, size)

	for i := 0; i < len(identifiers); i++ {
		var err error
		bytes, err = identifiers[i].AppendBinary(bytes)
		if err != nil {
			return nil, err
		}
	}

	return bytes, nil
}
