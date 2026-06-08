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
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestSerializeIdentifier_StringId(t *testing.T) {
	// Test case for StringId
	identifier, _ := NewIdentifier("Hello")

	// Serialize the identifier
	serialized, err := identifier.MarshalBinary()
	if err != nil {
		t.Errorf("Error serializing identifier: %v", err)
	}

	// Expected serialized bytes for StringId
	expected := []byte{
		0x02,                         // Kind (StringId)
		0x05,                         // Length (5)
		0x48, 0x65, 0x6C, 0x6C, 0x6F, // Value ("Hello")
	}

	// Check if the serialized bytes match the expected bytes
	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect for StringId. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerializeIdentifier_NumericId(t *testing.T) {
	// Test case for NumericId
	identifier, _ := NewIdentifier(uint32(123))

	// Serialize the identifier
	serialized, err := identifier.MarshalBinary()
	if err != nil {
		t.Errorf("Error serializing identifier: %v", err)
	}

	// Expected serialized bytes for NumericId
	expected := []byte{
		0x01,                   // Kind (NumericId)
		0x04,                   // Length (4)
		0x7B, 0x00, 0x00, 0x00, // Value (123)
	}

	// Check if the serialized bytes match the expected bytes
	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect for NumericId. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerializeIdentifier_EmptyStringId(t *testing.T) {
	// Test case for an empty StringId
	_, err := NewIdentifier("")

	// Check if the serialized bytes match the expected bytes
	if !errors.Is(err, ierror.ErrInvalidIdentifier) {
		t.Errorf("Expected error: %v, got: %v", ierror.ErrInvalidIdentifier, err)
	}
}

// TestIdentifier_MarshalledSizeMatchesAppendBinary guards against drift
// between the size-computation and encode paths — they're implicitly
// coupled by the request encoder (PollMessages pre-sizes its buffer via
// MarshalledSize, then writes via AppendBinary).
func TestIdentifier_MarshalledSizeMatchesAppendBinary(t *testing.T) {
	mkLongString := func(n int) string {
		s := make([]byte, n)
		for i := range s {
			s[i] = 'a'
		}
		return string(s)
	}
	cases := []struct {
		name string
		make func() Identifier
	}{
		{"numeric", func() Identifier { id, _ := NewIdentifier(uint32(42)); return id }},
		{"string-short", func() Identifier { id, _ := NewIdentifier("hi"); return id }},
		{"string-long", func() Identifier { id, _ := NewIdentifier(mkLongString(200)); return id }},
		{"zero-value-numeric", func() Identifier {
			return Identifier{Kind: NumericId, Length: 4, Value: []byte{1, 2, 3, 4}}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := tc.make()
			out, err := id.AppendBinary(nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(out) != id.MarshalledSize() {
				t.Fatalf("AppendBinary wrote %d bytes, MarshalledSize=%d", len(out), id.MarshalledSize())
			}
		})
	}
}

// TestIdentifier_ValueMutationDoesNotCorruptEncoded guards that mutating
// the exported Value field after construction leaves the cached wire bytes
// intact.
func TestIdentifier_ValueMutationDoesNotCorruptEncoded(t *testing.T) {
	id, _ := NewIdentifier("hello")
	before, err := id.AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range id.Value {
		id.Value[i] ^= 0xFF
	}
	after, err := id.AppendBinary(nil)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("encoded bytes changed after mutating Value: before=%v after=%v", before, after)
	}
}
