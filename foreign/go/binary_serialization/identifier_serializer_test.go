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

package binaryserialization

import (
	"errors"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestSerializeIdentifier_StringId(t *testing.T) {
	// Test case for StringId
	identifier, _ := iggcon.NewIdentifier("Hello")

	// Serialize the identifier
	serialized := SerializeIdentifier(identifier)

	// Expected serialized bytes for StringId
	expected := []byte{
		0x02,                         // Kind (StringId)
		0x05,                         // Length (5)
		0x48, 0x65, 0x6C, 0x6C, 0x6F, // Value ("Hello")
	}

	// Check if the serialized bytes match the expected bytes
	if !areBytesEqual(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect for StringId. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerializeIdentifier_NumericId(t *testing.T) {
	// Test case for NumericId
	identifier, _ := iggcon.NewIdentifier(uint32(123))

	// Serialize the identifier
	serialized := SerializeIdentifier(identifier)

	// Expected serialized bytes for NumericId
	expected := []byte{
		0x01,                   // Kind (NumericId)
		0x04,                   // Length (4)
		0x7B, 0x00, 0x00, 0x00, // Value (123)
	}

	// Check if the serialized bytes match the expected bytes
	if !areBytesEqual(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect for NumericId. \nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

func TestSerializeIdentifier_EmptyStringId(t *testing.T) {
	// Test case for an empty StringId
	_, err := iggcon.NewIdentifier("")

	// Check if the serialized bytes match the expected bytes
	if !errors.Is(err, ierror.InvalidIdentifier) {
		t.Errorf("Expected error: %v, got: %v", ierror.InvalidIdentifier, err)
	}
}
