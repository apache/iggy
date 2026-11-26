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

func TestConsumer_MarshalBinary(t *testing.T) {
	numericID, err := NewIdentifier(uint32(0x01020304))
	if err != nil {
		t.Fatal(err)
	}
	stringID, err := NewIdentifier("cg")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name     string
		consumer Consumer
		expected []byte
	}{
		{
			name:     "single consumer with numeric id",
			consumer: NewSingleConsumer(numericID),
			expected: []byte{
				0x01,                   // Consumer kind (single)
				0x01,                   // Identifier kind (NumericId)
				0x04,                   // Length (4)
				0x04, 0x03, 0x02, 0x01, // Value (0x01020304, little-endian)
			},
		},
		{
			name:     "group consumer with numeric id",
			consumer: NewGroupConsumer(numericID),
			expected: []byte{
				0x02,                   // Consumer kind (group)
				0x01,                   // Identifier kind (NumericId)
				0x04,                   // Length (4)
				0x04, 0x03, 0x02, 0x01, // Value (0x01020304, little-endian)
			},
		},
		{
			name:     "group consumer with string id",
			consumer: NewGroupConsumer(stringID),
			expected: []byte{
				0x02,       // Consumer kind (group)
				0x02,       // Identifier kind (StringId)
				0x02,       // Length (2)
				0x63, 0x67, // Value ("cg")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized, err := tt.consumer.MarshalBinary()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(serialized, tt.expected) {
				t.Fatalf("serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", tt.expected, serialized)
			}
		})
	}
}

func TestDefaultConsumer(t *testing.T) {
	consumer := DefaultConsumer()

	if consumer.Kind != ConsumerKindSingle {
		t.Fatalf("kind = %v, want %v", consumer.Kind, ConsumerKindSingle)
	}
	if consumer.Id.Kind() != NumericId {
		t.Fatalf("identifier kind = %v, want %v", consumer.Id.Kind(), NumericId)
	}
	id, err := consumer.Id.Uint32()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != 0 {
		t.Fatalf("identifier = %d, want 0", id)
	}
}
