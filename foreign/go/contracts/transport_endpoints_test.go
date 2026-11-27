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
	"reflect"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

func TestTransportEndpoints_MatchLittleEndian(t *testing.T) {
	tsep := NewTransportEndpoints(8090, 8080, 3000, 8092)
	got, err := tsep.MarshalBinary()
	if err != nil {
		t.Fatalf("TransportEndpoints.MarshalBinary() error = %v", err)
	}
	want := []byte{0x9A, 0x1F, 0x90, 0x1F, 0xB8, 0x0B, 0x9C, 0x1F}
	if !bytes.Equal(got, want) {
		t.Errorf("TransportEndpoints.MatchLittleEndian() = %v, want %v", got, want)
	}
}

func TestTransportEndpoints_UnmarshalBinaryLengthShouldNotBeSmallerThanEight(t *testing.T) {
	for _, length := range []int{0, 7} {
		var endpoints TransportEndpoints
		err := endpoints.UnmarshalBinary(make([]byte, length))
		if !errors.Is(err, ierror.ErrInvalidCommand) {
			t.Errorf("UnmarshalBinary() for %d-byte input returned %v, want %v", length, err, ierror.ErrInvalidCommand)
		}
	}
}

func TestTransportEndpoints_UnmarshalBinaryAlwaysDecodesTheFirstEightBytes(t *testing.T) {
	want := NewTransportEndpoints(8090, 8080, 3000, 8092)
	input, err := want.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	input = append(input, 0xCA, 0xFE, 0xBA, 0xBE) // appending four extra bytes to the input slice, which should be ignored by UnmarshalBinary
	got := NewTransportEndpoints(0, 0, 0, 0)
	err = got.UnmarshalBinary(input)
	if err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("UnmarshalBinary() = %v, want %v", got, want)
	}
}

func TestTransportEndpoints_RoundTrip(t *testing.T) {
	original := NewTransportEndpoints(8090, 8080, 3000, 8092)
	if original.GetBufferSize() != 8 {
		t.Errorf("GetBufferSize() = %d, want %d", original.GetBufferSize(), 8)
	}

	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	// data is anticipated to be 8 bytes long, so we can check that here
	if len(data) != 8 {
		t.Errorf("MarshalBinary() returned %d bytes, want 8", len(data))
	}

	restored := NewTransportEndpoints(0, 0, 0, 0)
	err = restored.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}
	if !reflect.DeepEqual(original, restored) {
		t.Errorf("UnmarshalBinary() = %v, want %v", restored, original)
	}
}
