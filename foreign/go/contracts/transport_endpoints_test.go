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

// wantTransportEndpointsBytes is the little-endian layout of ports 8090,
// 8080, 3000 and 8092, worked out by hand from the port numbers:
// 8090 = 0x1f9a, 8080 = 0x1f90, 3000 = 0x0bb8, 8092 = 0x1f9c.
var wantTransportEndpointsBytes = []byte{
	0x9a, 0x1f, // TCP 8090
	0x90, 0x1f, // QUIC 8080
	0xb8, 0x0b, // HTTP 3000
	0x9c, 0x1f, // WebSocket 8092
}

func TestTransportEndpoints_MarshalBinaryPortOrder(t *testing.T) {
	endpoints := NewTransportEndpoints(8090, 8080, 3000, 8092)
	if got := endpoints.GetBufferSize(); got != 8 {
		t.Fatalf("GetBufferSize() = %d, want 8", got)
	}
	got, err := endpoints.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if !bytes.Equal(got, wantTransportEndpointsBytes) {
		t.Errorf("MarshalBinary() = % x, want % x", got, wantTransportEndpointsBytes)
	}
}

func TestTransportEndpoints_RoundTrip(t *testing.T) {
	want := NewTransportEndpoints(8090, 8080, 3000, 8092)
	body, err := want.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	var got TransportEndpoints
	if err := got.UnmarshalBinary(body); err != nil {
		t.Fatalf("UnmarshalBinary() error = %v", err)
	}
	if got != want {
		t.Errorf("UnmarshalBinary() = %+v, want %+v", got, want)
	}
}

func TestTransportEndpoints_UnmarshalBinaryLength(t *testing.T) {
	tooLong := append(append([]byte{}, wantTransportEndpointsBytes...), 0xff, 0xfe)
	tests := []struct {
		name    string
		input   []byte
		wantErr bool
		want    TransportEndpoints
	}{
		{"empty", nil, true, TransportEndpoints{}},
		{"seven bytes", make([]byte, 7), true, TransportEndpoints{}},
		{"extra bytes", tooLong, false, NewTransportEndpoints(8090, 8080, 3000, 8092)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := TransportEndpoints{Tcp: 1}
			got := original
			err := got.UnmarshalBinary(tt.input)
			if tt.wantErr {
				if !errors.Is(err, ierror.ErrInvalidCommand) {
					t.Fatalf("UnmarshalBinary() error = %v, want %v", err, ierror.ErrInvalidCommand)
				}
				if got != original {
					t.Errorf("UnmarshalBinary() changed the receiver on error: %+v, want %+v", got, original)
				}
				return
			}
			if err != nil {
				t.Fatalf("UnmarshalBinary() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("UnmarshalBinary() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
