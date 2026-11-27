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

func TestClusterNodeStatus_TryFrom(t *testing.T) {
	tests := []struct {
		input   byte
		want    ClusterNodeStatus
		wantErr error
	}{
		{0, Healthy, nil},
		{1, Starting, nil},
		{2, Stopping, nil},
		{3, Unreachable, nil},
		{4, Maintenance, nil},
		// Unknown exists as a constant but is not a valid wire value.
		{5, 0, ierror.ErrInvalidCommand},
		{255, 0, ierror.ErrInvalidCommand},
	}
	for _, test := range tests {
		got, err := TryFrom(test.input)
		if !errors.Is(err, test.wantErr) {
			t.Errorf("TryFrom(%d) error = %v, want %v", test.input, err, test.wantErr)
		}
		if got != test.want {
			t.Errorf("TryFrom(%d) = %d, want %d", test.input, got, test.want)
		}
	}
}

func TestClusterNodeStatus_BinaryRoundTrip(t *testing.T) {
	for _, status := range []ClusterNodeStatus{Healthy, Starting, Stopping, Unreachable, Maintenance} {
		b, err := status.MarshalBinary()
		if err != nil {
			t.Fatalf("ClusterNodeStatus(%d).MarshalBinary() error = %v", status, err)
		}
		if want := []byte{byte(status)}; !bytes.Equal(b, want) {
			t.Errorf("ClusterNodeStatus(%d).MarshalBinary() = %v, want %v", status, b, want)
		}

		var got ClusterNodeStatus
		if err := got.UnmarshalBinary(b); err != nil {
			t.Fatalf("UnmarshalBinary(%v) error = %v", b, err)
		}
		if got != status {
			t.Errorf("UnmarshalBinary(%v) = %d, want %d", b, got, status)
		}
	}
}

func TestClusterNodeStatus_UnmarshalBinaryRejectsInvalidInput(t *testing.T) {
	for _, input := range [][]byte{{}, {byte(Unknown)}} {
		got := Maintenance
		if err := got.UnmarshalBinary(input); !errors.Is(err, ierror.ErrInvalidCommand) {
			t.Errorf("UnmarshalBinary(%v) error = %v, want %v", input, err, ierror.ErrInvalidCommand)
		}
		if got != Maintenance {
			t.Errorf("UnmarshalBinary(%v) changed the status to %d", input, got)
		}
	}
}

func TestClusterNodeStatus_String(t *testing.T) {
	tests := []struct {
		status ClusterNodeStatus
		want   string
	}{
		{Healthy, "healthy"},
		{Starting, "starting"},
		{Stopping, "stopping"},
		{Unreachable, "unreachable"},
		{Maintenance, "maintenance"},
		{Unknown, "unknown"},
		{ClusterNodeStatus(255), "unknown"},
	}
	for _, test := range tests {
		if got := test.status.String(); got != test.want {
			t.Errorf("ClusterNodeStatus(%d).String() = %q, want %q", uint8(test.status), got, test.want)
		}
	}
}
