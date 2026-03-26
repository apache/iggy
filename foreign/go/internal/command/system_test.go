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

package command

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestGetClient_MarshalBinary(t *testing.T) {
	request := GetClient{
		ClientID: 42,
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	expected := make([]byte, 4)
	binary.LittleEndian.PutUint32(expected, 42)

	if !bytes.Equal(serialized, expected) {
		t.Fatalf("unexpected bytes.\nexpected:\t%v\ngot:\t\t%v", expected, serialized)
	}
}

func TestGetClients_MarshalBinary(t *testing.T) {
	request := GetClients{}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(serialized) != 0 {
		t.Fatalf("expected empty bytes, got %v", serialized)
	}
}

func TestGetClusterMetadata_MarshalBinary(t *testing.T) {
	request := GetClusterMetadata{}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(serialized) != 0 {
		t.Fatalf("expected empty bytes, got %v", serialized)
	}
}

func TestGetStats_MarshalBinary(t *testing.T) {
	request := GetStats{}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(serialized) != 0 {
		t.Fatalf("expected empty bytes, got %v", serialized)
	}
}

func TestPing_MarshalBinary(t *testing.T) {
	request := Ping{}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	if len(serialized) != 0 {
		t.Fatalf("expected empty bytes, got %v", serialized)
	}
}
