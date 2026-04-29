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
	"testing"
)

// TestSerialize_Ping tests serialization of Ping command
func TestSerialize_Ping(t *testing.T) {
	cmd := Ping{}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize Ping: %v", err)
	}

	expected := []byte{} // Empty byte array

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetStats tests serialization of GetStats command
func TestSerialize_GetStats(t *testing.T) {
	cmd := GetStats{}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetStats: %v", err)
	}

	expected := []byte{} // Empty byte array

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetClients tests serialization of GetClients command
func TestSerialize_GetClients(t *testing.T) {
	cmd := GetClients{}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetClients: %v", err)
	}

	expected := []byte{} // Empty byte array

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetClusterMetadata tests serialization of GetClusterMetadata command
func TestSerialize_GetClusterMetadata(t *testing.T) {
	cmd := GetClusterMetadata{}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetClusterMetadata: %v", err)
	}

	expected := []byte{} // Empty byte array

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetClient tests serialization of GetClient command with normal value
func TestSerialize_GetClient(t *testing.T) {
	cmd := GetClient{
		ClientID: 42,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetClient: %v", err)
	}

	expected := []byte{
		0x2A, 0x00, 0x00, 0x00, // ClientID = 42 (little endian uint32)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetClient_Zero tests serialization with zero ClientID (edge case)
func TestSerialize_GetClient_Zero(t *testing.T) {
	cmd := GetClient{
		ClientID: 0,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetClient with zero ClientID: %v", err)
	}

	expected := []byte{
		0x00, 0x00, 0x00, 0x00, // ClientID = 0 (little endian uint32)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetClient_MaxValue tests serialization with maximum uint32 value (edge case)
func TestSerialize_GetClient_MaxValue(t *testing.T) {
	cmd := GetClient{
		ClientID: 4294967295, // Max uint32 value
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetClient with max ClientID: %v", err)
	}

	expected := []byte{
		0xFF, 0xFF, 0xFF, 0xFF, // ClientID = 4294967295 (little endian uint32)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_GetClient_LargeValue tests serialization with a large ClientID value
func TestSerialize_GetClient_LargeValue(t *testing.T) {
	cmd := GetClient{
		ClientID: 16909060, // 0x01020304 in hex
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetClient with large ClientID: %v", err)
	}

	expected := []byte{
		0x04, 0x03, 0x02, 0x01, // ClientID = 16909060 (little endian: 0x01020304)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}
