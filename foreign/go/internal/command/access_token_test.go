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

// TestSerialize_GetPersonalAccessTokens tests serialization of GetPersonalAccessTokens command
func TestSerialize_GetPersonalAccessTokens(t *testing.T) {
	cmd := GetPersonalAccessTokens{}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize GetPersonalAccessTokens: %v", err)
	}

	expected := []byte{} // Empty byte array

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeletePersonalAccessToken tests serialization with normal token name
func TestSerialize_DeletePersonalAccessToken(t *testing.T) {
	cmd := DeletePersonalAccessToken{
		Name: "test_token",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeletePersonalAccessToken: %v", err)
	}

	expected := []byte{
		0x0A,                                                       // Name length = 10
		0x74, 0x65, 0x73, 0x74, 0x5F, 0x74, 0x6F, 0x6B, 0x65, 0x6E, // "test_token"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeletePersonalAccessToken_SingleChar tests edge case with single character name
func TestSerialize_DeletePersonalAccessToken_SingleChar(t *testing.T) {
	cmd := DeletePersonalAccessToken{
		Name: "a",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeletePersonalAccessToken with single char: %v", err)
	}

	expected := []byte{
		0x01, // Name length = 1
		0x61, // "a"
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_DeletePersonalAccessToken_EmptyName tests edge case with empty name
func TestSerialize_DeletePersonalAccessToken_EmptyName(t *testing.T) {
	cmd := DeletePersonalAccessToken{
		Name: "",
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize DeletePersonalAccessToken with empty name: %v", err)
	}

	expected := []byte{
		0x00, // Name length = 0
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreatePersonalAccessToken tests serialization with normal values
func TestSerialize_CreatePersonalAccessToken(t *testing.T) {
	cmd := CreatePersonalAccessToken{
		Name:   "test",
		Expiry: 3600, // 1 hour in seconds
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreatePersonalAccessToken: %v", err)
	}

	expected := []byte{
		0x04,                   // Name length = 4
		0x74, 0x65, 0x73, 0x74, // "test"
		0x00, 0x00, 0x00, 0x00, // Expiry high bytes (protocol uses u64, Go writes u32 at end)
		0x10, 0x0E, 0x00, 0x00, // Expiry low bytes = 3600
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreatePersonalAccessToken_ZeroExpiry tests edge case with zero expiry
func TestSerialize_CreatePersonalAccessToken_ZeroExpiry(t *testing.T) {
	cmd := CreatePersonalAccessToken{
		Name:   "token",
		Expiry: 0,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreatePersonalAccessToken with zero expiry: %v", err)
	}

	expected := []byte{
		0x05,                         // Name length = 5
		0x74, 0x6F, 0x6B, 0x65, 0x6E, // "token"
		0x00, 0x00, 0x00, 0x00, // Expiry high bytes (protocol uses u64, Go writes u32 at end)
		0x00, 0x00, 0x00, 0x00, // Expiry = 0 (little endian uint32)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreatePersonalAccessToken_MaxExpiry tests edge case with maximum uint32 expiry
func TestSerialize_CreatePersonalAccessToken_MaxExpiry(t *testing.T) {
	cmd := CreatePersonalAccessToken{
		Name:   "long_token",
		Expiry: 4294967295, // Max uint32 value
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreatePersonalAccessToken with max expiry: %v", err)
	}

	expected := []byte{
		0x0A,                                                       // Name length = 10
		0x6C, 0x6F, 0x6E, 0x67, 0x5F, 0x74, 0x6F, 0x6B, 0x65, 0x6E, // "long_token"
		0x00, 0x00, 0x00, 0x00, // Expiry high bytes (protocol uses u64, Go writes u32 at end)
		0xFF, 0xFF, 0xFF, 0xFF, // Expiry = 4294967295 (little endian uint32)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreatePersonalAccessToken_EmptyName tests edge case with empty name
func TestSerialize_CreatePersonalAccessToken_EmptyName(t *testing.T) {
	cmd := CreatePersonalAccessToken{
		Name:   "",
		Expiry: 1000,
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreatePersonalAccessToken with empty name: %v", err)
	}

	expected := []byte{
		0x00,                   // Name length = 0
		0x00, 0x00, 0x00, 0x00, // Expiry high bytes (protocol uses u64, Go writes u32 at end)
		0xE8, 0x03, 0x00, 0x00, // Expiry = 1000 (little endian uint32)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}

// TestSerialize_CreatePersonalAccessToken_LongName tests with a longer token name
func TestSerialize_CreatePersonalAccessToken_LongName(t *testing.T) {
	cmd := CreatePersonalAccessToken{
		Name:   "my_very_long_personal_access_token_name",
		Expiry: 86400, // 24 hours in seconds
	}

	serialized, err := cmd.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize CreatePersonalAccessToken with long name: %v", err)
	}

	expected := []byte{
		0x27, // Name length = 39
		// "my_very_long_personal_access_token_name"
		0x6D, 0x79, 0x5F, 0x76, 0x65, 0x72, 0x79, 0x5F,
		0x6C, 0x6F, 0x6E, 0x67, 0x5F, 0x70, 0x65, 0x72,
		0x73, 0x6F, 0x6E, 0x61, 0x6C, 0x5F, 0x61, 0x63,
		0x63, 0x65, 0x73, 0x73, 0x5F, 0x74, 0x6F, 0x6B,
		0x65, 0x6E, 0x5F, 0x6E, 0x61, 0x6D, 0x65,
		0x00, 0x00, 0x00, 0x00, // Expiry high bytes (protocol uses u64, Go writes u32 at end)
		0x80, 0x51, 0x01, 0x00, // Expiry = 86400 (little endian uint32)
	}

	if !bytes.Equal(serialized, expected) {
		t.Errorf("Serialized bytes are incorrect.\nExpected:\t%v\nGot:\t\t%v", expected, serialized)
	}
}
