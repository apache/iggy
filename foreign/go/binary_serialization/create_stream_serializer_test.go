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
	"reflect"
	"testing"
)

func TestSerialize_TcpCreateStreamRequest(t *testing.T) {
	// Create a sample TcpCreateStreamRequest
	request := TcpCreateStreamRequest{
		Name:     "test_stream",
	}

	// Serialize the request
	serialized := request.Serialize()

	// Expected serialized bytes
	expectedNameLength := byte(11) // Length of "test_stream"
	expectedPayload := []byte("test_stream")

	// Check the length of the serialized bytes
	if len(serialized) != int(payloadOffset+len(request.Name)) {
		t.Errorf("Serialized length is incorrect. Expected: %d, Got: %d", payloadOffset+len(request.Name), len(serialized))
	}

	// Check the NameLength field
	if serialized[nameLengthOffset] != expectedNameLength {
		t.Errorf("NameLength is incorrect. Expected: %d, Got: %d", expectedNameLength, serialized[nameLengthOffset])
	}

	// Check the Payload field
	if !reflect.DeepEqual(serialized[payloadOffset:], expectedPayload) {
		t.Errorf("Payload is incorrect. Expected: %v, Got: %v", expectedPayload, serialized[payloadOffset:])
	}
}
