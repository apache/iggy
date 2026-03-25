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
	"encoding/binary"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestSerialize_LoginUser_ContainsVersion(t *testing.T) {
	request := LoginUser{
		Username: "iggy",
		Password: "iggy",
	}

	serialized, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("Failed to serialize LoginUser: %v", err)
	}

	// Skip past username (1-byte len + "iggy") and password (1-byte len + "iggy")
	pos := 1 + len("iggy") + 1 + len("iggy")

	// Read version length (u32 LE)
	versionLen := binary.LittleEndian.Uint32(serialized[pos : pos+4])
	pos += 4

	// Read version string
	version := string(serialized[pos : pos+int(versionLen)])

	if version != iggcon.Version {
		t.Errorf("Version mismatch. Expected: %q, Got: %q", iggcon.Version, version)
	}
}
