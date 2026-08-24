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
	"encoding/binary"
	"math"
	"testing"
)

func TestClusterMetadataUnmarshalBinaryRejectsOversizedName(t *testing.T) {
	payload := make([]byte, 8)
	binary.LittleEndian.PutUint32(payload, math.MaxUint32)

	var metadata ClusterMetadata
	if err := metadata.UnmarshalBinary(payload); err == nil {
		t.Fatal("expected an error for an oversized cluster name")
	}
}

func TestClusterNodeUnmarshalBinaryRejectsOversizedLengths(t *testing.T) {
	tests := map[string][]byte{
		"name": func() []byte {
			payload := make([]byte, 10)
			binary.LittleEndian.PutUint32(payload, math.MaxUint32)
			return payload
		}(),
		"IP": func() []byte {
			payload := make([]byte, 14)
			binary.LittleEndian.PutUint32(payload[4:], math.MaxUint32)
			return payload
		}(),
	}

	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			var node ClusterNode
			if err := node.UnmarshalBinary(payload); err == nil {
				t.Fatal("expected an error for an oversized field")
			}
		})
	}
}
