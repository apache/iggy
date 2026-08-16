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
	"encoding/binary"
	"testing"
)

func TestCompressionAlgorithm_OptionValue(t *testing.T) {
	tests := []struct {
		algorithm CompressionAlgorithm
		want      string
		wantErr   bool
	}{
		{CompressionAlgorithmDefault, "", false},
		{CompressionAlgorithmNone, "", false},
		{CompressionAlgorithmGzip, "gzip", false},
		{CompressionAlgorithm(200), "", true},
	}
	for _, test := range tests {
		got, err := test.algorithm.OptionValue()
		if (err != nil) != test.wantErr {
			t.Errorf("CompressionAlgorithm(%d).OptionValue() error = %v, wantErr %v",
				uint8(test.algorithm), err, test.wantErr)
		}
		if got != test.want {
			t.Errorf("CompressionAlgorithm(%d).OptionValue() = %q, want %q",
				uint8(test.algorithm), got, test.want)
		}
	}
}

func TestCompressionAlgorithm_StringNamesUnknownCode(t *testing.T) {
	if got := CompressionAlgorithm(200).String(); got != "unknown(200)" {
		t.Errorf("String() = %q, want %q", got, "unknown(200)")
	}
}

// goldenOptionsBlock is the cross-SDK golden vector for an options block.
//
// Rust pins the identical bytes in core/binary_protocol/src/primitives/options.rs,
// as do the Node and Java SDKs. Round-tripping through this SDK's own decoder
// proves nothing about interoperability; these bytes are the contract.
var goldenOptionsBlock = []byte{
	2, 13, 0, 0, 0,
	'e', 'n', 'f', 'o', 'r', 'c', 'e', '_', 'f', 's', 'y', 'n', 'c',
	3, 1, 0, 0, 0, 1,
	2, 12, 0, 0, 0,
	's', 'e', 'g', 'm', 'e', 'n', 't', '_', 's', 'i', 'z', 'e',
	12, 8, 0, 0, 0,
	0, 0, 0, 64, 0, 0, 0, 0,
}

func TestGetHeadersBytes_MatchesTheCrossSdkGoldenVector(t *testing.T) {
	segmentSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(segmentSize, 1073741824)
	entries := []HeaderEntry{
		{
			Key:   HeaderKey{Kind: String, Value: []byte("enforce_fsync")},
			Value: HeaderValue{Kind: Bool, Value: []byte{1}},
		},
		{
			Key:   HeaderKey{Kind: String, Value: []byte("segment_size")},
			Value: HeaderValue{Kind: Uint64, Value: segmentSize},
		},
	}

	got := GetHeadersBytes(entries)

	if !bytes.Equal(got, goldenOptionsBlock) {
		t.Errorf("GetHeadersBytes() = %v, want %v", got, goldenOptionsBlock)
	}
}
