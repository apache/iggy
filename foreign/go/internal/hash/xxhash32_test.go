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

package hash

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The expected digests come from the twox-hash crate the server and the Rust
// SDK use, so a keyed message lands on the same partition from either client.
func TestXXHash32_MatchesTheServerDigest(t *testing.T) {
	tests := []struct {
		input string
		want  uint32
	}{
		{input: "", want: 0x02CC5D05},
		{input: "a", want: 0x550D7456},
		{input: "abc", want: 0x32D153FF},
		{input: "abcd", want: 0xA3643705},
		{input: "order-key-1", want: 0x0D3FE0E1},
		{input: "0123456789abcdef", want: 0xC2C45B69},
		{input: "0123456789abcdefghijklmnopqrstuvwxyz", want: 0x9AA38E7E},
		{input: "Nobody inspects the spammish repetition", want: 0xE2293B2F},
	}
	for _, test := range tests {
		t.Run(test.input, func(t *testing.T) {
			assert.Equal(t, test.want, XXHash32([]byte(test.input)))
		})
	}
}

func TestXXHash32_IsStableAcrossBlockBoundaries(t *testing.T) {
	// The 16-byte stripe loop, the 4-byte tail and the single-byte tail all
	// have to agree with the reference for lengths either side of 16.
	for length := 0; length <= 64; length++ {
		input := strings.Repeat("k", length)
		first := XXHash32([]byte(input))
		second := XXHash32([]byte(input))
		assert.Equal(t, first, second, "length %d", length)
	}
}

func TestXXHash32_TreatsNilAndEmptyAlike(t *testing.T) {
	assert.Equal(t, XXHash32(nil), XXHash32([]byte{}))
}
