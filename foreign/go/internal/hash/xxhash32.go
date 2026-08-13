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

// Package hash provides the hash the SDK shares with the server for
// key-based partition selection.
package hash

import (
	"encoding/binary"
	"math/bits"
)

// XXH32 round primes.
const (
	prime1 uint32 = 2654435761
	prime2 uint32 = 2246822519
	prime3 uint32 = 3266489917
	prime4 uint32 = 668265263
	prime5 uint32 = 374761393
)

// XXHash32 returns the seedless XXH32 digest of data.
//
// A message key must land on the same partition whichever SDK produced it, so
// this matches core/common/src/utils/hash.rs, which the server and every other
// Iggy client use to place keyed messages.
func XXHash32(data []byte) uint32 {
	var accumulator uint32
	remaining := data

	if len(data) >= 16 {
		// The lane seeds wrap, so they are computed in uint32 variables
		// rather than as constant expressions.
		var lane1, lane4 uint32 = prime1, 0
		lane1 += prime2
		lane4 -= prime1
		lane2 := prime2
		lane3 := uint32(0)

		for len(remaining) >= 16 {
			lane1 = round(lane1, binary.LittleEndian.Uint32(remaining[0:4]))
			lane2 = round(lane2, binary.LittleEndian.Uint32(remaining[4:8]))
			lane3 = round(lane3, binary.LittleEndian.Uint32(remaining[8:12]))
			lane4 = round(lane4, binary.LittleEndian.Uint32(remaining[12:16]))
			remaining = remaining[16:]
		}

		accumulator = bits.RotateLeft32(lane1, 1) +
			bits.RotateLeft32(lane2, 7) +
			bits.RotateLeft32(lane3, 12) +
			bits.RotateLeft32(lane4, 18)
	} else {
		accumulator = prime5
	}

	accumulator += uint32(len(data))

	for len(remaining) >= 4 {
		accumulator += binary.LittleEndian.Uint32(remaining[0:4]) * prime3
		accumulator = bits.RotateLeft32(accumulator, 17) * prime4
		remaining = remaining[4:]
	}
	for _, value := range remaining {
		accumulator += uint32(value) * prime5
		accumulator = bits.RotateLeft32(accumulator, 11) * prime1
	}

	return avalanche(accumulator)
}

func round(accumulator, lane uint32) uint32 {
	return bits.RotateLeft32(accumulator+lane*prime2, 13) * prime1
}

func avalanche(accumulator uint32) uint32 {
	accumulator ^= accumulator >> 15
	accumulator *= prime2
	accumulator ^= accumulator >> 13
	accumulator *= prime3
	accumulator ^= accumulator >> 16
	return accumulator
}
