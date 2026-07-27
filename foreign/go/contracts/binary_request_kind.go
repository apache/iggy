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

// BinaryRequestKind declares how a raw binary request executes on the server.
//
// Classic framing carries [length][code][payload] with no operation field, so
// this SDK encodes both kinds identically and the server decides how a command
// runs. The declaration exists so the API matches every other Iggy SDK and is
// already in place when the Go client gains VSR framing.
type BinaryRequestKind uint8

const (
	// BinaryRequestKindNonReplicated runs on the receiving node only, outside
	// consensus.
	BinaryRequestKindNonReplicated BinaryRequestKind = 1
	// BinaryRequestKindReplicated is replicated through consensus before it
	// takes effect.
	BinaryRequestKindReplicated BinaryRequestKind = 2
)

// String returns the cross-SDK name of the kind.
func (kind BinaryRequestKind) String() string {
	switch kind {
	case BinaryRequestKindNonReplicated:
		return "non_replicated"
	case BinaryRequestKindReplicated:
		return "replicated"
	default:
		return "unknown"
	}
}

// IsValid reports whether the kind is one the API defines. Go has no exhaustive
// enums, so a caller can hand over any byte.
func (kind BinaryRequestKind) IsValid() bool {
	return kind == BinaryRequestKindNonReplicated || kind == BinaryRequestKindReplicated
}
