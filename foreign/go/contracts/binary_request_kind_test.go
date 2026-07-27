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

import "testing"

func TestBinaryRequestKind_String(t *testing.T) {
	cases := []struct {
		kind BinaryRequestKind
		want string
	}{
		{BinaryRequestKindNonReplicated, "non_replicated"},
		{BinaryRequestKindReplicated, "replicated"},
		{BinaryRequestKind(0), "unknown"},
		{BinaryRequestKind(7), "unknown"},
	}
	for _, testCase := range cases {
		if got := testCase.kind.String(); got != testCase.want {
			t.Errorf("BinaryRequestKind(%d).String() = %q, want %q", testCase.kind, got, testCase.want)
		}
	}
}

func TestBinaryRequestKind_IsValid(t *testing.T) {
	cases := []struct {
		kind BinaryRequestKind
		want bool
	}{
		{BinaryRequestKindNonReplicated, true},
		{BinaryRequestKindReplicated, true},
		{BinaryRequestKind(0), false},
		{BinaryRequestKind(7), false},
	}
	for _, testCase := range cases {
		if got := testCase.kind.IsValid(); got != testCase.want {
			t.Errorf("BinaryRequestKind(%d).IsValid() = %v, want %v", testCase.kind, got, testCase.want)
		}
	}
}
