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

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestGetClientMarshalBinary(t *testing.T) {
	tests := []struct {
		name     string
		clientID uint32
		want     []byte
	}{
		{"zero", 0, []byte{0, 0, 0, 0}},
		{"little endian", 0x01020304, []byte{4, 3, 2, 1}},
		{"maximum", ^uint32(0), []byte{255, 255, 255, 255}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := (&GetClient{ClientID: test.clientID}).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("GetClient body = %v, want %v", got, test.want)
			}
		})
	}
}

func TestDescribeOptionsMarshalBinary(t *testing.T) {
	for _, test := range []struct {
		name  string
		scope iggcon.OptionsScope
		want  byte
	}{
		{"topic", iggcon.OptionsScopeTopic, 1},
		{"stream", iggcon.OptionsScopeStream, 2},
		{"user", iggcon.OptionsScopeUser, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := (&DescribeOptions{Scope: test.scope}).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, []byte{test.want}) {
				t.Fatalf("DescribeOptions body = %v, want [%d]", got, test.want)
			}
		})
	}

	for _, scope := range []iggcon.OptionsScope{0, 4, 255} {
		t.Run(scope.String(), func(t *testing.T) {
			got, err := (&DescribeOptions{Scope: scope}).MarshalBinary()
			if err == nil {
				t.Fatalf("scope %d unexpectedly accepted: %v", scope, got)
			}
			if len(got) != 0 {
				t.Fatalf("scope %d returned a body on error: %v", scope, got)
			}
		})
	}
}

func TestEmptySystemCommandBodies(t *testing.T) {
	for _, test := range []struct {
		name    string
		command Command
	}{
		{"GetClients", &GetClients{}},
		{"GetClusterMetadata", &GetClusterMetadata{}},
		{"GetStats", &GetStats{}},
		{"Ping", &Ping{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.command.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 0 {
				t.Fatalf("%s body = %v, want empty", test.name, got)
			}
		})
	}
}
