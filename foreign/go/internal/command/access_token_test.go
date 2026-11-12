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
	"strings"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func TestPersonalAccessTokenCommandsMarshalBinary(t *testing.T) {
	for _, test := range []struct {
		name    string
		request Command
		code    Code
		want    []byte
	}{
		{
			name:    "server default expiry",
			request: &CreatePersonalAccessToken{Name: "token", Expiry: iggcon.IggyExpiryServerDefault},
			code:    42,
			want:    []byte{5, 't', 'o', 'k', 'e', 'n', 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:    "little endian expiry",
			request: &CreatePersonalAccessToken{Name: "token", Expiry: 0x01020304},
			code:    42,
			want:    []byte{5, 't', 'o', 'k', 'e', 'n', 4, 3, 2, 1, 0, 0, 0, 0},
		},
		{
			name:    "expiry past 32 bits",
			request: &CreatePersonalAccessToken{Name: "token", Expiry: 0x0102030405060708},
			code:    42,
			want:    []byte{5, 't', 'o', 'k', 'e', 'n', 8, 7, 6, 5, 4, 3, 2, 1},
		},
		{
			name:    "never expire",
			request: &CreatePersonalAccessToken{Name: "token", Expiry: iggcon.IggyExpiryNeverExpire},
			code:    42,
			want:    []byte{5, 't', 'o', 'k', 'e', 'n', 255, 255, 255, 255, 255, 255, 255, 255},
		},
		{
			name:    "create token with one-character name",
			request: &CreatePersonalAccessToken{Name: "t", Expiry: 0x01020304},
			code:    42,
			want:    []byte{1, 't', 4, 3, 2, 1, 0, 0, 0, 0},
		},
		{
			name:    "token with 255-byte name",
			request: &CreatePersonalAccessToken{Name: strings.Repeat("t", 255), Expiry: iggcon.IggyExpiryServerDefault},
			code:    42,
			want:    append(append([]byte{255}, bytes.Repeat([]byte{'t'}, 255)...), 0, 0, 0, 0, 0, 0, 0, 0),
		},
		{
			name:    "get tokens with no request payload",
			request: &GetPersonalAccessTokens{},
			code:    41,
			want:    []byte{},
		},
		{
			name:    "short name",
			request: &DeletePersonalAccessToken{Name: "token"},
			code:    43,
			want:    []byte{5, 't', 'o', 'k', 'e', 'n'},
		},
		{
			name:    "delete token with one-character name",
			request: &DeletePersonalAccessToken{Name: "t"},
			code:    43,
			want:    []byte{1, 't'},
		},
		{
			name:    "delete token with 255-byte name",
			request: &DeletePersonalAccessToken{Name: strings.Repeat("t", 255)},
			code:    43,
			want:    append([]byte{255}, bytes.Repeat([]byte{'t'}, 255)...),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.request.Code(); got != test.code {
				t.Fatalf("command code = %d, want %d", got, test.code)
			}
			got, err := test.request.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("command body = %x, want %x", got, test.want)
			}
		})
	}
}
