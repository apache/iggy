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

func TestCreatePersonalAccessTokenMarshalBinary(t *testing.T) {
	for _, test := range []struct {
		name    string
		request *CreatePersonalAccessToken
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
			name:    "empty name",
			request: &CreatePersonalAccessToken{Name: "", Expiry: 0x01020304},
			code:    42,
			want:    []byte{0, 4, 3, 2, 1, 0, 0, 0, 0},
		},
		{
			name:    "longer name",
			request: &CreatePersonalAccessToken{Name: "my-access-token", Expiry: iggcon.IggyExpiryServerDefault},
			code:    42,
			want:    []byte{15, 'm', 'y', '-', 'a', 'c', 'c', 'e', 's', 's', '-', 't', 'o', 'k', 'e', 'n', 0, 0, 0, 0, 0, 0, 0, 0},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.request.Code(); got != test.code {
				t.Fatalf("CreatePersonalAccessToken code = %d, want %d", got, test.code)
			}
			got, err := test.request.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("CreatePersonalAccessToken body = %x, want %x", got, test.want)
			}
		})
	}
}

func TestGetPersonalAccessTokensMarshalBinary(t *testing.T) {
	for _, test := range []struct {
		name    string
		request *GetPersonalAccessTokens
		code    Code
		want    []byte
	}{
		{
			name:    "empty body",
			request: &GetPersonalAccessTokens{},
			code:    41,
			want:    []byte{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.request.Code(); got != test.code {
				t.Fatalf("GetPersonalAccessTokens code = %d, want %d", got, test.code)
			}
			got, err := test.request.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("GetPersonalAccessTokens body = %x, want %x", got, test.want)
			}
		})
	}
}

func TestDeletePersonalAccessTokenMarshalBinary(t *testing.T) {
	for _, test := range []struct {
		name    string
		request *DeletePersonalAccessToken
		code    Code
		want    []byte
	}{
		{
			name:    "short name",
			request: &DeletePersonalAccessToken{Name: "token"},
			code:    43,
			want:    []byte{5, 't', 'o', 'k', 'e', 'n'},
		},
		{
			name:    "empty name",
			request: &DeletePersonalAccessToken{Name: ""},
			code:    43,
			want:    []byte{0},
		},
		{
			name:    "longer name",
			request: &DeletePersonalAccessToken{Name: "my-access-token"},
			code:    43,
			want:    []byte{15, 'm', 'y', '-', 'a', 'c', 'c', 'e', 's', 's', '-', 't', 'o', 'k', 'e', 'n'},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := test.request.Code(); got != test.code {
				t.Fatalf("DeletePersonalAccessToken code = %d, want %d", got, test.code)
			}
			got, err := test.request.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("DeletePersonalAccessToken body = %x, want %x", got, test.want)
			}
		})
	}
}
