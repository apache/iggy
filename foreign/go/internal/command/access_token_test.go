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
	tests := []struct {
		name   string
		expiry iggcon.Duration
		want   []byte
	}{
		{"server default", iggcon.IggyExpiryServerDefault, []byte{5, 't', 'o', 'k', 'e', 'n', 0, 0, 0, 0, 0, 0, 0, 0}},
		{"little endian", 0x01020304, []byte{5, 't', 'o', 'k', 'e', 'n', 4, 3, 2, 1, 0, 0, 0, 0}},
		{"past 32 bits", 0x0102030405060708, []byte{5, 't', 'o', 'k', 'e', 'n', 8, 7, 6, 5, 4, 3, 2, 1}},
		{"never expire", iggcon.IggyExpiryNeverExpire, []byte{5, 't', 'o', 'k', 'e', 'n', 255, 255, 255, 255, 255, 255, 255, 255}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := (&CreatePersonalAccessToken{Name: "token", Expiry: test.expiry}).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("CreatePersonalAccessToken body = % x, want % x", got, test.want)
			}
		})
	}
}
