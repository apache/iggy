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

package vsr

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The cancellations of the shared deferred polling vectors: a header without a
// body that names the client, session and request of the cancelled request.
func TestFixture_BuildsEachCancellationOfTheSharedVectors(t *testing.T) {
	data, err := os.ReadFile("../../contracts/testdata/deferred_polling.json")
	require.NoError(t, err)
	var fixture struct {
		Cancellations []struct {
			Case        string `json:"case"`
			CancelsCode uint32 `json:"cancels_code"`
			Client      uint64 `json:"client"`
			Request     uint64 `json:"request"`
			Session     uint64 `json:"session"`
			HeaderHex   string `json:"header_hex"`
		} `json:"cancellations"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.NotEmpty(t, fixture.Cancellations)
	for _, vector := range fixture.Cancellations {
		t.Run(vector.Case, func(t *testing.T) {
			request := make([]byte, HeaderSize)
			EncodeRequestHeader((*[HeaderSize]byte)(request), RequestFields{
				Size:              HeaderSize,
				Client:            ClientID{Lo: vector.Client},
				Request:           vector.Request,
				Operation:         OperationNonReplicated,
				Session:           vector.Session,
				NonReplicatedCode: vector.CancelsCode,
			})

			cancellation, deferred := DeferredCancellation(request)

			require.True(t, deferred)
			require.Equal(t, vector.HeaderHex, hex.EncodeToString(cancellation))
		})
	}
}
