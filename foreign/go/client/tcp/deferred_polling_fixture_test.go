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

package tcp

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The refused fetches of the deferred polling vectors that the Rust encoders
// generate: a raw fetch that the server would refuse fails before it is sent.
func TestFixture_RefusesEachRefusedFetchOfTheSharedVectors(t *testing.T) {
	data, err := os.ReadFile("../../contracts/testdata/deferred_polling.json")
	require.NoError(t, err)
	var fixture struct {
		Refused struct {
			Fetches []struct {
				Case    string `json:"case"`
				BodyHex string `json:"body_hex"`
			} `json:"fetches"`
		} `json:"refused"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.NotEmpty(t, fixture.Refused.Fetches)
	for _, vector := range fixture.Refused.Fetches {
		t.Run(vector.Case, func(t *testing.T) {
			body, err := hex.DecodeString(vector.BodyHex)
			require.NoError(t, err)

			_, err = parseFetch(body)

			require.Error(t, err)
		})
	}
}
