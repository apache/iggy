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

package util

import (
	"testing"

	"github.com/apache/iggy/foreign/go/contracts"
	"github.com/stretchr/testify/assert"
)

func TestIsSameAddress(t *testing.T) {
	cases := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{"identical", "127.0.0.1:8090", "127.0.0.1:8090", true},
		{"localhost", "localhost:8090", "127.0.0.1:8090", true},
		{"different port", "127.0.0.1:8090", "127.0.0.1:8091", false},
		{"different ip", "192.168.1.1:8090", "127.0.0.1:8090", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isSameAddress(tc.a, tc.b))
		})
	}
}

func TestNormalizeAddress(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"localhost", "localhost:8090", "127.0.0.1:8090"},
		{"uppercase localhost", "LOCALHOST:8090", "127.0.0.1:8090"},
		{"ipv6 any", "[::]:8090", "[::1]:8090"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, normalizeAddress(tc.in))
		})
	}
}

func TestLeaderRedirectionState(t *testing.T) {
	state := iggcon.NewLeaderRedirectionState()
	assert.True(t, state.CanRedirect())
	assert.Equal(t, uint8(0), state.RedirectCount)

	state.IncrementRedirect("127.0.0.1:8090")
	assert.True(t, state.CanRedirect())
	assert.Equal(t, uint8(1), state.RedirectCount)
	assert.Equal(t, "127.0.0.1:8090", state.LastLeaderAddress)

	state.IncrementRedirect("127.0.0.1:8091")
	state.IncrementRedirect("127.0.0.1:8092")
	assert.False(t, state.CanRedirect())
	assert.Equal(t, iggcon.MaxLeaderRedirects, state.RedirectCount)

	state.Reset()
	assert.True(t, state.CanRedirect())
	assert.Equal(t, uint8(0), state.RedirectCount)
}
