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
	"context"
	"strings"
	"testing"

	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetireGeoOrigin_SendsTheOriginAsAMetadataMutation(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationRetireGeoOrigin, resultSection())
	})

	require.NoError(t, client.RetireGeoOrigin(context.Background(), "dc-a"))

	reads := server.recorded()
	require.Len(t, reads, 1)
	assert.Equal(t, vsr.OperationRetireGeoOrigin, reads[0].operation())
	assert.Equal(t, []byte{4, 'd', 'c', '-', 'a'}, reads[0].payload)
}

func TestRetireGeoOrigin_RejectsAnInvalidOriginWithoutWriting(t *testing.T) {
	client, serverConn := newPipeClient(t)
	server := serve(serverConn, func(_ int, _ request) []byte {
		return replyFrame(vsr.OperationRetireGeoOrigin, resultSection())
	})

	for _, origin := range []string{"", strings.Repeat("o", 256)} {
		assert.ErrorIs(t, client.RetireGeoOrigin(context.Background(), origin), ierror.ErrInvalidFormat)
	}
	assert.Empty(t, server.recorded())
}
