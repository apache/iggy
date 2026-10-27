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

package tests_test

import (
	"context"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	tokenLifetime       = 2 * time.Second
	tokenExpiryBudget   = 15 * time.Second
	tokenExpiryInterval = 100 * time.Millisecond
)

func TestE2E_PersonalAccessTokenLivesAsLongAsRequested(t *testing.T) {
	owner := connect(t)
	ctx := context.Background()
	name := uniqueName("pat")

	before := time.Now()
	token, err := owner.CreatePersonalAccessToken(ctx, name, iggcon.Duration(tokenLifetime.Microseconds()))
	require.NoError(t, err)
	after := time.Now()
	t.Cleanup(func() { _ = owner.DeletePersonalAccessToken(context.Background(), name) })

	listed := findPersonalAccessToken(t, owner, name)
	require.NotNil(t, listed.Expiry, "a token created with a lifetime lists its expiry")
	assert.WithinRange(t, *listed.Expiry,
		before.Add(tokenLifetime).Add(-time.Second), after.Add(tokenLifetime).Add(time.Second))

	_, err = newClient(t).LoginWithPersonalAccessToken(ctx, token.Token)
	require.NoError(t, err, "the token signs in before its lifetime ends")

	deadline := time.Now().Add(tokenExpiryBudget)
	for {
		_, err = newClient(t).LoginWithPersonalAccessToken(ctx, token.Token)
		if err != nil {
			break
		}
		if time.Now().After(deadline) {
			require.Failf(t, "the token outlived its lifetime",
				"a token created for %v still signs in after %v", tokenLifetime, tokenExpiryBudget)
		}
		time.Sleep(tokenExpiryInterval)
	}
	assert.False(t, time.Now().Before(before.Add(tokenLifetime)), "the token is refused only once its lifetime ends")
}

func TestE2E_PersonalAccessTokenPastTheClockIsRefused(t *testing.T) {
	owner := connect(t)
	ctx := context.Background()
	name := uniqueName("pat")

	_, err := owner.CreatePersonalAccessToken(ctx, name, iggcon.IggyExpiryNeverExpire-1)
	require.ErrorIs(t, err, ierror.ErrInvalidPersonalAccessTokenExpiry)

	tokens, err := owner.GetPersonalAccessTokens(ctx)
	require.NoError(t, err, "the server keeps serving after the refusal")
	for _, token := range tokens {
		assert.NotEqual(t, name, token.Name, "a refused token is not stored")
	}
}

func findPersonalAccessToken(t *testing.T, connected iggcon.Client, name string) iggcon.PersonalAccessTokenInfo {
	t.Helper()

	tokens, err := connected.GetPersonalAccessTokens(context.Background())
	require.NoError(t, err)
	for _, token := range tokens {
		if token.Name == name {
			return token
		}
	}
	require.Failf(t, "token not listed", "no token named %s in %+v", name, tokens)
	return iggcon.PersonalAccessTokenInfo{}
}
