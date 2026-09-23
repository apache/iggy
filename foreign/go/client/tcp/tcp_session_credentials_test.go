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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCredentialClient(autoLogin AutoLogin) *IggyTcpClient {
	config := defaultTcpClientConfig()
	config.autoLogin = autoLogin
	return &IggyTcpClient{config: config}
}

func TestSignInCredentials_AreAbsentUntilSomethingSignsIn(t *testing.T) {
	client := newCredentialClient(AutoLogin{})

	_, ok := client.signInCredentials()
	assert.False(t, ok)
}

func TestSignInCredentials_ComeFromAManualSignInWithoutAutoLogin(t *testing.T) {
	client := newCredentialClient(AutoLogin{})

	client.rememberLogin(NewUsernamePasswordCredentials("iggy", "secret"))

	credentials, ok := client.signInCredentials()
	require.True(t, ok)
	assert.Equal(t, "iggy", credentials.username)
	assert.Equal(t, "secret", credentials.password)

	// An explicit sign-out leaves no session to restore, and a reconnect must
	// not resurrect one.
	client.forgetLogin()
	_, ok = client.signInCredentials()
	assert.False(t, ok)
}

func TestSignInCredentials_PreferTheConfiguredOnes(t *testing.T) {
	client := newCredentialClient(NewAutoLogin(NewUsernamePasswordCredentials("configured", "secret")))

	client.rememberLogin(NewPersonalAccessTokenCredentials("signed-in-token"))

	credentials, ok := client.signInCredentials()
	require.True(t, ok)
	assert.Equal(t, "configured", credentials.username)
	assert.Empty(t, credentials.personalAccessToken)
}
