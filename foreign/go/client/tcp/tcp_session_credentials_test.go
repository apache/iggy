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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	signedInUserID  = 7
	signInUsername  = "iggy"
	signInPassword  = "secret"
	renamedUsername = "renamed"
	rotatedPassword = "rotated"
)

// credentialChange is a password change or a rename of the signed-in user.
type credentialChange struct {
	request command.Command
	typed   func(ctx context.Context, client *IggyTcpClient) error
}

func passwordChange(t *testing.T, currentPassword string) credentialChange {
	t.Helper()
	user, err := iggcon.NewIdentifier(signInUsername)
	require.NoError(t, err)
	return credentialChange{
		request: &command.ChangePassword{UserID: user, CurrentPassword: currentPassword, NewPassword: rotatedPassword},
		typed: func(ctx context.Context, client *IggyTcpClient) error {
			return client.ChangePassword(ctx, user, currentPassword, rotatedPassword)
		},
	}
}

func rename(t *testing.T) credentialChange {
	t.Helper()
	user := numericIdentifier(t, signedInUserID)
	username := renamedUsername
	return credentialChange{
		request: &command.UpdateUser{UserID: user, Username: &username},
		typed: func(ctx context.Context, client *IggyTcpClient) error {
			return client.UpdateUser(ctx, user, &username, nil)
		},
	}
}

// send makes the change through its typed method, or as a raw request. A call
// that ends uncertain is resolved, so the result is the proved outcome.
func (change credentialChange) send(t *testing.T, client *IggyTcpClient, raw bool) error {
	t.Helper()
	ctx := context.Background()
	var err error
	if raw {
		body, marshalErr := change.request.MarshalBinary()
		require.NoError(t, marshalErr)
		_, err = client.SendBinaryRequest(ctx, uint32(change.request.Code()), body)
	} else {
		err = change.typed(ctx, client)
	}
	var uncertain ierror.UncertainOutcome
	if errors.As(err, &uncertain) {
		return client.ResolveOperation(ctx, uncertain.OperationID)
	}
	return err
}

// credentialServer is a one-node server that signs in every Register and
// answers each credential change with change, by the attempt of its request ID
// from 0. After evict, it ends the session of the next Ping.
func credentialServer(t *testing.T, change func(attempt int, read request) []byte) (*testListener, *atomic.Bool) {
	t.Helper()
	var server *testListener
	var evict atomic.Bool
	var mtx sync.Mutex
	attempts := make(map[uint64]int)
	server = listenVSR(t, nil, func(_, _ int, read request) []byte {
		switch {
		case read.operation() == vsr.OperationRegister:
			return registerReplyFrame(signedInUserID, 128)
		case read.code() == uint32(command.BindSessionCode):
			return bindReplyFrame(signedInUserID, read.sessionID())
		case read.code() == uint32(command.GetClusterMetadataCode):
			return clusterMetadataFrame(t, 0, server.address())
		case read.code() == uint32(command.PingCode) && evict.CompareAndSwap(true, false):
			return evictionFrame(vsr.EvictionSessionError, 0, 0)
		case read.operation() == vsr.OperationChangePassword || read.operation() == vsr.OperationUpdateUser:
			mtx.Lock()
			attempt := attempts[read.requestID()]
			attempts[read.requestID()]++
			mtx.Unlock()
			return change(attempt, read)
		default:
			return replyFrame(read.operation(), nil)
		}
	})
	return server, &evict
}

// signedInClient signs in as the configured user, or as the same user by hand.
func signedInClient(t *testing.T, server *testListener, autoLogin bool) *IggyTcpClient {
	t.Helper()
	var options []Option
	if autoLogin {
		options = append(options, WithAutoLogin(NewUsernamePasswordCredentials(signInUsername, signInPassword)))
	}
	client := newDialingClient(t, server.address(), options...)
	require.NoError(t, client.Connect(context.Background()))
	if !autoLogin {
		_, err := client.LoginUser(context.Background(), signInUsername, signInPassword)
		require.NoError(t, err)
	}
	return client
}

// requireSignInAgain ends the session on the server and checks that the
// client then signs in as username with password.
func requireSignInAgain(t *testing.T, client *IggyTcpClient, server *testListener, evict *atomic.Bool, username, password string) {
	t.Helper()
	registers := func() []request { return requestsFor(server.recorded(), vsr.OperationRegister) }
	before := len(registers())
	evict.Store(true)
	require.ErrorIs(t, client.Ping(context.Background()), ierror.ErrUnauthenticated)
	require.Eventually(t, func() bool { return len(registers()) > before }, time.Second, time.Millisecond)
	body := registers()[before].payload
	offset := versionInfoEnd(t, body) + vsr.BindSecretBytes
	var names []string
	for range 2 {
		require.Greater(t, len(body), offset)
		end := offset + 1 + int(body[offset])
		require.GreaterOrEqual(t, len(body), end)
		names = append(names, string(body[offset+1:end]))
		offset = end
	}
	require.Equal(t, []string{username, password}, names, "the username and password of the new sign-in")
}

func credentialChanges(reads []request) int {
	return len(requestsFor(reads, vsr.OperationChangePassword)) + len(requestsFor(reads, vsr.OperationUpdateUser))
}

func TestCredentialChange_CommittedChangeIsTheNextSignIn(t *testing.T) {
	for _, test := range []struct {
		name               string
		change             func(t *testing.T) credentialChange
		username, password string
	}{
		{"password", func(t *testing.T) credentialChange { return passwordChange(t, signInPassword) }, signInUsername, rotatedPassword},
		{"rename", rename, renamedUsername, signInPassword},
	} {
		for _, raw := range []bool{false, true} {
			for _, resolved := range []bool{false, true} {
				for _, autoLogin := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/raw_%t/resolved_%t/auto_login_%t", test.name, raw, resolved, autoLogin), func(t *testing.T) {
						server, evict := credentialServer(t, func(attempt int, read request) []byte {
							if resolved && attempt < 2 {
								return uncertainAnswer(attempt, read)
							}
							return committedReply(read, 0)
						})
						client := signedInClient(t, server, autoLogin)

						require.NoError(t, test.change(t).send(t, client, raw))

						attempts := 1
						if resolved {
							attempts = 3
						}
						require.Equal(t, attempts, credentialChanges(server.recorded()))
						requireSignInAgain(t, client, server, evict, test.username, test.password)
					})
				}
			}
		}
	}
}

func TestCredentialChange_CommittedRejectionKeepsTheNextSignIn(t *testing.T) {
	for _, raw := range []bool{false, true} {
		for _, resolved := range []bool{false, true} {
			for _, autoLogin := range []bool{false, true} {
				t.Run(fmt.Sprintf("raw_%t/resolved_%t/auto_login_%t", raw, resolved, autoLogin), func(t *testing.T) {
					server, evict := credentialServer(t, func(attempt int, read request) []byte {
						if resolved && attempt < 2 {
							return uncertainAnswer(attempt, read)
						}
						return committedReply(read, ierror.InvalidCredentialsCode)
					})
					client := signedInClient(t, server, autoLogin)

					err := passwordChange(t, "wrong").send(t, client, raw)

					require.ErrorIs(t, err, ierror.ErrInvalidCredentials)
					requireSignInAgain(t, client, server, evict, signInUsername, signInPassword)
				})
			}
		}
	}
}

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
