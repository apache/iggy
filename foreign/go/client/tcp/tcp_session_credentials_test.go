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
			return client.UpdateUser(ctx, user, iggcon.UserUpdate{Username: &username})
		},
	}
}

// otherRename is a patch that names something other than the signed-in user.
func otherRename(t *testing.T, name string) credentialChange {
	t.Helper()
	newName := renamedUsername
	switch name {
	case "other_user":
		user := numericIdentifier(t, signedInUserID+1)
		return credentialChange{
			request: &command.UpdateUser{UserID: user, Username: &newName},
			typed: func(ctx context.Context, client *IggyTcpClient) error {
				return client.UpdateUser(ctx, user, iggcon.UserUpdate{Username: &newName})
			},
		}
	case "status_only":
		user := numericIdentifier(t, signedInUserID)
		status := iggcon.Active
		return credentialChange{
			request: &command.UpdateUser{UserID: user, Status: &status},
			typed: func(ctx context.Context, client *IggyTcpClient) error {
				return client.UpdateUser(ctx, user, iggcon.UserUpdate{Status: &status})
			},
		}
	case "stream":
		stream := numericIdentifier(t, signedInUserID)
		return credentialChange{
			request: &command.UpdateStream{StreamId: stream, Name: &newName},
			typed: func(ctx context.Context, client *IggyTcpClient) error {
				return client.UpdateStream(ctx, stream, iggcon.StreamUpdate{Name: &newName})
			},
		}
	default:
		stream := numericIdentifier(t, signedInUserID)
		topic := numericIdentifier(t, signedInUserID)
		return credentialChange{
			request: &command.UpdateTopic{StreamId: stream, TopicId: topic, Name: &newName},
			typed: func(ctx context.Context, client *IggyTcpClient) error {
				return client.UpdateTopic(ctx, stream, topic, iggcon.TopicUpdate{Name: &newName})
			},
		}
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
	var uncertain ierror.OperationUncertain
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
		case read.operation() == vsr.OperationChangePassword || read.operation() == vsr.OperationUpdateResource:
			mtx.Lock()
			attempt := attempts[read.requestID()]
			attempts[read.requestID()]++
			mtx.Unlock()
			return resolvedReply(change(attempt, read), read)
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
	require.NoError(t, client.Ping(context.Background()), "the ping completes under the session of the new sign-in")
	require.Greater(t, len(registers()), before)
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

// resolvedReply stamps a committed patch reply with the operation prepare
// rewrites it to, as a server does: 73 for a topic, 72 for a stream, 74 for a
// user, chosen by the scope byte that leads the request body.
func resolvedReply(reply []byte, read request) []byte {
	if read.operation() != vsr.OperationUpdateResource || reply[220] != 1 || len(read.payload) == 0 {
		return reply
	}
	resolved := map[byte]vsr.Operation{
		byte(iggcon.OptionsScopeTopic):  vsr.OperationUpdateTopicResolved,
		byte(iggcon.OptionsScopeStream): vsr.OperationUpdateStreamResolved,
		byte(iggcon.OptionsScopeUser):   vsr.OperationUpdateUserResolved,
	}[read.payload[0]]
	reply[replyFrameOffsetOperation] = byte(resolved)
	return reply
}

func credentialChanges(reads []request) int {
	return len(requestsFor(reads, vsr.OperationChangePassword)) + len(requestsFor(reads, vsr.OperationUpdateResource))
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

func TestCredentialChange_GenericUserPatch(t *testing.T) {
	t.Run("given_logged_in_user_rename_when_reconnecting_should_keep_new_username", func(t *testing.T) {
		for _, raw := range []bool{false, true} {
			for _, autoLogin := range []bool{false, true} {
				t.Run(fmt.Sprintf("self_rename/raw_%t/auto_login_%t", raw, autoLogin), func(t *testing.T) {
					server, evict := credentialServer(t, func(_ int, read request) []byte {
						return committedReply(read, 0)
					})
					client := signedInClient(t, server, autoLogin)

					require.NoError(t, rename(t).send(t, client, raw))

					require.Len(t, requestsFor(server.recorded(), vsr.OperationUpdateResource), 1,
						"a rename travels as the generic patch")
					requireSignInAgain(t, client, server, evict, renamedUsername, signInPassword)
				})

				t.Run(fmt.Sprintf("rejected_rename/raw_%t/auto_login_%t", raw, autoLogin), func(t *testing.T) {
					server, evict := credentialServer(t, func(_ int, read request) []byte {
						return committedReply(read, ierror.UserAlreadyExistsCode)
					})
					client := signedInClient(t, server, autoLogin)

					err := rename(t).send(t, client, raw)

					require.ErrorIs(t, err, ierror.ErrUserAlreadyExists)
					requireSignInAgain(t, client, server, evict, signInUsername, signInPassword)
				})

				for _, name := range []string{"other_user", "status_only", "stream", "topic"} {
					t.Run(fmt.Sprintf("%s/raw_%t/auto_login_%t", name, raw, autoLogin), func(t *testing.T) {
						server, evict := credentialServer(t, func(_ int, read request) []byte {
							return committedReply(read, 0)
						})
						client := signedInClient(t, server, autoLogin)

						require.NoError(t, otherRename(t, name).send(t, client, raw))

						requireSignInAgain(t, client, server, evict, signInUsername, signInPassword)
					})
				}
			}
		}
	})
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

	client.rememberedLogin = NewAutoLogin(NewUsernamePasswordCredentials("iggy", "secret"))

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

func TestSignInCredentials_PreferTheLastSignIn(t *testing.T) {
	client := newCredentialClient(NewAutoLogin(NewUsernamePasswordCredentials("configured", "secret")))

	client.rememberedLogin = NewAutoLogin(NewPersonalAccessTokenCredentials("signed-in-token"))

	credentials, ok := client.signInCredentials()
	require.True(t, ok)
	assert.Equal(t, "signed-in-token", credentials.personalAccessToken)
	assert.Empty(t, credentials.username)
}

// readPasswordReply stops at the driver's validated-reply boundary. Acceptance
// publishes credentials atomically, so there is no separate publication step.
func readPasswordReply(t *testing.T, ctx context.Context, client *IggyTcpClient) (*signInEpoch, *pendingOperation, linkReply) {
	t.Helper()
	body, err := passwordChange(t, signInPassword).request.MarshalBinary()
	require.NoError(t, err)
	frame := append(reserveHeader(nil), body...)
	client.mtx.Lock()
	epoch := client.callEpoch(ctx)
	fence := client.sessionFence
	err = vsr.StampRequestHeader(client.session, uint32(command.ChangePasswordCode), frame)
	client.mtx.Unlock()
	require.NoError(t, err)
	operation := &pendingOperation{code: command.ChangePasswordCode, fence: fence, frame: newOperationFrame(frame, nil)}
	t.Cleanup(operation.frame.release)
	reply, _ := client.links.any().exchange(ctx, operation.frame)
	require.NoError(t, reply.err)
	outcome, err := vsr.DecodeReplyFor((*[vsr.HeaderSize]byte)(frame), &reply.header, reply.body)
	require.NoError(t, err)
	require.NoError(t, outcome.Err)
	require.True(t, outcome.Committed)
	return epoch, operation, reply
}

// Automatic sign-in selects credentials before signIn resets the fence. This
// component proof carries that snapshot through a real Register after the
// accepted password change publishes, then checks the next wire credentials.
func TestRecoveryRegisterKeepsCredentialsPublishedByTheSameOwner(t *testing.T) {
	for _, refused := range []bool{false, true} {
		for _, autoLogin := range []bool{false, true} {
			t.Run(fmt.Sprintf("refused_%t/auto_login_%t", refused, autoLogin), func(t *testing.T) {
				registered, release := make(chan struct{}), make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				var registrations atomic.Uint32
				var evict atomic.Bool
				var server *testListener
				server = listenVSR(t, nil, func(_, _ int, read request) []byte {
					switch {
					case read.operation() == vsr.OperationRegister:
						number := registrations.Add(1)
						if number == 2 {
							close(registered)
							<-release
							if refused {
								return statusReplyFrame(read.operation(), uint32(ierror.InvalidCredentialsCode), nil)
							}
						}
						return registerReplyFrame(signedInUserID, managedLinkSession+uint64(number))
					case read.operation() == vsr.OperationChangePassword:
						return committedReply(read, 0)
					case read.code() == uint32(command.PingCode) && evict.CompareAndSwap(true, false):
						return evictionFrame(vsr.EvictionSessionError, 0, 0)
					default:
						return singleNodeHandler(t, func() string { return server.address() })(0, 0, read)
					}
				})
				client := signedInClient(t, server, autoLogin)
				ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
				defer cancel()
				attempted, enabled := client.signInCredentials()
				require.True(t, enabled)
				secret := vsr.NewBindSecret()
				body, err := vsr.SerializeLoginRegister(attempted.username, attempted.password, iggcon.Version, secret)
				require.NoError(t, err)
				epoch, operation, reply := readPasswordReply(t, ctx, client)
				require.True(t, client.acceptOperationReply(epoch, operation, &reply.header, true))
				done := make(chan error, 1)
				go func() {
					_, err := client.signIn(ctx, client.links.any(), uint32(command.LoginRegisterCode), body, secret, attempted, true)
					done <- err
				}()
				select {
				case <-registered:
				case <-ctx.Done():
					t.Fatal("the automatic attempt did not send its selected credentials")
				}
				unblock()
				err = <-done
				if refused {
					var refusal *signInRefusal
					require.ErrorAs(t, err, &refusal)
					retry, _ := client.endRefusedSignIn(epoch, attempted, refusal.err)
					require.True(t, retry, "the new credential was never refused")
					require.Same(t, epoch, client.epoch.Load())
					require.NoError(t, client.Connect(ctx))
					require.Len(t, registerRequests(server), 3)
					read := registerRequests(server)[2]
					offset := versionInfoEnd(t, read.payload) + vsr.BindSecretBytes
					_, rest, ok := splitName(read.payload[offset:])
					require.True(t, ok)
					password, _, ok := splitName(rest)
					require.True(t, ok)
					require.Equal(t, rotatedPassword, password)
				} else {
					require.NoError(t, err)
					require.Len(t, registerRequests(server), 2, "an accepted Register must not be restarted")
				}
				credentials, enabled := client.signInCredentials()
				require.True(t, enabled)
				require.Equal(t, rotatedPassword, credentials.password)
				requireSignInAgain(t, client, server, &evict, signInUsername, rotatedPassword)
			})
		}
	}
}

func TestCredentialAcceptanceCannotChangeANewerExplicitOwner(t *testing.T) {
	server, evict := credentialServer(t, func(_ int, read request) []byte { return committedReply(read, 0) })
	client := signedInClient(t, server, true)
	ctx, cancel := context.WithTimeout(context.Background(), managedLinkTestBudget)
	defer cancel()
	epoch, operation, reply := readPasswordReply(t, ctx, client)
	const newerPassword = "newer-owner-password"
	_, err := client.LoginUser(ctx, signInUsername, newerPassword)
	require.NoError(t, err)
	require.False(t, client.acceptOperationReply(epoch, operation, &reply.header, true))
	retry, ended := client.endRefusedSignIn(epoch, NewUsernamePasswordCredentials(signInUsername, signInPassword), ierror.ErrInvalidCredentials)
	require.False(t, retry || ended, "old refusal cannot end the newer owner")
	requireSignInAgain(t, client, server, evict, signInUsername, newerPassword)
	client.mtx.Lock()
	configuredPassword := client.config.autoLogin.credentials.password
	client.mtx.Unlock()
	require.Equal(t, signInPassword, configuredPassword, "old publication must not change the newer owner's configured fallback")
}

func TestAcceptedLogoutCompletionCannotClearANewerExplicitOwner(t *testing.T) {
	server, evict := credentialServer(t, func(_ int, read request) []byte { return committedReply(read, 0) })
	client := signedInClient(t, server, true)
	ctx, cancel := client.operationContext(context.Background())
	defer cancel()
	epoch := client.callEpoch(ctx)
	_, err := client.do(ctx, &command.LogoutUser{})
	require.NoError(t, err)
	_, err = client.LoginUser(context.Background(), "newer", signInPassword)
	require.NoError(t, err)
	newerEpoch := client.epoch.Load()
	client.finishLogout(epoch)
	require.Same(t, newerEpoch, client.epoch.Load(), "accepted Logout cleanup must retain the newer caller owner")
	require.True(t, sessionBound(client))
	requireSignInAgain(t, client, server, evict, "newer", signInPassword)
}
