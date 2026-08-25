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
	"log/slog"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/util"
	"github.com/apache/iggy/foreign/go/internal/vsr"
)

func (c *IggyTcpClient) LoginUser(ctx context.Context, username string, password string) (*iggcon.IdentityInfo, error) {
	body, err := vsr.SerializeLoginRegister(username, password, iggcon.Version)
	if err != nil {
		return nil, err
	}
	return c.register(
		ctx,
		uint32(command.LoginRegisterCode),
		body,
		NewUsernamePasswordCredentials(username, password),
	)
}

func (c *IggyTcpClient) LoginWithPersonalAccessToken(ctx context.Context, token string) (*iggcon.IdentityInfo, error) {
	body, err := vsr.SerializeLoginRegisterWithToken(token, iggcon.Version)
	if err != nil {
		return nil, err
	}
	return c.register(
		ctx,
		uint32(command.LoginRegisterWithPATCode),
		body,
		NewPersonalAccessTokenCredentials(token),
	)
}

// register runs the sign-in handshake, binds the session the server assigned,
// settles the connection on the cluster leader, and remembers the credentials
// it succeeded with so a reconnect can re-establish the session.
//
// The credentials are remembered here rather than by the callers because this
// is what holds registerMtx: remembered outside it, two concurrent sign-ins
// could leave A remembered while the session is B, and the next reconnect
// would sign in as A.
func (c *IggyTcpClient) register(
	ctx context.Context,
	code uint32,
	body []byte,
	credentials Credentials,
) (*iggcon.IdentityInfo, error) {
	// One sign-in at a time. BeginRegister runs inside the exchange lock but
	// Bind runs after it, so two interleaved sign-ins would let the second
	// BeginRegister reset the identity the first is about to bind: one
	// committed Register would be orphaned in the server's client table and
	// the losing caller would see ErrSessionAlreadyBound.
	c.registerMtx.Lock()
	defer c.registerMtx.Unlock()

	c.logger.Info("Iggy client is signing in...", slog.String("client_address", c.clientAddress))

	if err := c.endBoundSession(ctx); err != nil {
		return nil, err
	}

	identity, err := c.signIn(ctx, code, body)
	if err != nil {
		return nil, err
	}

	settled, err := c.settleOnLeader(ctx, code, body)
	if err != nil {
		return nil, err
	}
	c.rememberLogin(credentials)
	if settled != nil {
		return settled, nil
	}
	return identity, nil
}

// signIn runs one sign-in exchange on the current connection and binds the
// session the server assigned.
//
// A failed sign-in never writes the session state: a server-side reject leaves
// the existing session untouched, and a connection that dies mid-attempt is
// already reset by invalidateConnLocked.
func (c *IggyTcpClient) signIn(ctx context.Context, code uint32, body []byte) (*iggcon.IdentityInfo, error) {
	bp := acquireRequestBuf()
	defer releaseRequestBuf(bp)
	frame := append(reserveHeader(*bp), body...)
	*bp = frame

	response, err := c.exchange(ctx, code, frame)
	if err != nil {
		return nil, err
	}

	registered, err := vsr.DecodeLoginRegister(response)
	if err != nil {
		return nil, err
	}

	c.mtx.Lock()
	err = c.session.Bind(registered.Session)
	if err == nil {
		c.sessionState = iggcon.SessionStateAuthenticated
		c.loggedOut = false
	} else {
		// The server committed a Register this client failed to adopt, so the
		// connection carries a session the local state does not track. It is
		// unusable; drop it like any other terminal session failure.
		c.invalidateConnLocked()
	}
	c.mtx.Unlock()
	if err != nil {
		return nil, err
	}

	c.logger.Info("Iggy client has signed in successfully.",
		slog.String("client_address", c.clientAddress),
		slog.String("server_version", registered.ServerVersion))
	return &iggcon.IdentityInfo{UserId: registered.UserID}, nil
}

// settleOnLeader moves a freshly signed-in session to the cluster leader.
//
// Only the leader accepts replicated commands, and the roster read is
// auth-gated, so the topology cannot be inspected before a login binds a
// session. A login dialed at a backup still succeeds (the server forwards the
// register to the primary); this settlement decides where later requests
// land, not whether the sign-in works. The redirect drops the fresh session
// along with the socket, so the sign-in is replayed on the leader and its
// identity supersedes the dialed node's. Leadership can move between the
// roster read and the replay, so each freshly bound hop rechecks the roster
// under the shared redirect budget.
//
// Returns nil when the client stays where it is.
func (c *IggyTcpClient) settleOnLeader(ctx context.Context, code uint32, body []byte) (*iggcon.IdentityInfo, error) {
	var settled *iggcon.IdentityInfo
	for {
		// The roster read runs while register holds the sign-in lock, so it must
		// not enter the reconnect path: the reconnect's automatic sign-in would
		// deadlock on that lock. The connect scope fails it fast instead.
		redirect, err := c.HandleLeaderRedirection(
			context.WithValue(ctx, connectScoped{}, struct{}{}))
		if err != nil || !redirect {
			return settled, err
		}

		// The replayed sign-in below owns the session; the redirected Connect
		// must not sign in on its own, or the replay commits a second Register.
		c.mtx.Lock()
		c.skipAutoLoginOnce = true
		c.mtx.Unlock()
		if err := c.Connect(ctx); err != nil {
			c.mtx.Lock()
			c.skipAutoLoginOnce = false
			c.mtx.Unlock()
			return nil, err
		}
		settled, err = c.signIn(ctx, code, body)
		if err != nil {
			return nil, err
		}
	}
}

// endBoundSession logs out a live session before a re-login, so the server
// drops its client-table entry instead of leaving it to be fenced.
//
// The logout runs connect-scoped, and a failure it could recover from is
// swallowed. Both because this call holds registerMtx: a logout that entered
// the reconnect path would reconnect, sign in with the remembered credentials,
// and deadlock on that lock. There is nothing to salvage either way -- a
// session whose logout cannot be delivered died with its socket, and the
// server fences what it left behind -- and the sign-in that follows replays
// through its own reconnect.
func (c *IggyTcpClient) endBoundSession(ctx context.Context) error {
	c.mtx.Lock()
	bound := c.session.Bound()
	c.mtx.Unlock()
	if !bound {
		return nil
	}

	err := c.LogoutUser(context.WithValue(ctx, connectScoped{}, struct{}{}))
	if err == nil {
		return nil
	}
	if !isReconnectable(err) {
		return err
	}

	c.logger.Debug("The bound session's logout was not delivered; its socket ended it.",
		slog.Any("error", err))
	c.mtx.Lock()
	c.sessionState = iggcon.SessionStateUnauthenticated
	c.session.Reset()
	c.groups.clear()
	c.topics.clearCounts()
	c.mtx.Unlock()
	// The session this sign-in belonged to is over either way, so the
	// credentials that established it go with it. Kept, a sign-in that then
	// fails would leave them behind for the next dropped request to replay --
	// signing the old user back in after the caller asked for another one.
	c.forgetLogin()
	return nil
}

func (c *IggyTcpClient) LogoutUser(ctx context.Context) error {
	if _, err := c.do(ctx, &command.LogoutUser{}); err != nil {
		return err
	}
	c.mtx.Lock()
	c.sessionState = iggcon.SessionStateUnauthenticated
	c.session.Reset()
	// The sign-out is caller intent: it suppresses the automatic sign-in on
	// the reconnect path until the caller explicitly signs in again.
	c.loggedOut = true
	c.groups.clear()
	c.topics.clearCounts()
	c.mtx.Unlock()
	c.forgetLogin()
	return nil
}

func (c *IggyTcpClient) HandleLeaderRedirection(ctx context.Context) (bool, error) {
	// Clone current address
	c.mtx.Lock()
	currentAddress := c.currentServerAddress
	c.mtx.Unlock()

	leaderAddress, serverAddresses, err := util.CheckAndRedirectToLeader(
		ctx,
		c,
		currentAddress,
		iggcon.Tcp,
		c.logger,
	)
	if err != nil {
		return false, err
	}
	if len(serverAddresses) > 0 {
		c.mtx.Lock()
		c.knownServerAddresses = serverAddresses
		c.mtx.Unlock()
	}

	if leaderAddress == "" {
		// No leader redirection
		c.mtx.Lock()
		c.leaderRedirectionState.Reset()
		c.mtx.Unlock()

		return false, nil
	}

	c.mtx.Lock()
	if !c.leaderRedirectionState.CanRedirect() {
		c.mtx.Unlock()
		c.logger.Warn("Maximum leader redirections reached, continuing with current connection")
		return false, nil
	}
	c.mtx.Unlock()

	if err = c.disconnect(); err != nil {
		return false, err
	}

	c.mtx.Lock()
	c.leaderRedirectionState.IncrementRedirect(leaderAddress)
	// Clear connectedAt to avoid reestablish delay during redirection
	c.connectedAt = time.Time{}
	c.currentServerAddress = leaderAddress
	c.mtx.Unlock()

	return true, nil
}
