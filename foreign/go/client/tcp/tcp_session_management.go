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
	return c.register(ctx, uint32(command.LoginRegisterCode), body)
}

func (c *IggyTcpClient) LoginWithPersonalAccessToken(ctx context.Context, token string) (*iggcon.IdentityInfo, error) {
	body, err := vsr.SerializeLoginRegisterWithToken(token, iggcon.Version)
	if err != nil {
		return nil, err
	}
	return c.register(ctx, uint32(command.LoginRegisterWithPATCode), body)
}

// register runs the sign-in handshake and binds the session the server
// assigned. Leadership is already settled by the connect flow, so no
// redirection happens here.
//
// A failed sign-in never writes the session state: a server-side reject leaves
// the existing session untouched, and a connection that dies mid-attempt is
// already reset by invalidateConnLocked.
func (c *IggyTcpClient) register(ctx context.Context, code uint32, body []byte) (*iggcon.IdentityInfo, error) {
	c.logger.Info("Iggy client is signing in...", slog.String("client_address", c.clientAddress))

	if err := c.endBoundSession(ctx); err != nil {
		return nil, err
	}

	bp := acquireRequestBuf()
	defer releaseRequestBuf(bp)
	frame := append(append((*bp)[:0], requestPrologue[:]...), body...)
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

// endBoundSession logs out a live session before a re-login, so the server
// drops its client-table entry instead of leaving it to be fenced.
func (c *IggyTcpClient) endBoundSession(ctx context.Context) error {
	c.mtx.Lock()
	bound := c.session.Bound()
	c.mtx.Unlock()
	if !bound {
		return nil
	}
	return c.LogoutUser(ctx)
}

func (c *IggyTcpClient) LogoutUser(ctx context.Context) error {
	if _, err := c.do(ctx, &command.LogoutUser{}); err != nil {
		return err
	}
	c.mtx.Lock()
	c.sessionState = iggcon.SessionStateUnauthenticated
	c.session.Reset()
	c.groups.clear()
	c.topics.clear()
	c.mtx.Unlock()
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
