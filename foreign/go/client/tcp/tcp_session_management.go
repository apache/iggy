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

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/util"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func (c *IggyTcpClient) LoginUser(ctx context.Context, username string, password string) (*iggcon.IdentityInfo, error) {
	c.logger.Info("Iggy client is signing in...", slog.String("client_address", c.clientAddress))
	buffer, err := c.do(ctx, &command.LoginUser{
		Username: username,
		Password: password,
	})
	if err != nil {
		return nil, err
	}

	c.logger.Info("Iggy client has signed in successfully.", slog.String("client_address", c.clientAddress))
	identity := binaryserialization.DeserializeLogInResponse(buffer)
	shouldRedirect, err := c.HandleLeaderRedirection(ctx)
	if err != nil {
		return nil, err
	}
	if shouldRedirect {
		if err = c.Connect(ctx); err != nil {
			return nil, err
		}
		return c.LoginUser(ctx, username, password)
	}
	c.events.publish(iggcon.DiagnosticEventSignedIn)
	return identity, nil
}

func (c *IggyTcpClient) LoginWithPersonalAccessToken(ctx context.Context, token string) (*iggcon.IdentityInfo, error) {
	c.logger.Info("Iggy client is signing in...", slog.String("client_address", c.clientAddress))
	buffer, err := c.do(ctx, &command.LoginWithPersonalAccessToken{
		Token: token,
	})
	if err != nil {
		return nil, err
	}

	c.logger.Info("Iggy client has signed in successfully.", slog.String("client_address", c.clientAddress))
	identity := binaryserialization.DeserializeLogInResponse(buffer)
	shouldRedirect, err := c.HandleLeaderRedirection(ctx)
	if err != nil {
		return nil, err
	}
	if shouldRedirect {
		if err = c.Connect(ctx); err != nil {
			return nil, err
		}
		return c.LoginWithPersonalAccessToken(ctx, token)
	}
	c.events.publish(iggcon.DiagnosticEventSignedIn)
	return identity, nil
}

func (c *IggyTcpClient) LogoutUser(ctx context.Context) error {
	if _, err := c.do(ctx, &command.LogoutUser{}); err != nil {
		return err
	}
	c.events.publish(iggcon.DiagnosticEventSignedOut)
	return nil
}

func (c *IggyTcpClient) HandleLeaderRedirection(ctx context.Context) (bool, error) {
	// Clone current address
	c.mtx.Lock()
	currentAddress := c.currentServerAddress
	c.mtx.Unlock()

	leaderAddress, err := util.CheckAndRedirectToLeader(
		ctx,
		c,
		currentAddress,
		iggcon.Tcp,
		c.logger,
	)
	if err != nil {
		return false, err
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

	c.events.publish(iggcon.DiagnosticEventRedirected)

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
