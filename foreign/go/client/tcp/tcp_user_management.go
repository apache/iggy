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

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
)

// usernamePresent flags the optional username of an UpdateUser body.
const usernamePresent = 1

func (c *IggyTcpClient) GetUser(ctx context.Context, identifier iggcon.Identifier) (*iggcon.UserInfoDetails, error) {
	buffer, err := c.do(ctx, &command.GetUser{Id: identifier})
	if err != nil {
		return nil, err
	}
	if len(buffer) == 0 {
		return nil, ierror.ErrResourceNotFound
	}

	return binaryserialization.DeserializeUser(buffer)
}

func (c *IggyTcpClient) GetUsers(ctx context.Context) ([]iggcon.UserInfo, error) {
	buffer, err := c.do(ctx, &command.GetUsers{})
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeUsers(buffer)
}

func (c *IggyTcpClient) CreateUser(ctx context.Context, username string, password string, status iggcon.UserStatus, permissions *iggcon.Permissions) (*iggcon.UserInfoDetails, error) {
	buffer, err := c.do(ctx, &command.CreateUser{
		Username:    username,
		Password:    password,
		Status:      status,
		Permissions: permissions,
	})
	if err != nil {
		return nil, err
	}
	userInfo, err := binaryserialization.DeserializeUser(buffer)
	if err != nil {
		return nil, err
	}
	return userInfo, nil
}

func (c *IggyTcpClient) UpdateUser(ctx context.Context, userID iggcon.Identifier, username *string, status *iggcon.UserStatus) error {
	_, err := c.do(ctx, &command.UpdateUser{
		UserID:   userID,
		Username: username,
		Status:   status,
	})
	return err
}

func (c *IggyTcpClient) DeleteUser(ctx context.Context, identifier iggcon.Identifier) error {
	_, err := c.do(ctx, &command.DeleteUser{
		Id: identifier,
	})
	return err
}

func (c *IggyTcpClient) UpdatePermissions(ctx context.Context, userID iggcon.Identifier, permissions *iggcon.Permissions) error {
	_, err := c.do(ctx, &command.UpdatePermissions{
		UserID:      userID,
		Permissions: permissions,
	})
	return err
}

func (c *IggyTcpClient) ChangePassword(ctx context.Context, userID iggcon.Identifier, currentPassword string, newPassword string) error {
	_, err := c.do(ctx, &command.ChangePassword{
		UserID:          userID,
		CurrentPassword: currentPassword,
		NewPassword:     newPassword,
	})
	return err
}

// refreshSignIn applies a committed password change or rename to the
// stored sign-in credentials. It reads the committed request body, so
// typed calls, raw sends and replays follow one rule. A body that does
// not decode changes nothing and never fails the committed call.
func (c *IggyTcpClient) refreshSignIn(code command.Code, body []byte) {
	if (code != command.ChangePasswordCode && code != command.UpdateUserCode) || len(body) < fieldPrefixSize {
		return
	}
	end := fieldPrefixSize + int(body[1])
	if len(body) < end {
		return
	}
	user, rest := string(body[:end]), body[end:]
	if code == command.ChangePasswordCode {
		if _, rest, ok := splitName(rest); ok {
			if password, _, ok := splitName(rest); ok {
				c.refreshCredentials(user, nil, &password)
			}
		}
		return
	}
	if len(rest) > 0 && rest[0] == usernamePresent {
		if username, _, ok := splitName(rest[1:]); ok {
			c.refreshCredentials(user, &username, nil)
		}
	}
}

// refreshCredentials updates each stored login that user names. Only the
// signed-in user has a known numeric ID, so a change by ID reaches the
// configured login only when it is the signed-in user.
func (c *IggyTcpClient) refreshCredentials(user string, username, password *string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	oldUsername := c.rememberedLogin.credentials.username
	configuredUsername := c.config.autoLogin.credentials.username
	self := c.rememberedLogin.enabled && (user == encodedUser(c.sessionUserID) || user == encodedUser(oldUsername))
	configured := c.config.autoLogin.enabled &&
		(self && oldUsername == configuredUsername || user == encodedUser(configuredUsername))
	for _, login := range []struct {
		value   *AutoLogin
		matches bool
	}{{&c.rememberedLogin, self}, {&c.config.autoLogin, configured}} {
		if !login.matches || login.value.credentials.personalAccessToken != "" {
			continue
		}
		if username != nil {
			login.value.credentials.username = *username
		}
		if password != nil {
			login.value.credentials.password = *password
		}
	}
}

// encodedUser is the wire form of a user identifier. An invalid value,
// such as the empty username of a token login, encodes to "" and so
// matches no request body.
func encodedUser[T uint32 | string](value T) string {
	identifier, err := iggcon.NewIdentifier(value)
	if err != nil {
		return ""
	}
	encoded, _ := identifier.MarshalBinary()
	return string(encoded)
}

// splitName splits a [u8 length][value] field off data.
func splitName(data []byte) (string, []byte, bool) {
	if len(data) == 0 || len(data) < 1+int(data[0]) {
		return "", nil, false
	}
	end := 1 + int(data[0])
	return string(data[1:end]), data[end:], true
}
