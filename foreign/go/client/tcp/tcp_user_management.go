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

func (c *IggyTcpClient) CreateUser(
	ctx context.Context,
	username string,
	password string,
	status iggcon.UserStatus,
	permissions *iggcon.Permissions,
	options *iggcon.UserOptions,
) (*iggcon.UserInfoDetails, error) {
	request := &command.CreateUser{
		Username:    username,
		Password:    password,
		Status:      status,
		Permissions: permissions,
	}
	if options != nil {
		request.Options = *options
	}
	buffer, err := c.do(ctx, request)
	if err != nil {
		return nil, err
	}
	userInfo, err := binaryserialization.DeserializeUser(buffer)
	if err != nil {
		return nil, err
	}
	return userInfo, nil
}

func (c *IggyTcpClient) UpdateUser(ctx context.Context, userID iggcon.Identifier, update iggcon.UserUpdate) error {
	_, err := c.do(ctx, &command.UpdateUser{
		UserID:   userID,
		Username: update.Username,
		Status:   update.Status,
		Reset:    update.Reset,
		RawReset: update.RawReset,
		Options:  update.Options,
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

// refreshSignInLocked applies a committed password change or rename to the
// stored sign-in credentials. It reads the committed request body, so
// typed calls, raw sends and replays follow one rule. A body that does
// not decode changes nothing and never fails the committed call.
func (c *IggyTcpClient) refreshSignInLocked(epoch *signInEpoch, code command.Code, body []byte) {
	switch code {
	case command.ChangePasswordCode:
		user, rest, ok := splitIdentifier(body)
		if !ok {
			return
		}
		if _, rest, ok := splitName(rest); ok {
			if password, _, ok := splitName(rest); ok {
				c.refreshCredentialsLocked(epoch, user, nil, &password)
			}
		}
	case command.UpdateResourceCode:
		if len(body) == 0 || body[0] != byte(iggcon.OptionsScopeUser) {
			return
		}
		if user, username, ok := patchedUsername(body[1:]); ok {
			c.refreshCredentialsLocked(epoch, user, &username, nil)
		}
	}
}

// patchedUsername reads the user identifier and the username a user-scope
// UpdateResource body sets: [user_id][reset_count][reset keys][options].
func patchedUsername(body []byte) (string, string, bool) {
	user, rest, ok := splitIdentifier(body)
	if !ok || len(rest) == 0 {
		return "", "", false
	}
	resetCount := int(rest[0])
	rest = rest[1:]
	for range resetCount {
		if _, rest, ok = splitName(rest); !ok {
			return "", "", false
		}
	}
	options, err := iggcon.DeserializeHeaders(rest)
	if err != nil {
		return "", "", false
	}
	for _, entry := range options {
		if entry.Key.Kind == iggcon.String && string(entry.Key.Value) == string(iggcon.UserOptionUsername) && entry.Value.Kind == iggcon.String {
			return user, string(entry.Value.Value), true
		}
	}
	return "", "", false
}

// splitIdentifier splits a [kind][u8 length][value] identifier off data and
// returns it in its wire form.
func splitIdentifier(data []byte) (string, []byte, bool) {
	if len(data) < fieldPrefixSize || len(data) < fieldPrefixSize+int(data[1]) {
		return "", nil, false
	}
	end := fieldPrefixSize + int(data[1])
	return string(data[:end]), data[end:], true
}

// refreshCredentialsLocked updates each stored login that user names. Only the
// signed-in user has a known numeric ID, so a change by ID reaches the
// configured login only when it is the signed-in user.
func (c *IggyTcpClient) refreshCredentialsLocked(epoch *signInEpoch, user string, username, password *string) {
	if c.epoch.Load() != epoch {
		return
	}
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
