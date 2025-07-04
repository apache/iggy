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
	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	. "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

func (tms *IggyTcpClient) GetUser(identifier Identifier) (*UserInfoDetails, error) {
	message := binaryserialization.SerializeIdentifier(identifier)
	buffer, err := tms.sendAndFetchResponse(message, GetUserCode)
	if err != nil {
		return nil, err
	}
	if len(buffer) == 0 {
		return nil, ierror.ResourceNotFound
	}

	return binaryserialization.DeserializeUser(buffer)
}

func (tms *IggyTcpClient) GetUsers() ([]UserInfo, error) {
	buffer, err := tms.sendAndFetchResponse([]byte{}, GetUsersCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeUsers(buffer)
}

func (tms *IggyTcpClient) CreateUser(username string, password string, status UserStatus, permissions *Permissions) (*UserInfoDetails, error) {
	message := binaryserialization.SerializeCreateUserRequest(CreateUserRequest{
		Username:    username,
		Password:    password,
		Status:      status,
		Permissions: permissions,
	})
	buffer, err := tms.sendAndFetchResponse(message, CreateUserCode)
	if err != nil {
		return nil, err
	}
	userInfo, err := binaryserialization.DeserializeUser(buffer)
	if err != nil {
		return nil, err
	}
	return userInfo, nil
}

func (tms *IggyTcpClient) UpdateUser(userID Identifier, username *string, status *UserStatus) error {
	message := binaryserialization.SerializeUpdateUser(UpdateUserRequest{
		UserID:   userID,
		Username: username,
		Status:   status,
	})
	_, err := tms.sendAndFetchResponse(message, UpdateUserCode)
	return err
}

func (tms *IggyTcpClient) DeleteUser(identifier Identifier) error {
	message := binaryserialization.SerializeIdentifier(identifier)
	_, err := tms.sendAndFetchResponse(message, DeleteUserCode)
	return err
}

func (tms *IggyTcpClient) UpdatePermissions(userID Identifier, permissions *Permissions) error {
	message := binaryserialization.SerializeUpdateUserPermissionsRequest(UpdatePermissionsRequest{
		UserID:      userID,
		Permissions: permissions,
	})
	_, err := tms.sendAndFetchResponse(message, UpdatePermissionsCode)
	return err
}

func (tms *IggyTcpClient) ChangePassword(userID Identifier, currentPassword string, newPassword string) error {
	message := binaryserialization.SerializeChangePasswordRequest(ChangePasswordRequest{
		UserID:          userID,
		CurrentPassword: currentPassword,
		NewPassword:     newPassword,
	})
	_, err := tms.sendAndFetchResponse(message, ChangePasswordCode)
	return err
}
