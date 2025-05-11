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
	binaryserialization "github.com/iggy-rs/iggy-go-client/binary_serialization"
	. "github.com/iggy-rs/iggy-go-client/contracts"
	ierror "github.com/iggy-rs/iggy-go-client/errors"
)

func (tms *IggyTcpClient) GetUser(identifier Identifier) (*UserResponse, error) {
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

func (tms *IggyTcpClient) GetUsers() ([]*UserResponse, error) {
	buffer, err := tms.sendAndFetchResponse([]byte{}, GetUsersCode)
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeUsers(buffer)
}

func (tms *IggyTcpClient) CreateUser(request CreateUserRequest) error {
	message := binaryserialization.SerializeCreateUserRequest(request)
	_, err := tms.sendAndFetchResponse(message, CreateUserCode)
	return err
}

func (tms *IggyTcpClient) UpdateUser(request UpdateUserRequest) error {
	message := binaryserialization.SerializeUpdateUser(request)
	_, err := tms.sendAndFetchResponse(message, UpdateUserCode)
	return err
}

func (tms *IggyTcpClient) DeleteUser(identifier Identifier) error {
	message := binaryserialization.SerializeIdentifier(identifier)
	_, err := tms.sendAndFetchResponse(message, DeleteUserCode)
	return err
}

func (tms *IggyTcpClient) UpdateUserPermissions(request UpdateUserPermissionsRequest) error {
	message := binaryserialization.SerializeUpdateUserPermissionsRequest(request)
	_, err := tms.sendAndFetchResponse(message, UpdatePermissionsCode)
	return err
}

func (tms *IggyTcpClient) ChangePassword(request ChangePasswordRequest) error {
	message := binaryserialization.SerializeChangePasswordRequest(request)
	_, err := tms.sendAndFetchResponse(message, ChangePasswordCode)
	return err
}
