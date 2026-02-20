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

package iggcon

import "encoding/binary"

type UpdatePermissions struct {
	UserID      Identifier   `json:"-"`
	Permissions *Permissions `json:"Permissions,omitempty"`
}

func (u *UpdatePermissions) Code() CommandCode {
	return UpdatePermissionsCode
}

func (u *UpdatePermissions) MarshalBinary() ([]byte, error) {
	userIdBytes, err := u.UserID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	length := len(userIdBytes)

	if u.Permissions != nil {
		length += 1 + 4 + u.Permissions.Size()
	}

	bytes := make([]byte, length)
	position := 0

	copy(bytes[position:position+len(userIdBytes)], userIdBytes)
	position += len(userIdBytes)

	if u.Permissions != nil {
		bytes[position] = 1
		position++
		permissionsBytes, err := u.Permissions.MarshalBinary()
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(len(permissionsBytes)))
		position += 4
		copy(bytes[position:position+len(permissionsBytes)], permissionsBytes)
	} else {
		bytes[position] = 0
	}

	return bytes, nil
}
