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

type CreateUser struct {
	Username    string       `json:"username"`
	Password    string       `json:"Password"`
	Status      UserStatus   `json:"Status"`
	Permissions *Permissions `json:"Permissions,omitempty"`
}

func (c *CreateUser) Code() CommandCode {
	return CreateUserCode
}

func (c *CreateUser) MarshalBinary() ([]byte, error) {
	capacity := 4 + len(c.Username) + len(c.Password)
	if c.Permissions != nil {
		capacity += 1 + 4 + c.Permissions.Size()
	}

	bytes := make([]byte, capacity)
	position := 0

	bytes[position] = byte(len(c.Username))
	position += 1
	copy(bytes[position:position+len(c.Username)], []byte(c.Username))
	position += len(c.Username)

	bytes[position] = byte(len(c.Password))
	position += 1
	copy(bytes[position:position+len(c.Password)], []byte(c.Password))
	position += len(c.Password)

	statusByte := byte(0)
	switch c.Status {
	case Active:
		statusByte = byte(1)
	case Inactive:
		statusByte = byte(2)
	}
	bytes[position] = statusByte
	position += 1

	if c.Permissions != nil {
		bytes[position] = byte(1)
		position += 1
		permissionsBytes, err := c.Permissions.MarshalBinary()
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(bytes[position:position+4], uint32(len(permissionsBytes)))
		position += 4
		copy(bytes[position:position+len(permissionsBytes)], permissionsBytes)
	} else {
		bytes[position] = byte(0)
	}

	return bytes, nil
}
