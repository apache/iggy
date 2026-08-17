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

package command

import "github.com/apache/iggy/foreign/go/contracts"

type UpdateUser struct {
	UserID   iggcon.Identifier  `json:"-"`
	Username *string            `json:"username"`
	Status   *iggcon.UserStatus `json:"userStatus"`
	// Options carries the trailing options block. Users have no catalog keys
	// yet, so the server rejects every key; the block is the extension point
	// for the first one.
	Options []iggcon.HeaderEntry `json:"-"`
}

func (u *UpdateUser) Code() Code {
	return UpdateUserCode
}

func (u *UpdateUser) MarshalBinary() ([]byte, error) {
	userIdBytes, err := u.UserID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	username := ""
	if u.Username != nil {
		username = *u.Username
	}

	// Both presence flags are always written; only the payload behind a flag is
	// conditional. The options block decodes to the end of the payload, so a
	// byte of slack here is a stray zero-kind entry the server rejects.
	length := len(userIdBytes) + 2
	if len(username) != 0 {
		length += 1 + len(username)
	}
	if u.Status != nil {
		length++
	}

	optionsBytes := iggcon.GetHeadersBytes(u.Options)
	bytes := make([]byte, length+len(optionsBytes))
	position := 0

	copy(bytes[position:position+len(userIdBytes)], userIdBytes)
	position += len(userIdBytes)

	if len(username) != 0 {
		bytes[position] = 1
		position++
		bytes[position] = byte(len(username))
		position++
		copy(bytes[position:position+len(username)], username)
		position += len(username)
	} else {
		bytes[position] = 0
		position++
	}

	if u.Status != nil {
		bytes[position] = 1
		position++
		statusByte := byte(0)
		switch *u.Status {
		case iggcon.Active:
			statusByte = 1
		case iggcon.Inactive:
			statusByte = 2
		}
		bytes[position] = statusByte
		position++
	} else {
		bytes[position] = 0
		position++
	}

	copy(bytes[position:], optionsBytes)

	return bytes, nil
}
