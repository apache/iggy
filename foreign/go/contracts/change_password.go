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

type ChangePassword struct {
	UserID          Identifier `json:"-"`
	CurrentPassword string     `json:"CurrentPassword"`
	NewPassword     string     `json:"NewPassword"`
}

func (c *ChangePassword) Code() CommandCode {
	return ChangePasswordCode
}

func (c *ChangePassword) MarshalBinary() ([]byte, error) {
	userIdBytes, err := c.UserID.MarshalBinary()
	if err != nil {
		return nil, err
	}
	length := len(userIdBytes) + len(c.CurrentPassword) + len(c.NewPassword) + 2
	bytes := make([]byte, length)
	position := 0

	copy(bytes[position:position+len(userIdBytes)], userIdBytes)
	position += len(userIdBytes)

	bytes[position] = byte(len(c.CurrentPassword))
	position++
	copy(bytes[position:position+len(c.CurrentPassword)], c.CurrentPassword)
	position += len(c.CurrentPassword)

	bytes[position] = byte(len(c.NewPassword))
	position++
	copy(bytes[position:position+len(c.NewPassword)], c.NewPassword)

	return bytes, nil
}
