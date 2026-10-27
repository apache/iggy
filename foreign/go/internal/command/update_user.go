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

import (
	"github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// UpdateUser patches a user through the generic UpdateResource command.
type UpdateUser struct {
	UserID   iggcon.Identifier
	Username *string
	Status   *iggcon.UserStatus
	Reset    []iggcon.UserOptionKey
	RawReset []string
	Options  []iggcon.HeaderEntry
}

func (u *UpdateUser) Code() Code {
	return UpdateResourceCode
}

func (u *UpdateUser) MarshalBinary() ([]byte, error) {
	var typed []iggcon.HeaderEntry
	if u.Username != nil {
		typed = append(typed, textOption(string(iggcon.UserOptionUsername), *u.Username))
	}
	if u.Status != nil {
		status, err := userStatusName(*u.Status)
		if err != nil {
			return nil, err
		}
		typed = append(typed, stringOption(string(iggcon.UserOptionStatus), status))
	}
	reset := make([]string, 0, len(u.Reset))
	for _, key := range u.Reset {
		reset = append(reset, string(key))
	}
	return encodeUpdateResource(iggcon.OptionsScopeUser, []iggcon.Identifier{u.UserID}, typed, u.Options, reset, u.RawReset)
}

// userStatusName is the status as the catalog spells it in an update.
func userStatusName(status iggcon.UserStatus) (string, error) {
	switch status {
	case iggcon.Active:
		return "active", nil
	case iggcon.Inactive:
		return "inactive", nil
	default:
		return "", ierror.ErrInvalidUserStatus
	}
}
