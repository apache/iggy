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

// UserOptionKey names a user option of the server catalog.
type UserOptionKey string

const (
	// UserOptionUsername is the user's name, set by CreateUser and changed by
	// UpdateUser.
	UserOptionUsername UserOptionKey = "username"
	// UserOptionStatus is the user's status, set by CreateUser and changed by
	// UpdateUser.
	UserOptionStatus UserOptionKey = "status"
)

// UserOptions are the options CreateUser may carry. No user option exists
// yet, so the value is empty unless Raw names a key newer than this build. A
// nil *UserOptions selects the defaults.
type UserOptions struct {
	// Raw carries keys newer than this build.
	Raw []HeaderEntry
}

// UserUpdate is a patch of a user's options. Absent keys keep their stored
// values.
type UserUpdate struct {
	// Username renames the user when set. A rename of the signed-in user also
	// becomes the username the client signs in with after a reconnect.
	Username *string
	// Status changes the user's status when set. Deactivating a user ends its
	// sessions.
	Status *UserStatus
	// Reset names keys to return to their defaults. No user key is resettable,
	// so a non-empty list is refused before anything is sent.
	Reset []UserOptionKey
	// RawReset names keys to reset that this build may not know. The server
	// decides whether each one may be reset, and a patch that carries any raw
	// name or Options entry is checked by the server alone.
	RawReset []string
	// Options carries updatable keys newer than this build. A typed field wins
	// on collision.
	Options []HeaderEntry
}
