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

package vsr

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ProtocolVersion is the packed semver of the wire contract this codec
// implements, 0.11.0 per core/binary_protocol/src/version.rs. Bump it together
// with the Rust constant on any wire-incompatible change.
var ProtocolVersion = packProtocolVersion(0, 11, 0)

// packProtocolVersion packs a semver into the ten-bits-per-field layout the
// register handshake carries.
func packProtocolVersion(major, minor, patch uint32) uint32 {
	return major<<20 | minor<<10 | patch
}

// SDKName identifies this SDK to the server during the register handshake.
const SDKName = "go-sdk"

// maxWireNameLength is the u8 length prefix ceiling shared by every
// length-prefixed name in the register bodies.
const maxWireNameLength = 255

// loginRegisterFixedLength is the fixed part of a register reply:
// user id, session, and the server protocol version, followed by a
// length-prefixed server version string.
const loginRegisterFixedLength = 17

var (
	// ErrWireNameLength is returned when a name does not fit the u8 length
	// prefix.
	ErrWireNameLength = errors.New("vsr: wire name must be 1 to 255 bytes")
	// ErrTruncatedRegisterReply is returned when a register reply is shorter
	// than its declared layout requires.
	ErrTruncatedRegisterReply = errors.New("vsr: register reply is truncated")
)

// SerializeLoginRegister builds the LoginRegister body:
// [protocol version u32][sdk name][sdk version][username][password]
// [context length u32]. Each name is a u8 length prefix followed by its bytes.
func SerializeLoginRegister(username, password, sdkVersion string) ([]byte, error) {
	if len(password) > maxWireNameLength {
		return nil, fmt.Errorf("password: %w", ErrWireNameLength)
	}
	body, err := appendVersionInfo(nil, sdkVersion)
	if err != nil {
		return nil, err
	}
	body, err = appendWireName(body, username)
	if err != nil {
		return nil, fmt.Errorf("username: %w", err)
	}
	// The password slot allows an empty value, unlike the other names.
	body = append(body, byte(len(password)))
	body = append(body, password...)
	return append(body, 0, 0, 0, 0), nil
}

// SerializeLoginRegisterWithToken builds the LoginRegisterWithPat body, where
// a personal access token takes the credential slot.
func SerializeLoginRegisterWithToken(token, sdkVersion string) ([]byte, error) {
	if len(token) > maxWireNameLength {
		return nil, fmt.Errorf("access token: %w", ErrWireNameLength)
	}
	body, err := appendVersionInfo(nil, sdkVersion)
	if err != nil {
		return nil, err
	}
	body = append(body, byte(len(token)))
	body = append(body, token...)
	return append(body, 0, 0, 0, 0), nil
}

// LoginRegisterResponse is the decoded register reply.
type LoginRegisterResponse struct {
	UserID uint32
	// Session is the fence every later request carries in its header.
	Session uint64
	// ServerProtocolVersion is the packed version the server answered with.
	ServerProtocolVersion uint32
	ServerVersion         string
}

// DecodeLoginRegister decodes
// [user_id u32][session u64][server protocol version u32][server version].
func DecodeLoginRegister(body []byte) (LoginRegisterResponse, error) {
	if len(body) < loginRegisterFixedLength {
		return LoginRegisterResponse{}, ErrTruncatedRegisterReply
	}
	versionLength := int(body[16])
	if len(body) < loginRegisterFixedLength+versionLength {
		return LoginRegisterResponse{}, ErrTruncatedRegisterReply
	}
	return LoginRegisterResponse{
		UserID:                binary.LittleEndian.Uint32(body[0:4]),
		Session:               binary.LittleEndian.Uint64(body[4:12]),
		ServerProtocolVersion: binary.LittleEndian.Uint32(body[12:16]),
		ServerVersion:         string(body[17 : 17+versionLength]),
	}, nil
}

// appendVersionInfo appends [protocol version u32][sdk name][sdk version].
func appendVersionInfo(dst []byte, sdkVersion string) ([]byte, error) {
	dst = binary.LittleEndian.AppendUint32(dst, ProtocolVersion)
	dst, err := appendWireName(dst, SDKName)
	if err != nil {
		return nil, fmt.Errorf("sdk name: %w", err)
	}
	dst, err = appendWireName(dst, sdkVersion)
	if err != nil {
		return nil, fmt.Errorf("sdk version: %w", err)
	}
	return dst, nil
}

// appendWireName appends a u8 length prefix and the UTF-8 bytes of value.
func appendWireName(dst []byte, value string) ([]byte, error) {
	if len(value) < 1 || len(value) > maxWireNameLength {
		return nil, ErrWireNameLength
	}
	dst = append(dst, byte(len(value)))
	return append(dst, value...), nil
}
