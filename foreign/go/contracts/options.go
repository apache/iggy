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

import "fmt"

// OptionsScope is the resource whose option catalog DescribeOptions serves.
type OptionsScope uint8

const (
	OptionsScopeTopic  OptionsScope = 1
	OptionsScopeStream OptionsScope = 2
	OptionsScopeUser   OptionsScope = 3
)

// Validate rejects a scope outside the three the server knows.
func (s OptionsScope) Validate() error {
	switch s {
	case OptionsScopeTopic, OptionsScopeStream, OptionsScopeUser:
		return nil
	default:
		return fmt.Errorf("unknown options scope %d", uint8(s))
	}
}

func (s OptionsScope) String() string {
	switch s {
	case OptionsScopeTopic:
		return "topic"
	case OptionsScopeStream:
		return "stream"
	case OptionsScopeUser:
		return "user"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(s))
	}
}

// OptionSpec is one entry of a resource's option catalog.
//
// This is the discovery surface for the option keys a create command accepts: a
// key outside the catalog is refused at create, and the binary transports carry
// only the error code back, so there is no other way to learn which keys a
// given server knows.
type OptionSpec struct {
	// Key is the option key the create command accepts.
	Key string
	// DefaultValue is the key's default, carrying the kind the server encodes
	// it under. Empty for a key with no default. Reuses HeaderValue because
	// options ride the user-headers codec.
	DefaultValue HeaderValue
	// Description says what the option does, including the bounds its value is
	// checked against.
	Description string
}
