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

import ierror "github.com/apache/iggy/foreign/go/errors"

type Identifier struct {
	Kind   IdKind
	Length int
	Value  any
}

type IdKind int

const (
	NumericId IdKind = 1
	StringId  IdKind = 2
)

// NewNumericIdentifier creates a new identifier from the given numeric value.
func NewNumericIdentifier(value uint32) (Identifier, error) {
	if value == 0 {
		return Identifier{}, ierror.InvalidIdentifier
	}
	return Identifier{
		Kind:   NumericId,
		Length: 4,
		Value:  value,
	}, nil
}

// NewStringIdentifier creates a new identifier from the given string value.
func NewStringIdentifier(value string) (Identifier, error) {
	length := len(value)
	if length == 0 || length > 255 {
		return Identifier{}, ierror.InvalidIdentifier
	}
	return Identifier{
		Kind:   StringId,
		Length: len(value),
		Value:  value,
	}, nil
}

// Uint32 returns the numeric value of the identifier.
func (id Identifier) Uint32() (uint32, error) {
	if id.Kind != NumericId || id.Length != 4 {
		return 0, ierror.ResourceNotFound
	}

	return id.Value.(uint32), nil
}

// String returns the string value of the identifier.
func (id Identifier) String() (string, error) {
	if id.Kind != StringId {
		return "", ierror.InvalidIdentifier
	}

	return id.Value.(string), nil
}
