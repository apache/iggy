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

import "errors"

type HeaderValue struct {
	Kind  HeaderKind
	Value []byte
}

type HeaderKey struct {
	Value string
}

func NewHeaderKey(val string) (HeaderKey, error) {
	if len(val) == 0 || len(val) > 255 {
		return HeaderKey{}, errors.New("Value has incorrect size, must be between 1 and 255")
	}
	return HeaderKey{Value: val}, nil
}

type HeaderKind int

const (
	Raw     HeaderKind = 1
	String  HeaderKind = 2
	Bool    HeaderKind = 3
	Int8    HeaderKind = 4
	Int16   HeaderKind = 5
	Int32   HeaderKind = 6
	Int64   HeaderKind = 7
	Int128  HeaderKind = 8
	Uint8   HeaderKind = 9
	Uint16  HeaderKind = 10
	Uint32  HeaderKind = 11
	Uint64  HeaderKind = 12
	Uint128 HeaderKind = 13
	Float   HeaderKind = 14
	Double  HeaderKind = 15
)
