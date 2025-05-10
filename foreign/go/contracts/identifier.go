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

func NewIdentifier(id any) Identifier {
	var kind IdKind
	var length int

	switch v := id.(type) {
	case int:
		kind = NumericId
		length = 4
	case string:
		kind = StringId
		length = len(v)
	}

	return Identifier{
		Kind:   kind,
		Length: length,
		Value:  id,
	}
}
