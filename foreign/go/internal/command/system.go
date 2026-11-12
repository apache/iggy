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
	"encoding/binary"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

type GetClient struct {
	ClientID uint32
}

func (c *GetClient) Code() Code {
	return GetClientCode
}

func (c *GetClient) MarshalBinary() ([]byte, error) {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, c.ClientID)
	return bytes, nil
}

type GetClients struct{}

func (c *GetClients) Code() Code {
	return GetClientsCode
}

func (c *GetClients) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

type GetClusterMetadata struct {
}

func (m *GetClusterMetadata) Code() Code {
	return GetClusterMetadataCode
}

func (m *GetClusterMetadata) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

// DescribeOptions asks for the option catalog of one resource scope.
type DescribeOptions struct {
	Scope iggcon.OptionsScope
}

func (d *DescribeOptions) Code() Code {
	return DescribeOptionsCode
}

func (d *DescribeOptions) MarshalBinary() ([]byte, error) {
	if err := d.Scope.Validate(); err != nil {
		return nil, err
	}
	return []byte{byte(d.Scope)}, nil
}

// GetMetadataChanges asks for one page of the metadata change log, or of the
// current state.
type GetMetadataChanges struct {
	Query iggcon.MetadataChangesQuery
}

// maxMetadataChangeGroups is the most group codes the one-byte count carries.
const maxMetadataChangeGroups = 255

func (g *GetMetadataChanges) Code() Code {
	return GetMetadataChangesCode
}

func (g *GetMetadataChanges) MarshalBinary() ([]byte, error) {
	size := 1 + iggcon.MetadataChangeCursorSize + 9 + 9 + 4 + 1 + len(g.Query.Groups) + 2
	return g.AppendBinary(make([]byte, 0, size))
}

// AppendBinary validates the query, then encodes [start:u8][cursor:44]
// [has_since:u8][since:u64][has_until:u8][until:u64][limit:u32]
// [groups_count:u8][group:u8]*[token_len:u16][token]. The cursor is all zero
// unless the query starts after one, and the token is empty unless it
// continues a current scan. Groups keep the caller's order.
func (g *GetMetadataChanges) AppendBinary(b []byte) ([]byte, error) {
	query := g.Query
	if err := query.Validate(); err != nil {
		return nil, err
	}
	b = append(b, byte(query.Start.Mode))
	var cursor iggcon.MetadataChangeCursor
	if query.Start.Mode == iggcon.MetadataChangeStartAfter {
		cursor = query.Start.Cursor
	}
	b, _ = cursor.AppendBinary(b)
	b = appendOptionalUint64(b, query.Since)
	b = appendOptionalUint64(b, query.Until)
	b = binary.LittleEndian.AppendUint32(b, query.Limit)
	groups := query.Groups
	if len(groups) > maxMetadataChangeGroups {
		groups = make([]iggcon.MetadataChangeGroup, 0, 8)
		var seen [256]bool
		for _, group := range query.Groups {
			if !seen[byte(group)] {
				seen[byte(group)] = true
				groups = append(groups, group)
			}
		}
	}
	b = append(b, byte(len(groups)))
	for _, group := range groups {
		b = append(b, byte(group))
	}
	tokenLengthAt := len(b)
	b = binary.LittleEndian.AppendUint16(b, 0)
	if token := query.Start.PageToken; query.Start.Mode == iggcon.MetadataChangeStartCurrent && token != nil {
		b, _ = token.AppendBinary(b)
		binary.LittleEndian.PutUint16(b[tokenLengthAt:], uint16(len(b)-tokenLengthAt-2))
	}
	return b, nil
}

type GetStats struct{}

func (c *GetStats) Code() Code {
	return GetStatsCode
}

func (c *GetStats) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

// GetMe asks for the info of the client that sent the request, so it needs no
// identifier and carries no payload.
type GetMe struct{}

func (c *GetMe) Code() Code {
	return GetMeCode
}

func (c *GetMe) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

type Ping struct{}

func (p *Ping) Code() Code {
	return PingCode
}

func (p *Ping) MarshalBinary() ([]byte, error) {
	return []byte{}, nil
}

func appendOptionalUint64(b []byte, value *uint64) []byte {
	if value == nil {
		return append(b, 0)
	}
	return binary.LittleEndian.AppendUint64(append(b, 1), *value)
}
