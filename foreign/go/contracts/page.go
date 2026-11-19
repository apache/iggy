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

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"unicode/utf8"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	// PageCursorPrefix starts the text form of a page cursor.
	PageCursorPrefix = "pg1_"
	// DefaultPageLimit is the page size NewPageQuery sets.
	DefaultPageLimit uint32 = 100
	// MaxPageLimit is the most items one page returns.
	MaxPageLimit uint32 = 1000
	// MaxPageCursorSize is the most bytes of a cursor: its length on the
	// wire is one byte.
	MaxPageCursorSize = 255
	// MaxPageNameSize is the most bytes of a name filter.
	MaxPageNameSize = 255
)

const maxPageCursorTextLength = len(PageCursorPrefix) + (MaxPageCursorSize*4+2)/3

// PageOrder is the order of a list page.
type PageOrder uint8

const (
	// PageOrderID walks the numeric ids.
	PageOrderID PageOrder = 0
	// PageOrderCreated walks the creation order. Entities created at the same
	// time keep the order of their ids. Partitions and consumer groups walk by
	// id in this order too, because their ids follow creation.
	PageOrderCreated PageOrder = 1
)

// PageCursor is the position after which the next page of a list starts.
// Send it back as you got it, with the query that returned it: it holds no
// order, direction, filter or parent. Only the server reads its bytes.
//
// Its text form is PageCursorPrefix followed by unpadded Base64url of its
// bytes.
type PageCursor struct {
	// payload is a string, so no copy of the cursor can change its bytes.
	payload string
}

// PageQuery is one page request of a list command. NewPageQuery fills the
// default limit: a zero Limit is refused.
type PageQuery struct {
	Order      PageOrder
	Descending bool
	// Limit caps the items of the page, 1 to MaxPageLimit.
	Limit uint32
	// Cursor is the Next of the previous page, nil for the first page.
	Cursor *PageCursor
}

// NamedPageQuery is a PageQuery with a name filter, for the lists whose
// entries have a name: streams, topics, consumer groups and users.
type NamedPageQuery struct {
	PageQuery
	// Name keeps only the entries whose name holds this text, case-sensitive.
	// Nil keeps every entry.
	Name *string
}

// Page is one page of a list. A page can be short or empty and still have a
// Next.
type Page[T any] struct {
	Items []T
	// Next is the cursor of the next page, nil on the last page.
	Next *PageCursor
}

// NewPageQuery builds the query of a first page in ascending id order with
// the default limit.
func NewPageQuery() PageQuery {
	return PageQuery{Order: PageOrderID, Limit: DefaultPageLimit}
}

// ParsePageCursor reads the text form of a cursor. Anything but the prefix
// and canonical unpadded Base64url of 1 to MaxPageCursorSize bytes is
// ierror.ErrInvalidCommand. The bytes themselves are not checked.
func ParsePageCursor(text string) (PageCursor, error) {
	if len(text) > maxPageCursorTextLength {
		return PageCursor{}, ierror.ErrInvalidCommand
	}
	payload, err := decodeCanonicalBase64(text, PageCursorPrefix)
	if err != nil {
		return PageCursor{}, err
	}
	var cursor PageCursor
	if err := cursor.UnmarshalBinary(payload); err != nil {
		return PageCursor{}, ierror.ErrInvalidCommand
	}
	return cursor, nil
}

func (c PageCursor) String() string {
	return PageCursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(c.payload))
}

// MarshalText renders the cursor's text form, so text encoders such as
// encoding/json keep the position.
func (c PageCursor) MarshalText() ([]byte, error) {
	return []byte(c.String()), nil
}

// UnmarshalText reads the text form with the checks of ParsePageCursor.
func (c *PageCursor) UnmarshalText(text []byte) error {
	cursor, err := ParsePageCursor(string(text))
	if err != nil {
		return err
	}
	*c = cursor
	return nil
}

// UnmarshalBinary keeps a copy of 1 to MaxPageCursorSize cursor bytes.
func (c *PageCursor) UnmarshalBinary(data []byte) error {
	if len(data) == 0 || len(data) > MaxPageCursorSize {
		return fmt.Errorf("%w: page cursor of %d bytes", ierror.ErrInvalidFormat, len(data))
	}
	c.payload = string(data)
	return nil
}

// Validate applies the page rules the server enforces, so a query that breaks
// one is refused with ierror.ErrInvalidCommand before anything is sent: a
// limit outside 1 to MaxPageLimit, an unknown order and a cursor without
// bytes.
func (q PageQuery) Validate() error {
	if q.Limit < 1 || q.Limit > MaxPageLimit || q.Order > PageOrderCreated {
		return ierror.ErrInvalidCommand
	}
	if q.Cursor != nil && q.Cursor.payload == "" {
		return ierror.ErrInvalidCommand
	}
	return nil
}

// Validate applies the rules of PageQuery.Validate. It also refuses a name
// that is empty, longer than MaxPageNameSize bytes or not UTF-8, with
// ierror.ErrInvalidCommand.
func (q NamedPageQuery) Validate() error {
	if err := q.PageQuery.Validate(); err != nil {
		return err
	}
	if q.Name != nil && (*q.Name == "" || len(*q.Name) > MaxPageNameSize || !utf8.ValidString(*q.Name)) {
		return ierror.ErrInvalidCommand
	}
	return nil
}

// AppendBinary validates the query, then appends the page block that ends
// every list request: [order:u8][descending:u8][limit:u32][cursor_len:u8]
// [cursor][name_len:u8][name]. A length of 0 means no cursor or no name. A
// PageQuery has no name, so its name length is always 0.
func (q PageQuery) AppendBinary(b []byte) ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	return q.appendBlock(b, ""), nil
}

// AppendBinary validates the query, then appends the page block of
// PageQuery.AppendBinary with the name.
func (q NamedPageQuery) AppendBinary(b []byte) ([]byte, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	var name string
	if q.Name != nil {
		name = *q.Name
	}
	return q.appendBlock(b, name), nil
}

func (q PageQuery) appendBlock(b []byte, name string) []byte {
	var cursor string
	if q.Cursor != nil {
		cursor = q.Cursor.payload
	}
	b = append(b, byte(q.Order), boolToByte(q.Descending))
	b = binary.LittleEndian.AppendUint32(b, q.Limit)
	b = append(b, byte(len(cursor)))
	b = append(b, cursor...)
	b = append(b, byte(len(name)))
	return append(b, name...)
}
