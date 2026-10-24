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
	"strings"
	"unicode/utf8"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	// MetadataChangeCursorPrefix starts the text form of a change cursor.
	MetadataChangeCursorPrefix = "mc1_"
	// MetadataCurrentPageTokenPrefix starts the text form of a current state
	// page token.
	MetadataCurrentPageTokenPrefix = "cs1_"
	// MetadataChangeCursorSize is the encoded size of a change cursor.
	MetadataChangeCursorSize = 44
)

const (
	metadataChangeCursorTextLength = len(MetadataChangeCursorPrefix) + 59
	// maxPageTokenKeySize fits a user id and a personal access token name.
	maxPageTokenKeySize    = 4 + 1 + 255
	pageTokenFixedSize     = MetadataChangeCursorSize + 8 + 1 + 1 + 2
	maxPageTokenSize       = pageTokenFixedSize + maxPageTokenKeySize
	maxPageTokenTextLength = len(MetadataCurrentPageTokenPrefix) + (maxPageTokenSize+2)/3*4
)

// Unsigned128 is an unsigned 128-bit value: a log id, a checksum or a client
// id. String renders the 32 lowercase hex characters the HTTP API uses.
type Unsigned128 struct {
	High uint64
	Low  uint64
}

// MetadataChangeCursor is the position of the last change record a caller
// consumed or scanned. Op 0 with a zero checksum and ordinal names the
// beginning of the history LogID identifies.
//
// Its text form is MetadataChangeCursorPrefix followed by unpadded Base64url
// of the 44 encoded bytes.
type MetadataChangeCursor struct {
	// LogID is the prepare checksum of op 1, the root of this history.
	LogID Unsigned128
	Op    uint64
	// Checksum is the prepare checksum of Op.
	Checksum Unsigned128
	// Ordinal is the position of the record within Op.
	Ordinal uint32
}

// MetadataCurrentPageToken continues a current state scan. Send it back
// unchanged with the same groups.
//
// Its text form is MetadataCurrentPageTokenPrefix followed by unpadded
// Base64url of the encoded token.
type MetadataCurrentPageToken struct {
	anchor       MetadataChangeCursor
	requiredHead uint64
	groupsMask   uint8
	group        MetadataChangeGroup
	lastKey      []byte
}

// ParseMetadataChangeCursor reads the text form of a change cursor. Anything
// but the prefix, the exact length and canonical unpadded Base64url of a
// cursor whose op 0 carries no checksum and ordinal is
// ierror.ErrInvalidCommand.
func ParseMetadataChangeCursor(text string) (MetadataChangeCursor, error) {
	if len(text) != metadataChangeCursorTextLength {
		return MetadataChangeCursor{}, ierror.ErrInvalidCommand
	}
	payload, err := decodeCanonicalBase64(text, MetadataChangeCursorPrefix)
	if err != nil {
		return MetadataChangeCursor{}, err
	}
	var cursor MetadataChangeCursor
	if err := cursor.UnmarshalBinary(payload); err != nil {
		return MetadataChangeCursor{}, ierror.ErrInvalidCommand
	}
	return cursor, nil
}

// ParseMetadataCurrentPageToken reads the text form of a page token. Anything
// but the prefix, canonical unpadded Base64url and a token that keeps its own
// invariants is ierror.ErrInvalidCommand.
func ParseMetadataCurrentPageToken(text string) (*MetadataCurrentPageToken, error) {
	if len(text) > maxPageTokenTextLength {
		return nil, ierror.ErrInvalidCommand
	}
	payload, err := decodeCanonicalBase64(text, MetadataCurrentPageTokenPrefix)
	if err != nil {
		return nil, err
	}
	token := &MetadataCurrentPageToken{}
	if err := token.UnmarshalBinary(payload); err != nil {
		return nil, ierror.ErrInvalidCommand
	}
	return token, nil
}

// IsZero reports whether every bit is clear.
func (v Unsigned128) IsZero() bool {
	return v.High == 0 && v.Low == 0
}

func (v Unsigned128) String() string {
	return fmt.Sprintf("%016x%016x", v.High, v.Low)
}

// String renders the cursor's text form.
func (c MetadataChangeCursor) String() string {
	var buffer [MetadataChangeCursorSize]byte
	payload, _ := c.AppendBinary(buffer[:0])
	return MetadataChangeCursorPrefix + base64.RawURLEncoding.EncodeToString(payload)
}

// AppendBinary appends the 44 wire bytes:
// [log_id:u128][op:u64][checksum:u128][ordinal:u32], little-endian.
func (c MetadataChangeCursor) AppendBinary(b []byte) ([]byte, error) {
	b = appendUnsigned128(b, c.LogID)
	b = binary.LittleEndian.AppendUint64(b, c.Op)
	b = appendUnsigned128(b, c.Checksum)
	return binary.LittleEndian.AppendUint32(b, c.Ordinal), nil
}

// UnmarshalBinary reads exactly the 44 wire bytes and refuses a cursor at op
// 0 that carries a checksum or an ordinal.
func (c *MetadataChangeCursor) UnmarshalBinary(data []byte) error {
	if len(data) != MetadataChangeCursorSize {
		return fmt.Errorf("%w: a change cursor is %d bytes, got %d",
			ierror.ErrInvalidFormat, MetadataChangeCursorSize, len(data))
	}
	cursor := MetadataChangeCursor{
		LogID:    readUnsigned128(data[0:16]),
		Op:       binary.LittleEndian.Uint64(data[16:24]),
		Checksum: readUnsigned128(data[24:40]),
		Ordinal:  binary.LittleEndian.Uint32(data[40:44]),
	}
	if !cursor.isCanonical() {
		return fmt.Errorf("%w: a cursor at op 0 carries no checksum and no ordinal", ierror.ErrInvalidFormat)
	}
	*c = cursor
	return nil
}

// String renders the token's text form.
func (t *MetadataCurrentPageToken) String() string {
	payload, _ := t.AppendBinary(make([]byte, 0, pageTokenFixedSize+len(t.lastKey)))
	return MetadataCurrentPageTokenPrefix + base64.RawURLEncoding.EncodeToString(payload)
}

// MarshalText renders the token's text form, so text encoders such as
// encoding/json keep the scan position.
func (t *MetadataCurrentPageToken) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

// UnmarshalText reads the text form with the checks of
// ParseMetadataCurrentPageToken.
func (t *MetadataCurrentPageToken) UnmarshalText(text []byte) error {
	token, err := ParseMetadataCurrentPageToken(string(text))
	if err != nil {
		return err
	}
	*t = *token
	return nil
}

// Anchor is the cursor captured before the scan's first page.
func (t *MetadataCurrentPageToken) Anchor() MetadataChangeCursor {
	return t.anchor
}

// RequiredHead is the highest head any page of the scan reported so far.
func (t *MetadataCurrentPageToken) RequiredHead() uint64 {
	return t.requiredHead
}

// Groups are the groups the scan was started with, in code order.
func (t *MetadataCurrentPageToken) Groups() []MetadataChangeGroup {
	return groupsOfMask(t.groupsMask)
}

// LastGroup is the group of the last item returned.
func (t *MetadataCurrentPageToken) LastGroup() MetadataChangeGroup {
	return t.group
}

// LastKey is a copy of the key of the last item returned, in the page token
// key layout of its group.
func (t *MetadataCurrentPageToken) LastKey() []byte {
	return append([]byte(nil), t.lastKey...)
}

// AppendBinary appends the token payload:
// [anchor:44][required_head:u64][groups_mask:u8][group:u8][key_len:u16][last_key].
func (t *MetadataCurrentPageToken) AppendBinary(b []byte) ([]byte, error) {
	b, _ = t.anchor.AppendBinary(b)
	b = binary.LittleEndian.AppendUint64(b, t.requiredHead)
	b = append(b, t.groupsMask, byte(t.group))
	b = binary.LittleEndian.AppendUint16(b, uint16(len(t.lastKey)))
	return append(b, t.lastKey...), nil
}

// UnmarshalBinary reads a token that fills data exactly and keeps its own
// invariants: a known group inside the mask, a head at or above the anchor and
// a key of that group's exact width.
func (t *MetadataCurrentPageToken) UnmarshalBinary(data []byte) error {
	if len(data) < pageTokenFixedSize {
		return fmt.Errorf("%w: a page token is at least %d bytes, got %d",
			ierror.ErrInvalidFormat, pageTokenFixedSize, len(data))
	}
	var anchor MetadataChangeCursor
	if err := anchor.UnmarshalBinary(data[:MetadataChangeCursorSize]); err != nil {
		return err
	}
	group := MetadataChangeGroup(data[MetadataChangeCursorSize+9])
	if !group.IsKnown() {
		return fmt.Errorf("%w: page token names unknown group %d", ierror.ErrInvalidFormat, uint8(group))
	}
	keyLength := int(binary.LittleEndian.Uint16(data[MetadataChangeCursorSize+10:]))
	if keyLength > maxPageTokenKeySize || len(data) != pageTokenFixedSize+keyLength {
		return fmt.Errorf("%w: page token key of %d bytes does not fill a %d byte token",
			ierror.ErrInvalidFormat, keyLength, len(data))
	}
	token := MetadataCurrentPageToken{
		anchor:       anchor,
		requiredHead: binary.LittleEndian.Uint64(data[MetadataChangeCursorSize:]),
		groupsMask:   data[MetadataChangeCursorSize+8],
		group:        group,
		lastKey:      append([]byte(nil), data[pageTokenFixedSize:]...),
	}
	if err := token.validate(); err != nil {
		return err
	}
	*t = token
	return nil
}

func (c MetadataChangeCursor) isCanonical() bool {
	return c.Op != 0 || (c.Checksum.IsZero() && c.Ordinal == 0)
}

func (t *MetadataCurrentPageToken) validate() error {
	switch {
	case t.groupsMask&t.group.maskBit() == 0:
		return fmt.Errorf("%w: page token group is not in its group mask", ierror.ErrInvalidFormat)
	case t.requiredHead < t.anchor.Op:
		return fmt.Errorf("%w: page token head is below its anchor", ierror.ErrInvalidFormat)
	case !keyFitsGroup(t.group, t.lastKey):
		return fmt.Errorf("%w: page token key of %d bytes does not fit group %s",
			ierror.ErrInvalidFormat, len(t.lastKey), t.group)
	}
	return nil
}

func keyFitsGroup(group MetadataChangeGroup, key []byte) bool {
	switch group {
	case MetadataChangeGroupStreams, MetadataChangeGroupUsers:
		return len(key) == 4
	case MetadataChangeGroupTopics, MetadataChangeGroupPartitions, MetadataChangeGroupSessions:
		return len(key) == 8
	case MetadataChangeGroupConsumerGroups:
		return len(key) == 12
	case MetadataChangeGroupConsumerGroupMembers:
		return len(key) == 16
	case MetadataChangeGroupPersonalAccessTokens:
		if len(key) <= 5 {
			return false
		}
		nameLength := int(key[4])
		return nameLength > 0 && len(key) == 5+nameLength && utf8.Valid(key[5:])
	default:
		return false
	}
}

// decodeCanonicalBase64 strips prefix and decodes unpadded Base64url that
// re-encodes to the same text, so one value has exactly one text form.
func decodeCanonicalBase64(text, prefix string) ([]byte, error) {
	encoded, found := strings.CutPrefix(text, prefix)
	if !found {
		return nil, ierror.ErrInvalidCommand
	}
	payload, err := base64.RawURLEncoding.Strict().DecodeString(encoded)
	if err != nil || base64.RawURLEncoding.EncodeToString(payload) != encoded {
		return nil, ierror.ErrInvalidCommand
	}
	return payload, nil
}

func appendUnsigned128(b []byte, value Unsigned128) []byte {
	b = binary.LittleEndian.AppendUint64(b, value.Low)
	return binary.LittleEndian.AppendUint64(b, value.High)
}

func readUnsigned128(data []byte) Unsigned128 {
	return Unsigned128{
		Low:  binary.LittleEndian.Uint64(data[0:8]),
		High: binary.LittleEndian.Uint64(data[8:16]),
	}
}
