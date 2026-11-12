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

package binaryserialization

import (
	"encoding/binary"
	"fmt"
	"unicode/utf8"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	// metadataChangeRecordMinimumSize is a record with no actor and an empty
	// body: op, ordinal, checksum, timestamp, actor flag, group, kind, length.
	metadataChangeRecordMinimumSize = 8 + 4 + 16 + 8 + 1 + 1 + 1 + 4
	// metadataCurrentItemMinimumSize is an item with an empty body.
	metadataCurrentItemMinimumSize = 1 + 4
)

// changeReader reads a response or one body front to back. The first failed
// read sticks: later reads return zero values and err reports the failure.
type changeReader struct {
	data     []byte
	position int
	err      error
}

// DeserializeMetadataChanges reads a GetMetadataChanges response.
//
// Wire format: [status:u8][phase:u8][has_log_id:u8][log_id:u128]
// [oldest_op:u64][retained_since:u64][head_op:u64][has_next:u8][cursor:44]
// [has_more:u8][records_count:u32][record]*[items_count:u32][item]*
// [token_len:u16][token]. Bodies are decoded strictly: unknown reason codes,
// presence bytes other than 0 and 1, a known kind under another group and
// trailing body bytes are refused, while a group or kind this build does not
// know is kept with its raw body. Every refusal wraps ierror.ErrInvalidFormat.
func DeserializeMetadataChanges(payload []byte) (*iggcon.MetadataChanges, error) {
	reader := &changeReader{data: payload}
	changes := &iggcon.MetadataChanges{
		Status: iggcon.MetadataChangeStatus(reader.readCode("status", uint8(iggcon.MetadataChangeStatusReset))),
		Phase:  iggcon.MetadataChangePhase(reader.readCode("phase", uint8(iggcon.MetadataChangePhaseCurrent))),
	}
	if reader.readFlag() {
		logID := reader.readUnsigned128()
		changes.LogID = &logID
	}
	changes.OldestOp = reader.readUint64()
	changes.RetainedSince = reader.readUint64()
	changes.HeadOp = reader.readUint64()
	if reader.readFlag() {
		next := reader.readCursor()
		changes.Next = &next
	}
	changes.HasMore = reader.readFlag()

	recordsCount := reader.readUint32()
	changes.Records = make([]iggcon.MetadataChangeRecord, 0,
		boundedCapacity(recordsCount, reader.remaining(), metadataChangeRecordMinimumSize))
	for range recordsCount {
		record := reader.readRecord()
		if reader.err != nil {
			break
		}
		changes.Records = append(changes.Records, record)
	}

	itemsCount := reader.readUint32()
	changes.Items = make([]iggcon.MetadataCurrentItem, 0,
		boundedCapacity(itemsCount, reader.remaining(), metadataCurrentItemMinimumSize))
	for range itemsCount {
		item := reader.readItem()
		if reader.err != nil {
			break
		}
		changes.Items = append(changes.Items, item)
	}

	if tokenLength := int(reader.readUint16()); tokenLength != 0 {
		tokenBytes := reader.take(tokenLength)
		if reader.err == nil {
			token := &iggcon.MetadataCurrentPageToken{}
			if err := token.UnmarshalBinary(tokenBytes); err != nil {
				return nil, err
			}
			changes.CurrentPageToken = token
		}
	}
	if reader.err != nil {
		return nil, reader.err
	}

	holdsOtherContent := len(changes.Items) != 0 || changes.CurrentPageToken != nil
	if changes.Phase == iggcon.MetadataChangePhaseCurrent {
		holdsOtherContent = len(changes.Records) != 0
	}
	if holdsOtherContent {
		return nil, fmt.Errorf("%w: a changes page carries only records and a current page only items",
			ierror.ErrInvalidFormat)
	}
	return changes, nil
}

func (r *changeReader) readRecord() iggcon.MetadataChangeRecord {
	record := iggcon.MetadataChangeRecord{
		Op:          r.readUint64(),
		Ordinal:     r.readUint32(),
		Checksum:    r.readUnsigned128(),
		Timestamp:   r.readUint64(),
		ActorUserID: r.readOptionalUint32(),
		Group:       iggcon.MetadataChangeGroup(r.readUint8()),
		Kind:        iggcon.MetadataChangeKind(r.readUint8()),
	}
	body := r.take(int(r.readUint32()))
	if r.err != nil {
		return record
	}
	record.Body, r.err = decodeChangeBody(record.Group, record.Kind, body)
	return record
}

func (r *changeReader) readItem() iggcon.MetadataCurrentItem {
	item := iggcon.MetadataCurrentItem{Group: iggcon.MetadataChangeGroup(r.readUint8())}
	body := r.take(int(r.readUint32()))
	if r.err != nil {
		return item
	}
	item.Body, r.err = decodeItemBody(item.Group, body)
	return item
}

// decodeChangeBody types a record body by its codes. A group or kind this
// build does not know keeps the raw body.
func decodeChangeBody(group iggcon.MetadataChangeGroup, kind iggcon.MetadataChangeKind, body []byte) (iggcon.MetadataChangeBody, error) {
	if !group.IsKnown() || !kind.Group().IsKnown() {
		return &iggcon.MetadataUnknownBody{Body: append([]byte(nil), body...)}, nil
	}
	if kind.Group() != group {
		return nil, fmt.Errorf("%w: change kind %d does not belong to group %d",
			ierror.ErrInvalidFormat, uint8(kind), uint8(group))
	}
	reader := &changeReader{data: body}
	var decoded iggcon.MetadataChangeBody
	switch kind {
	case iggcon.MetadataChangeKindStreamCreated, iggcon.MetadataChangeKindStreamUpdated:
		decoded = reader.readStreamChange()
	case iggcon.MetadataChangeKindStreamDeleted:
		decoded = &iggcon.MetadataStreamDeleted{
			MetadataStreamKey: reader.readStreamKey(),
			CreatedAt:         reader.readUint64(),
			Name:              reader.readName(),
			RequestedOp:       reader.readOptionalUint64(),
		}
	case iggcon.MetadataChangeKindTopicCreated, iggcon.MetadataChangeKindTopicUpdated:
		decoded = &iggcon.MetadataTopicChange{
			MetadataTopicKey: reader.readTopicKey(),
			CreatedAt:        reader.readUint64(),
			Name:             reader.readName(),
			PartitionsCount:  reader.readUint32(),
			Options:          reader.readOptions(),
		}
	case iggcon.MetadataChangeKindTopicDeleted:
		decoded = &iggcon.MetadataTopicDeleted{
			MetadataTopicKey: reader.readTopicKey(),
			CreatedAt:        reader.readUint64(),
			Name:             reader.readName(),
			RequestedOp:      reader.readOptionalUint64(),
		}
	case iggcon.MetadataChangeKindPartitionsChanged:
		decoded = &iggcon.MetadataPartitionsChanged{
			MetadataTopicKey:    reader.readTopicKey(),
			PartitionsCount:     reader.readUint32(),
			RemovedPartitionIDs: reader.readPartitionIDs(),
			RequestedOp:         reader.readOptionalUint64(),
		}
	case iggcon.MetadataChangeKindConsumerGroupCreated:
		decoded = &iggcon.MetadataConsumerGroupCreated{
			MetadataTopicKey:    reader.readTopicKey(),
			GroupID:             reader.readUint32(),
			Name:                reader.readName(),
			ConsumerGroupsCount: reader.readUint32(),
		}
	case iggcon.MetadataChangeKindConsumerGroupDeleted:
		decoded = &iggcon.MetadataConsumerGroupDeleted{
			MetadataTopicKey:    reader.readTopicKey(),
			GroupID:             reader.readUint32(),
			Name:                reader.readName(),
			ConsumerGroupsCount: reader.readUint32(),
			RequestedOp:         reader.readOptionalUint64(),
		}
	case iggcon.MetadataChangeKindConsumerGroupMemberJoined:
		decoded = &iggcon.MetadataMemberJoined{
			MetadataTopicKey: reader.readTopicKey(),
			GroupID:          reader.readUint32(),
			MemberID:         reader.readUint32(),
			ClientID:         reader.readUnsigned128(),
			MembersCount:     reader.readUint32(),
			PartitionIDs:     reader.readPartitionIDs(),
		}
	case iggcon.MetadataChangeKindConsumerGroupMemberLeft:
		decoded = &iggcon.MetadataMemberLeft{
			MetadataTopicKey: reader.readTopicKey(),
			GroupID:          reader.readUint32(),
			MemberID:         reader.readUint32(),
			ClientID:         reader.readUnsigned128(),
			MembersCount:     reader.readUint32(),
			Reason: iggcon.MetadataMemberLeftReason(
				reader.readCode("member left reason", uint8(iggcon.MetadataMemberLeftReasonSessionEnded))),
		}
	case iggcon.MetadataChangeKindConsumerGroupPartitionHandedOver:
		decoded = &iggcon.MetadataPartitionHandedOver{
			MetadataTopicKey: reader.readTopicKey(),
			GroupID:          reader.readUint32(),
			PartitionID:      reader.readUint32(),
			MemberID:         reader.readOptionalUint32(),
		}
	case iggcon.MetadataChangeKindUserCreated, iggcon.MetadataChangeKindUserUpdated:
		decoded = reader.readUserChange()
	case iggcon.MetadataChangeKindUserDeleted, iggcon.MetadataChangeKindUserPasswordChanged:
		decoded = &iggcon.MetadataUserIdentity{
			MetadataUserKey: reader.readUserKey(),
			Username:        reader.readName(),
		}
	case iggcon.MetadataChangeKindUserPermissionsUpdated:
		decoded = &iggcon.MetadataUserPermissionsUpdated{
			MetadataUserKey: reader.readUserKey(),
			Username:        reader.readName(),
			Permissions:     reader.readPermissions(),
			Reason: iggcon.MetadataPermissionsUpdatedReason(
				reader.readCode("permissions updated reason", uint8(iggcon.MetadataPermissionsUpdatedReasonResourceDeleted))),
		}
	case iggcon.MetadataChangeKindPersonalAccessTokenCreated:
		decoded = reader.readTokenChange()
	case iggcon.MetadataChangeKindPersonalAccessTokenDeleted:
		decoded = &iggcon.MetadataTokenDeleted{
			MetadataUserKey: reader.readUserKey(),
			Name:            reader.readName(),
			Reason: iggcon.MetadataTokenDeletedReason(
				reader.readCode("token deleted reason", uint8(iggcon.MetadataTokenDeletedReasonExpired))),
		}
	case iggcon.MetadataChangeKindSessionRegistered:
		decoded = reader.readSession()
	case iggcon.MetadataChangeKindSessionClosed:
		decoded = &iggcon.MetadataSessionClosed{
			MetadataUserKey: reader.readUserKey(),
			ClientID:        reader.readUnsigned128(),
			Session:         reader.readUint64(),
			Reason: iggcon.MetadataSessionClosedReason(
				reader.readCode("session closed reason", uint8(iggcon.MetadataSessionClosedReasonRevoked))),
		}
	}
	if err := reader.finish(); err != nil {
		return nil, err
	}
	return decoded, nil
}

// decodeItemBody types a current state item body by its group. A group this
// build does not know keeps the raw body.
func decodeItemBody(group iggcon.MetadataChangeGroup, body []byte) (iggcon.MetadataChangeBody, error) {
	reader := &changeReader{data: body}
	var decoded iggcon.MetadataChangeBody
	switch group {
	case iggcon.MetadataChangeGroupStreams:
		decoded = reader.readStreamChange()
	case iggcon.MetadataChangeGroupTopics:
		decoded = &iggcon.MetadataTopicItem{
			MetadataTopicKey: reader.readTopicKey(),
			CreatedAt:        reader.readUint64(),
			Name:             reader.readName(),
			Options:          reader.readOptions(),
		}
	case iggcon.MetadataChangeGroupPartitions:
		decoded = &iggcon.MetadataPartitionsItem{
			MetadataTopicKey: reader.readTopicKey(),
			PartitionsCount:  reader.readUint32(),
		}
	case iggcon.MetadataChangeGroupConsumerGroups:
		decoded = &iggcon.MetadataConsumerGroupItem{
			MetadataTopicKey: reader.readTopicKey(),
			GroupID:          reader.readUint32(),
			Name:             reader.readName(),
		}
	case iggcon.MetadataChangeGroupConsumerGroupMembers:
		decoded = &iggcon.MetadataMemberItem{
			MetadataTopicKey: reader.readTopicKey(),
			GroupID:          reader.readUint32(),
			MemberID:         reader.readUint32(),
			ClientID:         reader.readUnsigned128(),
			PartitionIDs:     reader.readPartitionIDs(),
		}
	case iggcon.MetadataChangeGroupUsers:
		decoded = reader.readUserChange()
	case iggcon.MetadataChangeGroupPersonalAccessTokens:
		decoded = reader.readTokenChange()
	case iggcon.MetadataChangeGroupSessions:
		decoded = reader.readSession()
	default:
		return &iggcon.MetadataUnknownBody{Body: append([]byte(nil), body...)}, nil
	}
	if err := reader.finish(); err != nil {
		return nil, err
	}
	return decoded, nil
}

func (r *changeReader) readStreamChange() *iggcon.MetadataStreamChange {
	return &iggcon.MetadataStreamChange{
		MetadataStreamKey: r.readStreamKey(),
		CreatedAt:         r.readUint64(),
		Name:              r.readName(),
		Options:           r.readOptions(),
	}
}

func (r *changeReader) readUserChange() *iggcon.MetadataUserChange {
	return &iggcon.MetadataUserChange{
		MetadataUserKey: r.readUserKey(),
		Username:        r.readName(),
		Status:          r.readUserStatus(),
		Permissions:     r.readPermissions(),
		Options:         r.readOptions(),
	}
}

// readTokenChange reads a token body, where expiry 0 means never.
func (r *changeReader) readTokenChange() *iggcon.MetadataTokenChange {
	token := &iggcon.MetadataTokenChange{
		MetadataUserKey: r.readUserKey(),
		Name:            r.readName(),
	}
	if expiry := r.readUint64(); expiry != 0 {
		token.Expiry = &expiry
	}
	return token
}

func (r *changeReader) readSession() *iggcon.MetadataSession {
	return &iggcon.MetadataSession{
		MetadataUserKey: r.readUserKey(),
		ClientID:        r.readUnsigned128(),
		Session:         r.readUint64(),
	}
}

func (r *changeReader) readStreamKey() iggcon.MetadataStreamKey {
	return iggcon.MetadataStreamKey{
		StreamID:        r.readUint32(),
		StreamCreatedOp: r.readUint64(),
	}
}

func (r *changeReader) readTopicKey() iggcon.MetadataTopicKey {
	return iggcon.MetadataTopicKey{
		MetadataStreamKey: r.readStreamKey(),
		TopicID:           r.readUint32(),
		TopicCreatedOp:    r.readUint64(),
	}
}

func (r *changeReader) readUserKey() iggcon.MetadataUserKey {
	return iggcon.MetadataUserKey{
		UserID:        r.readUint32(),
		UserCreatedAt: r.readUint64(),
	}
}

// readOptions reads [state:u8], then for state 0 the explicit and derived
// blocks exactly as the stream, topic and user reads carry them.
func (r *changeReader) readOptions() iggcon.MetadataOptions {
	offset := r.position
	switch state := r.readUint8(); {
	case r.err != nil:
		return iggcon.MetadataOptions{}
	case state == 1:
		return iggcon.MetadataOptions{Unreadable: true}
	case state != 0:
		r.fail("options state %d at offset %d", state, offset)
		return iggcon.MetadataOptions{}
	}
	explicit, consumed, err := deserializeOptions(r.data, r.position)
	if err != nil {
		r.fail("options at offset %d: %v", r.position, err)
		return iggcon.MetadataOptions{}
	}
	r.position += consumed
	derived, consumed, err := deserializeOptions(r.data, r.position)
	if err != nil {
		r.fail("derived options at offset %d: %v", r.position, err)
		return iggcon.MetadataOptions{}
	}
	r.position += consumed
	return iggcon.MetadataOptions{Options: explicit, DerivedOptions: derived}
}

// readPermissions reads [has:u8] and the permission set without a length
// prefix. Grant bytes read as booleans the way the server's decoder does.
func (r *changeReader) readPermissions() *iggcon.Permissions {
	if !r.readFlag() {
		return nil
	}
	permissions := &iggcon.Permissions{Global: iggcon.GlobalPermissions{
		ManageServers: r.readBool(),
		ReadServers:   r.readBool(),
		ManageUsers:   r.readBool(),
		ReadUsers:     r.readBool(),
		ManageStreams: r.readBool(),
		ReadStreams:   r.readBool(),
		ManageTopics:  r.readBool(),
		ReadTopics:    r.readBool(),
		PollMessages:  r.readBool(),
		SendMessages:  r.readBool(),
	}}
	if !r.readBool() {
		return permissions
	}
	permissions.Streams = make(map[int]*iggcon.StreamPermissions)
	for r.err == nil {
		streamID := int(r.readUint32())
		stream := &iggcon.StreamPermissions{
			ManageStream: r.readBool(),
			ReadStream:   r.readBool(),
			ManageTopics: r.readBool(),
			ReadTopics:   r.readBool(),
			PollMessages: r.readBool(),
			SendMessages: r.readBool(),
		}
		if r.readBool() {
			stream.Topics = make(map[int]*iggcon.TopicPermissions)
			for r.err == nil {
				topicID := int(r.readUint32())
				stream.Topics[topicID] = &iggcon.TopicPermissions{
					ManageTopic:  r.readBool(),
					ReadTopic:    r.readBool(),
					PollMessages: r.readBool(),
					SendMessages: r.readBool(),
				}
				if !r.readBool() {
					break
				}
			}
		}
		permissions.Streams[streamID] = stream
		if !r.readBool() {
			break
		}
	}
	return permissions
}

// readPartitionIDs reads [count:u32][id:u32]*, refusing ids that are not
// strictly ascending.
func (r *changeReader) readPartitionIDs() []uint32 {
	count := r.readUint32()
	ids := make([]uint32, 0, boundedCapacity(count, r.remaining(), 4))
	for range count {
		id := r.readUint32()
		if r.err != nil {
			break
		}
		if len(ids) > 0 && ids[len(ids)-1] >= id {
			r.fail("partition ids must be strictly ascending at offset %d", r.position-4)
			break
		}
		ids = append(ids, id)
	}
	return ids
}

func (r *changeReader) readUserStatus() iggcon.UserStatus {
	offset := r.position
	switch status := r.readUint8(); {
	case r.err != nil:
		return iggcon.Active
	case status == 1:
		return iggcon.Active
	case status == 2:
		return iggcon.Inactive
	default:
		r.fail("user status %d at offset %d", status, offset)
		return iggcon.Active
	}
}

// readName reads a 1 to 255 byte UTF-8 name behind its u8 length.
func (r *changeReader) readName() string {
	offset := r.position
	length := int(r.readUint8())
	if r.err == nil && length == 0 {
		r.fail("empty name at offset %d", offset)
	}
	name := r.take(length)
	if r.err == nil && !utf8.Valid(name) {
		r.fail("name at offset %d is not UTF-8", offset)
	}
	return string(name)
}

func (r *changeReader) readCursor() iggcon.MetadataChangeCursor {
	var cursor iggcon.MetadataChangeCursor
	if data := r.take(iggcon.MetadataChangeCursorSize); r.err == nil {
		if err := cursor.UnmarshalBinary(data); err != nil {
			r.err = err
		}
	}
	return cursor
}

// readCode reads a one-byte code, refusing any above highest.
func (r *changeReader) readCode(name string, highest uint8) uint8 {
	offset := r.position
	code := r.readUint8()
	if r.err == nil && code > highest {
		r.fail("unknown %s %d at offset %d", name, code, offset)
	}
	return code
}

func (r *changeReader) readOptionalUint64() *uint64 {
	if !r.readFlag() {
		return nil
	}
	value := r.readUint64()
	return &value
}

func (r *changeReader) readOptionalUint32() *uint32 {
	if !r.readFlag() {
		return nil
	}
	value := r.readUint32()
	return &value
}

// readFlag reads a presence or boolean byte, refusing anything but 0 and 1.
func (r *changeReader) readFlag() bool {
	offset := r.position
	value := r.readUint8()
	if r.err == nil && value > 1 {
		r.fail("flag byte %d at offset %d", value, offset)
	}
	return value == 1 && r.err == nil
}

func (r *changeReader) readBool() bool {
	return r.readUint8() != 0
}

func (r *changeReader) readUnsigned128() iggcon.Unsigned128 {
	data := r.take(16)
	if data == nil {
		return iggcon.Unsigned128{}
	}
	return iggcon.Unsigned128{
		Low:  binary.LittleEndian.Uint64(data[0:8]),
		High: binary.LittleEndian.Uint64(data[8:16]),
	}
}

func (r *changeReader) readUint64() uint64 {
	if data := r.take(8); data != nil {
		return binary.LittleEndian.Uint64(data)
	}
	return 0
}

func (r *changeReader) readUint32() uint32 {
	if data := r.take(4); data != nil {
		return binary.LittleEndian.Uint32(data)
	}
	return 0
}

func (r *changeReader) readUint16() uint16 {
	if data := r.take(2); data != nil {
		return binary.LittleEndian.Uint16(data)
	}
	return 0
}

func (r *changeReader) readUint8() uint8 {
	if data := r.take(1); data != nil {
		return data[0]
	}
	return 0
}

// take returns the next count bytes, aliasing the input.
func (r *changeReader) take(count int) []byte {
	if r.err != nil {
		return nil
	}
	// A u32 length above MaxInt32 converts to a negative int where int is 32
	// bits wide.
	if count < 0 || r.remaining() < count {
		r.fail("need %d bytes at offset %d, have %d", count, r.position, r.remaining())
		return nil
	}
	data := r.data[r.position : r.position+count]
	r.position += count
	return data
}

func (r *changeReader) remaining() int {
	return len(r.data) - r.position
}

// finish refuses bytes left after a body.
func (r *changeReader) finish() error {
	if r.err == nil && r.remaining() != 0 {
		r.fail("%d trailing bytes after a change body", r.remaining())
	}
	return r.err
}

func (r *changeReader) fail(format string, arguments ...any) {
	if r.err == nil {
		r.err = fmt.Errorf("%w: %s", ierror.ErrInvalidFormat, fmt.Sprintf(format, arguments...))
	}
}
