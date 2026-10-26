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
	"fmt"

	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	// DefaultMetadataChangesLimit is the page size NewMetadataChangesQuery
	// selects.
	DefaultMetadataChangesLimit uint32 = 100
	// MaxMetadataChangesLimit is the most records or items one page returns.
	MaxMetadataChangesLimit uint32 = 1000
)

// MetadataChangeGroup is the family of change records and the unit of
// filtering. Codes 9 to 127 are reserved for future groups, 128 to 255 for
// downstream extensions.
type MetadataChangeGroup uint8

const (
	MetadataChangeGroupStreams              MetadataChangeGroup = 1
	MetadataChangeGroupTopics               MetadataChangeGroup = 2
	MetadataChangeGroupPartitions           MetadataChangeGroup = 3
	MetadataChangeGroupConsumerGroups       MetadataChangeGroup = 4
	MetadataChangeGroupConsumerGroupMembers MetadataChangeGroup = 5
	MetadataChangeGroupUsers                MetadataChangeGroup = 6
	MetadataChangeGroupPersonalAccessTokens MetadataChangeGroup = 7
	MetadataChangeGroupSessions             MetadataChangeGroup = 8
)

// MetadataChangeKind is the kind of a change record. Existing kind bodies
// never change in place, a new layout is a new kind.
type MetadataChangeKind uint8

const (
	MetadataChangeKindStreamCreated                    MetadataChangeKind = 1
	MetadataChangeKindStreamUpdated                    MetadataChangeKind = 2
	MetadataChangeKindStreamDeleted                    MetadataChangeKind = 3
	MetadataChangeKindTopicCreated                     MetadataChangeKind = 10
	MetadataChangeKindTopicUpdated                     MetadataChangeKind = 11
	MetadataChangeKindTopicDeleted                     MetadataChangeKind = 12
	MetadataChangeKindPartitionsChanged                MetadataChangeKind = 20
	MetadataChangeKindConsumerGroupCreated             MetadataChangeKind = 30
	MetadataChangeKindConsumerGroupDeleted             MetadataChangeKind = 31
	MetadataChangeKindConsumerGroupMemberJoined        MetadataChangeKind = 40
	MetadataChangeKindConsumerGroupMemberLeft          MetadataChangeKind = 41
	MetadataChangeKindConsumerGroupPartitionHandedOver MetadataChangeKind = 42
	MetadataChangeKindUserCreated                      MetadataChangeKind = 50
	MetadataChangeKindUserUpdated                      MetadataChangeKind = 51
	MetadataChangeKindUserDeleted                      MetadataChangeKind = 52
	MetadataChangeKindUserPasswordChanged              MetadataChangeKind = 53
	MetadataChangeKindUserPermissionsUpdated           MetadataChangeKind = 54
	MetadataChangeKindPersonalAccessTokenCreated       MetadataChangeKind = 60
	MetadataChangeKindPersonalAccessTokenDeleted       MetadataChangeKind = 61
	MetadataChangeKindSessionRegistered                MetadataChangeKind = 70
	MetadataChangeKindSessionClosed                    MetadataChangeKind = 71
)

// MetadataChangeStatus is the outcome of a change log query.
type MetadataChangeStatus uint8

const (
	// MetadataChangeStatusOk carries the records or items after the requested
	// position.
	MetadataChangeStatusOk MetadataChangeStatus = 0
	// MetadataChangeStatusBehind means the answering node has not applied the
	// requested position yet. Ask again later or ask another node.
	MetadataChangeStatusBehind MetadataChangeStatus = 1
	// MetadataChangeStatusExpired means the node no longer holds every record
	// after the position. Rebuild from current state.
	MetadataChangeStatusExpired MetadataChangeStatus = 2
	// MetadataChangeStatusReset means the position belongs to another history.
	// Rebuild from current state.
	MetadataChangeStatusReset MetadataChangeStatus = 3
)

// MetadataChangePhase says whether a page holds change records or current
// state items.
type MetadataChangePhase uint8

const (
	MetadataChangePhaseChanges MetadataChangePhase = 0
	MetadataChangePhaseCurrent MetadataChangePhase = 1
)

// MetadataMemberLeftReason says why a consumer group member was removed.
type MetadataMemberLeftReason uint8

const (
	MetadataMemberLeftReasonLeft         MetadataMemberLeftReason = 0
	MetadataMemberLeftReasonSessionEnded MetadataMemberLeftReason = 1
)

// MetadataTokenDeletedReason says why a personal access token was removed.
type MetadataTokenDeletedReason uint8

const (
	MetadataTokenDeletedReasonDeleted MetadataTokenDeletedReason = 0
	MetadataTokenDeletedReasonExpired MetadataTokenDeletedReason = 1
)

// MetadataSessionClosedReason says why a session was closed.
type MetadataSessionClosedReason uint8

const (
	MetadataSessionClosedReasonLogout  MetadataSessionClosedReason = 0
	MetadataSessionClosedReasonExpired MetadataSessionClosedReason = 1
	// MetadataSessionClosedReasonRevoked is a session ended because its user
	// was deactivated or deleted.
	MetadataSessionClosedReasonRevoked MetadataSessionClosedReason = 2
)

// MetadataPermissionsUpdatedReason says why a user's public permission set
// changed.
type MetadataPermissionsUpdatedReason uint8

const (
	// MetadataPermissionsUpdatedReasonUpdated is an UpdatePermissions.
	MetadataPermissionsUpdatedReasonUpdated MetadataPermissionsUpdatedReason = 0
	// MetadataPermissionsUpdatedReasonResourceDeleted is a stream or topic
	// delete that pruned the user's grants on it.
	MetadataPermissionsUpdatedReasonResourceDeleted MetadataPermissionsUpdatedReason = 1
)

// MetadataChangeStartMode says where a change log query starts.
type MetadataChangeStartMode uint8

const (
	// MetadataChangeStartEarliest starts at the oldest operation the requested
	// groups still hold completely.
	MetadataChangeStartEarliest MetadataChangeStartMode = 0
	// MetadataChangeStartLatest returns no records, only a cursor at the
	// newest retained record.
	MetadataChangeStartLatest MetadataChangeStartMode = 1
	// MetadataChangeStartAfter returns the records after a cursor.
	MetadataChangeStartAfter MetadataChangeStartMode = 2
	// MetadataChangeStartCurrent pages the current state instead of changes.
	MetadataChangeStartCurrent MetadataChangeStartMode = 3
)

// MetadataChangeStart is where a change log query starts. Build it with
// EarliestMetadataChangeStart, LatestMetadataChangeStart,
// AfterMetadataChangeStart or CurrentMetadataChangeStart.
type MetadataChangeStart struct {
	Mode MetadataChangeStartMode
	// Cursor is the position MetadataChangeStartAfter continues from.
	Cursor MetadataChangeCursor
	// PageToken continues a MetadataChangeStartCurrent scan, nil starts one.
	PageToken *MetadataCurrentPageToken
}

// MetadataChangesQuery is one request for a page of the change log or of the
// current state. NewMetadataChangesQuery fills the default limit: a zero
// Limit is refused.
type MetadataChangesQuery struct {
	Start MetadataChangeStart
	// Groups selects the record families, empty means every group.
	Groups []MetadataChangeGroup
	// Since keeps records stamped at or after it, in microseconds since the
	// Unix epoch. Only for the earliest and after starts.
	Since *uint64
	// Until keeps records stamped at or before it, in microseconds since the
	// Unix epoch. Only for the earliest and after starts.
	Until *uint64
	// Limit caps the records or items of the page, 1 to
	// MaxMetadataChangesLimit.
	Limit uint32
}

// MetadataChanges is one page of the change log, or of the current state.
type MetadataChanges struct {
	Status MetadataChangeStatus
	Phase  MetadataChangePhase
	// LogID is the node's log id, nil before it applied op 1.
	LogID *Unsigned128
	// OldestOp is the lowest whole operation from which the requested groups
	// are complete.
	OldestOp uint64
	// RetainedSince is the timestamp above which the requested groups are
	// complete, 0 when nothing was evicted.
	RetainedSince uint64
	// HeadOp is the highest metadata operation the node applied.
	HeadOp uint64
	// Next is the cursor to send back. Present on ok, echoed on behind, nil
	// on expired and reset.
	Next *MetadataChangeCursor
	// HasMore says more records remain after Next, or another state page
	// exists.
	HasMore bool
	Records []MetadataChangeRecord
	Items   []MetadataCurrentItem
	// CurrentPageToken continues a current state scan, nil once it is
	// complete.
	CurrentPageToken *MetadataCurrentPageToken
}

// MetadataChangeRecord is one public result of a committed metadata
// operation: what is true after it.
type MetadataChangeRecord struct {
	Op      uint64
	Ordinal uint32
	// Checksum is the prepare checksum of Op.
	Checksum Unsigned128
	// Timestamp is the prepare timestamp of Op in microseconds.
	Timestamp uint64
	// ActorUserID is the acting user, nil for a server action.
	ActorUserID *uint32
	// Group and Kind are the wire codes, kept even when this build does not
	// know them.
	Group MetadataChangeGroup
	Kind  MetadataChangeKind
	// Body is the typed body of Kind, or a *MetadataUnknownBody.
	Body MetadataChangeBody
}

// MetadataCurrentItem is one item of a current state page.
type MetadataCurrentItem struct {
	// Group is the wire code, kept even when this build does not know it.
	Group MetadataChangeGroup
	// Body is the typed body of Group, or a *MetadataUnknownBody.
	Body MetadataChangeBody
}

// MetadataChangeBody is the body of a record or item. Records hold
// *MetadataStreamChange, *MetadataStreamDeleted, *MetadataTopicChange,
// *MetadataTopicDeleted, *MetadataPartitionsChanged,
// *MetadataConsumerGroupCreated, *MetadataConsumerGroupDeleted,
// *MetadataMemberJoined, *MetadataMemberLeft, *MetadataPartitionHandedOver,
// *MetadataUserChange, *MetadataUserIdentity,
// *MetadataUserPermissionsUpdated, *MetadataTokenChange,
// *MetadataTokenDeleted, *MetadataSession or *MetadataSessionClosed. Items
// hold *MetadataStreamChange, *MetadataTopicItem, *MetadataPartitionsItem,
// *MetadataConsumerGroupItem, *MetadataMemberItem, *MetadataUserChange,
// *MetadataTokenChange or *MetadataSession. Both hold *MetadataUnknownBody
// for a group or kind this build does not know.
type MetadataChangeBody interface {
	isMetadataChangeBody()
}

// MetadataStreamKey names a stream. Ids are reused after a delete, the
// creating op is not.
type MetadataStreamKey struct {
	StreamID        uint32
	StreamCreatedOp uint64
}

// MetadataTopicKey names a topic within its stream.
type MetadataTopicKey struct {
	MetadataStreamKey
	TopicID        uint32
	TopicCreatedOp uint64
}

// MetadataUserKey names a user, the identity sessions and tokens bind to.
type MetadataUserKey struct {
	UserID        uint32
	UserCreatedAt uint64
}

// MetadataOptions are the public options of a resource after a change, as
// the read commands return them. Unreadable marks stored critical state this
// build cannot read: both maps are then nil and a mirror keeps its previous
// values.
type MetadataOptions struct {
	Unreadable bool
	// Options are the keys the client set.
	Options map[string]HeaderValue
	// DerivedOptions are the values the server filled or computes.
	DerivedOptions map[string]HeaderValue
}

// MetadataStreamChange is a stream after a create or update, and a streams
// item.
type MetadataStreamChange struct {
	MetadataStreamKey
	CreatedAt uint64
	Name      string
	Options   MetadataOptions
}

// MetadataStreamDeleted is a removed stream. RequestedOp is the op of a
// deferred delete's request.
type MetadataStreamDeleted struct {
	MetadataStreamKey
	CreatedAt   uint64
	Name        string
	RequestedOp *uint64
}

// MetadataTopicChange is a topic after a create or update.
type MetadataTopicChange struct {
	MetadataTopicKey
	CreatedAt       uint64
	Name            string
	PartitionsCount uint32
	Options         MetadataOptions
}

// MetadataTopicItem is a topics item. Partition counts travel as partitions
// items.
type MetadataTopicItem struct {
	MetadataTopicKey
	CreatedAt uint64
	Name      string
	Options   MetadataOptions
}

// MetadataTopicDeleted is a removed topic.
type MetadataTopicDeleted struct {
	MetadataTopicKey
	CreatedAt   uint64
	Name        string
	RequestedOp *uint64
}

// MetadataPartitionsChanged is a partition count change.
// RemovedPartitionIDs lists the deleted ids, ascending, empty on create.
type MetadataPartitionsChanged struct {
	MetadataTopicKey
	PartitionsCount     uint32
	RemovedPartitionIDs []uint32
	RequestedOp         *uint64
}

// MetadataPartitionsItem is a partitions item.
type MetadataPartitionsItem struct {
	MetadataTopicKey
	PartitionsCount uint32
}

// MetadataConsumerGroupCreated is a created consumer group with the topic's
// resulting group count.
type MetadataConsumerGroupCreated struct {
	MetadataTopicKey
	GroupID             uint32
	Name                string
	ConsumerGroupsCount uint32
}

// MetadataConsumerGroupDeleted is a removed consumer group with the topic's
// resulting group count.
type MetadataConsumerGroupDeleted struct {
	MetadataTopicKey
	GroupID             uint32
	Name                string
	ConsumerGroupsCount uint32
	RequestedOp         *uint64
}

// MetadataConsumerGroupItem is a consumer_groups item.
type MetadataConsumerGroupItem struct {
	MetadataTopicKey
	GroupID uint32
	Name    string
}

// MetadataMemberJoined is a member that joined, with its installed
// partitions.
type MetadataMemberJoined struct {
	MetadataTopicKey
	GroupID      uint32
	MemberID     uint32
	ClientID     Unsigned128
	MembersCount uint32
	PartitionIDs []uint32
}

// MetadataMemberLeft is a member that left the group.
type MetadataMemberLeft struct {
	MetadataTopicKey
	GroupID      uint32
	MemberID     uint32
	ClientID     Unsigned128
	MembersCount uint32
	Reason       MetadataMemberLeftReason
}

// MetadataPartitionHandedOver is a completed partition handover. MemberID is
// nil when the last member left.
type MetadataPartitionHandedOver struct {
	MetadataTopicKey
	GroupID     uint32
	PartitionID uint32
	MemberID    *uint32
}

// MetadataMemberItem is a consumer_group_members item.
type MetadataMemberItem struct {
	MetadataTopicKey
	GroupID      uint32
	MemberID     uint32
	ClientID     Unsigned128
	PartitionIDs []uint32
}

// MetadataUserChange is a user after a create or update, and a users item.
type MetadataUserChange struct {
	MetadataUserKey
	Username    string
	Status      UserStatus
	Permissions *Permissions
	Options     MetadataOptions
}

// MetadataUserIdentity is a deleted user, or one whose password changed.
type MetadataUserIdentity struct {
	MetadataUserKey
	Username string
}

// MetadataUserPermissionsUpdated is a user's public permission set after a
// change.
type MetadataUserPermissionsUpdated struct {
	MetadataUserKey
	Username    string
	Permissions *Permissions
	Reason      MetadataPermissionsUpdatedReason
}

// MetadataTokenChange is a created personal access token, and a
// personal_access_tokens item. Expiry in microseconds is nil for a token that
// never expires.
type MetadataTokenChange struct {
	MetadataUserKey
	Name   string
	Expiry *uint64
}

// MetadataTokenDeleted is a removed personal access token.
type MetadataTokenDeleted struct {
	MetadataUserKey
	Name   string
	Reason MetadataTokenDeletedReason
}

// MetadataSession is a registered session, and a sessions item.
type MetadataSession struct {
	MetadataUserKey
	ClientID Unsigned128
	Session  uint64
}

// MetadataSessionClosed is a closed session.
type MetadataSessionClosed struct {
	MetadataUserKey
	ClientID Unsigned128
	Session  uint64
	Reason   MetadataSessionClosedReason
}

// MetadataUnknownBody is the raw body of a group or kind this build does not
// know. A mirror that cannot apply it stops without advancing its cursor.
type MetadataUnknownBody struct {
	Body []byte
}

var metadataChangeGroupNames = [...]string{
	MetadataChangeGroupStreams:              "streams",
	MetadataChangeGroupTopics:               "topics",
	MetadataChangeGroupPartitions:           "partitions",
	MetadataChangeGroupConsumerGroups:       "consumer_groups",
	MetadataChangeGroupConsumerGroupMembers: "consumer_group_members",
	MetadataChangeGroupUsers:                "users",
	MetadataChangeGroupPersonalAccessTokens: "personal_access_tokens",
	MetadataChangeGroupSessions:             "sessions",
}

type metadataChangeKindEntry struct {
	name  string
	group MetadataChangeGroup
}

var metadataChangeKinds = map[MetadataChangeKind]metadataChangeKindEntry{
	MetadataChangeKindStreamCreated:                    {"stream_created", MetadataChangeGroupStreams},
	MetadataChangeKindStreamUpdated:                    {"stream_updated", MetadataChangeGroupStreams},
	MetadataChangeKindStreamDeleted:                    {"stream_deleted", MetadataChangeGroupStreams},
	MetadataChangeKindTopicCreated:                     {"topic_created", MetadataChangeGroupTopics},
	MetadataChangeKindTopicUpdated:                     {"topic_updated", MetadataChangeGroupTopics},
	MetadataChangeKindTopicDeleted:                     {"topic_deleted", MetadataChangeGroupTopics},
	MetadataChangeKindPartitionsChanged:                {"partitions_changed", MetadataChangeGroupPartitions},
	MetadataChangeKindConsumerGroupCreated:             {"consumer_group_created", MetadataChangeGroupConsumerGroups},
	MetadataChangeKindConsumerGroupDeleted:             {"consumer_group_deleted", MetadataChangeGroupConsumerGroups},
	MetadataChangeKindConsumerGroupMemberJoined:        {"consumer_group_member_joined", MetadataChangeGroupConsumerGroupMembers},
	MetadataChangeKindConsumerGroupMemberLeft:          {"consumer_group_member_left", MetadataChangeGroupConsumerGroupMembers},
	MetadataChangeKindConsumerGroupPartitionHandedOver: {"consumer_group_partition_handed_over", MetadataChangeGroupConsumerGroupMembers},
	MetadataChangeKindUserCreated:                      {"user_created", MetadataChangeGroupUsers},
	MetadataChangeKindUserUpdated:                      {"user_updated", MetadataChangeGroupUsers},
	MetadataChangeKindUserDeleted:                      {"user_deleted", MetadataChangeGroupUsers},
	MetadataChangeKindUserPasswordChanged:              {"user_password_changed", MetadataChangeGroupUsers},
	MetadataChangeKindUserPermissionsUpdated:           {"user_permissions_updated", MetadataChangeGroupUsers},
	MetadataChangeKindPersonalAccessTokenCreated:       {"personal_access_token_created", MetadataChangeGroupPersonalAccessTokens},
	MetadataChangeKindPersonalAccessTokenDeleted:       {"personal_access_token_deleted", MetadataChangeGroupPersonalAccessTokens},
	MetadataChangeKindSessionRegistered:                {"session_registered", MetadataChangeGroupSessions},
	MetadataChangeKindSessionClosed:                    {"session_closed", MetadataChangeGroupSessions},
}

// metadataChangeGroupParents are the groups a current state scan of a group
// must include, so a mirror can apply the cascades of their deletes.
var metadataChangeGroupParents = [...][]MetadataChangeGroup{
	MetadataChangeGroupTopics:     {MetadataChangeGroupStreams},
	MetadataChangeGroupPartitions: {MetadataChangeGroupTopics, MetadataChangeGroupStreams},
	MetadataChangeGroupConsumerGroups: {
		MetadataChangeGroupTopics, MetadataChangeGroupStreams,
	},
	MetadataChangeGroupConsumerGroupMembers: {
		MetadataChangeGroupConsumerGroups, MetadataChangeGroupPartitions,
		MetadataChangeGroupTopics, MetadataChangeGroupStreams,
	},
	MetadataChangeGroupPersonalAccessTokens: {MetadataChangeGroupUsers},
	MetadataChangeGroupSessions:             {MetadataChangeGroupUsers},
}

var (
	metadataChangeStatusNames             = []string{"ok", "behind", "expired", "reset"}
	metadataChangePhaseNames              = []string{"changes", "current"}
	metadataMemberLeftReasonNames         = []string{"left", "session_ended"}
	metadataTokenDeletedReasonNames       = []string{"deleted", "expired"}
	metadataSessionClosedReasonNames      = []string{"logout", "expired", "revoked"}
	metadataPermissionsUpdatedReasonNames = []string{"updated", "resource_deleted"}
)

// allMetadataChangeGroups is the group mask of an empty selection.
const allMetadataChangeGroups uint8 = 0xff

// EarliestMetadataChangeStart starts at the oldest operation the requested
// groups still hold completely.
func EarliestMetadataChangeStart() MetadataChangeStart {
	return MetadataChangeStart{Mode: MetadataChangeStartEarliest}
}

// LatestMetadataChangeStart asks for a cursor at the newest retained record
// and no records.
func LatestMetadataChangeStart() MetadataChangeStart {
	return MetadataChangeStart{Mode: MetadataChangeStartLatest}
}

// AfterMetadataChangeStart asks for the records after cursor.
func AfterMetadataChangeStart(cursor MetadataChangeCursor) MetadataChangeStart {
	return MetadataChangeStart{Mode: MetadataChangeStartAfter, Cursor: cursor}
}

// CurrentMetadataChangeStart pages the current state: a nil token starts a
// scan, the token of the previous page continues it.
func CurrentMetadataChangeStart(token *MetadataCurrentPageToken) MetadataChangeStart {
	return MetadataChangeStart{Mode: MetadataChangeStartCurrent, PageToken: token}
}

// NewMetadataChangesQuery builds a query with the default limit.
func NewMetadataChangesQuery(start MetadataChangeStart, groups ...MetadataChangeGroup) MetadataChangesQuery {
	return MetadataChangesQuery{Start: start, Groups: groups, Limit: DefaultMetadataChangesLimit}
}

// ParseMetadataChangeGroup reads a group name such as "consumer_groups".
func ParseMetadataChangeGroup(name string) (MetadataChangeGroup, error) {
	for code, candidate := range metadataChangeGroupNames {
		if candidate != "" && candidate == name {
			return MetadataChangeGroup(code), nil
		}
	}
	return 0, fmt.Errorf("%w: unknown metadata change group %q", ierror.ErrInvalidCommand, name)
}

// ParseMetadataChangeKind reads a kind name such as "stream_created".
func ParseMetadataChangeKind(name string) (MetadataChangeKind, error) {
	for kind, entry := range metadataChangeKinds {
		if entry.name == name {
			return kind, nil
		}
	}
	return 0, fmt.Errorf("%w: unknown metadata change kind %q", ierror.ErrInvalidCommand, name)
}

// Validate applies the request rules the server enforces, so a query that
// breaks one is refused with ierror.ErrInvalidCommand before anything is
// sent: a limit outside 1 to MaxMetadataChangesLimit, Since above Until, a
// time bound with the latest or current start, an unknown group or start, a
// cursor at op 0 that carries a checksum or ordinal, a current scan that
// misses a parent group, and a page token issued for other groups.
func (q MetadataChangesQuery) Validate() error {
	if q.Limit < 1 || q.Limit > MaxMetadataChangesLimit {
		return ierror.ErrInvalidCommand
	}
	if q.Since != nil && q.Until != nil && *q.Since > *q.Until {
		return ierror.ErrInvalidCommand
	}
	mask, err := requestedGroupsMask(q.Groups)
	if err != nil {
		return err
	}
	hasTimeBound := q.Since != nil || q.Until != nil
	switch q.Start.Mode {
	case MetadataChangeStartEarliest:
		return nil
	case MetadataChangeStartAfter:
		if !q.Start.Cursor.isCanonical() {
			return ierror.ErrInvalidCommand
		}
		return nil
	case MetadataChangeStartLatest:
		if hasTimeBound {
			return ierror.ErrInvalidCommand
		}
		return nil
	case MetadataChangeStartCurrent:
		if hasTimeBound || missesParentGroup(mask) {
			return ierror.ErrInvalidCommand
		}
		if q.Start.PageToken != nil && q.Start.PageToken.groupsMask != mask {
			return ierror.ErrInvalidCommand
		}
		return nil
	default:
		return ierror.ErrInvalidCommand
	}
}

// IsKnown reports whether this build knows the group code.
func (g MetadataChangeGroup) IsKnown() bool {
	return g >= MetadataChangeGroupStreams && g <= MetadataChangeGroupSessions
}

func (g MetadataChangeGroup) String() string {
	if g.IsKnown() {
		return metadataChangeGroupNames[g]
	}
	return unknownName(uint8(g))
}

// Group is the group every record of the kind belongs to, zero for a kind
// this build does not know.
func (k MetadataChangeKind) Group() MetadataChangeGroup {
	return metadataChangeKinds[k].group
}

func (k MetadataChangeKind) String() string {
	if entry, ok := metadataChangeKinds[k]; ok {
		return entry.name
	}
	return unknownName(uint8(k))
}

func (s MetadataChangeStatus) String() string {
	return enumName(metadataChangeStatusNames, uint8(s))
}

func (p MetadataChangePhase) String() string {
	return enumName(metadataChangePhaseNames, uint8(p))
}

func (r MetadataMemberLeftReason) String() string {
	return enumName(metadataMemberLeftReasonNames, uint8(r))
}

func (r MetadataTokenDeletedReason) String() string {
	return enumName(metadataTokenDeletedReasonNames, uint8(r))
}

func (r MetadataSessionClosedReason) String() string {
	return enumName(metadataSessionClosedReasonNames, uint8(r))
}

func (r MetadataPermissionsUpdatedReason) String() string {
	return enumName(metadataPermissionsUpdatedReasonNames, uint8(r))
}

func (*MetadataStreamChange) isMetadataChangeBody()           {}
func (*MetadataStreamDeleted) isMetadataChangeBody()          {}
func (*MetadataTopicChange) isMetadataChangeBody()            {}
func (*MetadataTopicItem) isMetadataChangeBody()              {}
func (*MetadataTopicDeleted) isMetadataChangeBody()           {}
func (*MetadataPartitionsChanged) isMetadataChangeBody()      {}
func (*MetadataPartitionsItem) isMetadataChangeBody()         {}
func (*MetadataConsumerGroupCreated) isMetadataChangeBody()   {}
func (*MetadataConsumerGroupDeleted) isMetadataChangeBody()   {}
func (*MetadataConsumerGroupItem) isMetadataChangeBody()      {}
func (*MetadataMemberJoined) isMetadataChangeBody()           {}
func (*MetadataMemberLeft) isMetadataChangeBody()             {}
func (*MetadataPartitionHandedOver) isMetadataChangeBody()    {}
func (*MetadataMemberItem) isMetadataChangeBody()             {}
func (*MetadataUserChange) isMetadataChangeBody()             {}
func (*MetadataUserIdentity) isMetadataChangeBody()           {}
func (*MetadataUserPermissionsUpdated) isMetadataChangeBody() {}
func (*MetadataTokenChange) isMetadataChangeBody()            {}
func (*MetadataTokenDeleted) isMetadataChangeBody()           {}
func (*MetadataSession) isMetadataChangeBody()                {}
func (*MetadataSessionClosed) isMetadataChangeBody()          {}
func (*MetadataUnknownBody) isMetadataChangeBody()            {}

// maskBit is bit code - 1 of a page token's group mask.
func (g MetadataChangeGroup) maskBit() uint8 {
	if !g.IsKnown() {
		return 0
	}
	return 1 << (g - 1)
}

// requestedGroupsMask collapses duplicates, with an empty selection meaning
// every group.
func requestedGroupsMask(groups []MetadataChangeGroup) (uint8, error) {
	if len(groups) == 0 {
		return allMetadataChangeGroups, nil
	}
	var mask uint8
	for _, group := range groups {
		if !group.IsKnown() {
			return 0, ierror.ErrInvalidCommand
		}
		mask |= group.maskBit()
	}
	return mask, nil
}

func missesParentGroup(mask uint8) bool {
	for _, group := range groupsOfMask(mask) {
		for _, parent := range metadataChangeGroupParents[group] {
			if mask&parent.maskBit() == 0 {
				return true
			}
		}
	}
	return false
}

func groupsOfMask(mask uint8) []MetadataChangeGroup {
	groups := make([]MetadataChangeGroup, 0, 8)
	for group := MetadataChangeGroupStreams; group <= MetadataChangeGroupSessions; group++ {
		if mask&group.maskBit() != 0 {
			groups = append(groups, group)
		}
	}
	return groups
}

func enumName(names []string, value uint8) string {
	if int(value) < len(names) {
		return names[value]
	}
	return unknownName(value)
}

func unknownName(value uint8) string {
	return fmt.Sprintf("unknown(%d)", value)
}
