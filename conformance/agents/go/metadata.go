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

package main

// The canonical form of a metadata changes answer is the HTTP JSON that the
// fixture core/common/fixtures/metadata_changes.json names "expected". The
// views below follow the fixture test of the SDK, contracts/metadata_changes_test.go.

import (
	"encoding/base64"
	"fmt"
	"strconv"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

func changesView(changes *iggcon.MetadataChanges) (map[string]any, error) {
	view := map[string]any{
		"status":             changes.Status.String(),
		"phase":              changes.Phase.String(),
		"log_id":             nil,
		"oldest_op":          changes.OldestOp,
		"retained_since":     changes.RetainedSince,
		"head_op":            changes.HeadOp,
		"next":               nil,
		"has_more":           changes.HasMore,
		"current_page_token": nil,
	}
	if changes.LogID != nil {
		view["log_id"] = changes.LogID.String()
	}
	if changes.Next != nil {
		view["next"] = changes.Next.String()
	}
	if changes.CurrentPageToken != nil {
		view["current_page_token"] = changes.CurrentPageToken.String()
	}
	records := make([]map[string]any, 0, len(changes.Records))
	for _, record := range changes.Records {
		converted, err := recordView(record)
		if err != nil {
			return nil, err
		}
		records = append(records, converted)
	}
	items := make([]map[string]any, 0, len(changes.Items))
	for _, item := range changes.Items {
		converted, err := itemView(item)
		if err != nil {
			return nil, err
		}
		items = append(items, converted)
	}
	view["records"] = records
	view["items"] = items
	return view, nil
}

func recordView(record iggcon.MetadataChangeRecord) (map[string]any, error) {
	view := map[string]any{
		"op":        record.Op,
		"ordinal":   record.Ordinal,
		"checksum":  record.Checksum.String(),
		"timestamp": record.Timestamp,
		"group":     record.Group.String(),
		"kind":      record.Kind.String(),
	}
	if record.ActorUserID != nil {
		view["actor_user_id"] = *record.ActorUserID
	}
	if unknown, ok := record.Body.(*iggcon.MetadataUnknownBody); ok {
		view["group"] = strconv.Itoa(int(record.Group))
		view["kind"] = strconv.Itoa(int(record.Kind))
		view["body"] = base64.StdEncoding.EncodeToString(unknown.Body)
		return view, nil
	}
	return view, addBodyView(record.Body, view)
}

func itemView(item iggcon.MetadataCurrentItem) (map[string]any, error) {
	view := map[string]any{"group": item.Group.String()}
	if unknown, ok := item.Body.(*iggcon.MetadataUnknownBody); ok {
		view["group"] = strconv.Itoa(int(item.Group))
		view["body"] = base64.StdEncoding.EncodeToString(unknown.Body)
		return view, nil
	}
	return view, addBodyView(item.Body, view)
}

func addBodyView(body iggcon.MetadataChangeBody, view map[string]any) error {
	switch body := body.(type) {
	case *iggcon.MetadataStreamChange:
		addStreamKey(view, body.MetadataStreamKey)
		addFields(view, "created_at", body.CreatedAt, "name", body.Name)
		return addOptions(view, body.Options)
	case *iggcon.MetadataStreamDeleted:
		addStreamKey(view, body.MetadataStreamKey)
		addFields(view, "created_at", body.CreatedAt, "name", body.Name)
		addOptional(view, "requested_op", body.RequestedOp)
	case *iggcon.MetadataTopicChange:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "created_at", body.CreatedAt, "name", body.Name, "partitions_count", body.PartitionsCount)
		return addOptions(view, body.Options)
	case *iggcon.MetadataTopicItem:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "created_at", body.CreatedAt, "name", body.Name)
		return addOptions(view, body.Options)
	case *iggcon.MetadataTopicDeleted:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "created_at", body.CreatedAt, "name", body.Name)
		addOptional(view, "requested_op", body.RequestedOp)
	case *iggcon.MetadataPartitionsChanged:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "partitions_count", body.PartitionsCount, "removed_partition_ids", body.RemovedPartitionIDs)
		addOptional(view, "requested_op", body.RequestedOp)
	case *iggcon.MetadataPartitionsItem:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "partitions_count", body.PartitionsCount)
	case *iggcon.MetadataConsumerGroupCreated:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "group_id", body.GroupID, "name", body.Name, "consumer_groups_count", body.ConsumerGroupsCount)
	case *iggcon.MetadataConsumerGroupDeleted:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "group_id", body.GroupID, "name", body.Name, "consumer_groups_count", body.ConsumerGroupsCount)
		addOptional(view, "requested_op", body.RequestedOp)
	case *iggcon.MetadataConsumerGroupItem:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "group_id", body.GroupID, "name", body.Name)
	case *iggcon.MetadataMemberJoined:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "group_id", body.GroupID, "member_id", body.MemberID, "client_id", body.ClientID.String(),
			"members_count", body.MembersCount, "partition_ids", body.PartitionIDs)
	case *iggcon.MetadataMemberLeft:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "group_id", body.GroupID, "member_id", body.MemberID, "client_id", body.ClientID.String(),
			"members_count", body.MembersCount, "reason", body.Reason.String())
	case *iggcon.MetadataPartitionHandedOver:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "group_id", body.GroupID, "partition_id", body.PartitionID)
		addOptional(view, "member_id", body.MemberID)
	case *iggcon.MetadataMemberItem:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "group_id", body.GroupID, "member_id", body.MemberID, "client_id", body.ClientID.String(),
			"partition_ids", body.PartitionIDs)
	case *iggcon.MetadataUserChange:
		addUserKey(view, body.MetadataUserKey)
		addFields(view, "username", body.Username, "status", statusName(body.Status),
			"permissions", permissionsView(body.Permissions))
		return addOptions(view, body.Options)
	case *iggcon.MetadataUserIdentity:
		addUserKey(view, body.MetadataUserKey)
		addFields(view, "username", body.Username)
	case *iggcon.MetadataUserPermissionsUpdated:
		addUserKey(view, body.MetadataUserKey)
		addFields(view, "username", body.Username, "permissions", permissionsView(body.Permissions),
			"reason", body.Reason.String())
	case *iggcon.MetadataTokenChange:
		addUserKey(view, body.MetadataUserKey)
		addFields(view, "name", body.Name)
		addOptional(view, "expiry", body.Expiry)
	case *iggcon.MetadataTokenDeleted:
		addUserKey(view, body.MetadataUserKey)
		addFields(view, "name", body.Name, "reason", body.Reason.String())
	case *iggcon.MetadataSession:
		addUserKey(view, body.MetadataUserKey)
		addFields(view, "client_id", body.ClientID.String(), "session", body.Session)
	case *iggcon.MetadataSessionClosed:
		addUserKey(view, body.MetadataUserKey)
		addFields(view, "client_id", body.ClientID.String(), "session", body.Session, "reason", body.Reason.String())
	default:
		return fmt.Errorf("an unexpected metadata body %T", body)
	}
	return nil
}

func addStreamKey(view map[string]any, key iggcon.MetadataStreamKey) {
	addFields(view, "stream_id", key.StreamID, "stream_created_op", key.StreamCreatedOp)
}

func addTopicKey(view map[string]any, key iggcon.MetadataTopicKey) {
	addStreamKey(view, key.MetadataStreamKey)
	addFields(view, "topic_id", key.TopicID, "topic_created_op", key.TopicCreatedOp)
}

func addUserKey(view map[string]any, key iggcon.MetadataUserKey) {
	addFields(view, "user_id", key.UserID, "user_created_at", key.UserCreatedAt)
}

func addFields(view map[string]any, pairs ...any) {
	for index := 0; index+1 < len(pairs); index += 2 {
		if name, ok := pairs[index].(string); ok {
			view[name] = pairs[index+1]
		}
	}
}

func addOptional[T any](view map[string]any, name string, value *T) {
	if value != nil {
		view[name] = *value
	}
}

func addOptions(view map[string]any, options iggcon.MetadataOptions) error {
	if options.Unreadable {
		view["options"] = nil
		view["options_unreadable"] = true
		return nil
	}
	converted, err := canonicalOptions(options.Options, options.DerivedOptions)
	if err != nil {
		return err
	}
	view["options"] = converted
	return nil
}

func statusName(status iggcon.UserStatus) string {
	if status == iggcon.Inactive {
		return "inactive"
	}
	return "active"
}

// permissionsView renders permissions in the get_user JSON shape of the
// change log, with stream permissions.
func permissionsView(permissions *iggcon.Permissions) any {
	if permissions == nil {
		return nil
	}
	view := map[string]any{"global": globalView(permissions.Global), "streams": nil}
	if permissions.Streams == nil {
		return view
	}
	streams := make(map[string]any, len(permissions.Streams))
	for streamID, stream := range permissions.Streams {
		streamView := map[string]any{
			"manage_stream": stream.ManageStream, "read_stream": stream.ReadStream,
			"manage_topics": stream.ManageTopics, "read_topics": stream.ReadTopics,
			"poll_messages": stream.PollMessages, "send_messages": stream.SendMessages,
			"topics": nil,
		}
		if stream.Topics != nil {
			topics := make(map[string]any, len(stream.Topics))
			for topicID, topic := range stream.Topics {
				topics[strconv.Itoa(topicID)] = map[string]bool{
					"manage_topic": topic.ManageTopic, "read_topic": topic.ReadTopic,
					"poll_messages": topic.PollMessages, "send_messages": topic.SendMessages,
				}
			}
			streamView["topics"] = topics
		}
		streams[strconv.Itoa(streamID)] = streamView
	}
	view["streams"] = streams
	return view
}

func globalView(global iggcon.GlobalPermissions) map[string]bool {
	return map[string]bool{
		"manage_servers": global.ManageServers, "read_servers": global.ReadServers,
		"manage_users": global.ManageUsers, "read_users": global.ReadUsers,
		"manage_streams": global.ManageStreams, "read_streams": global.ReadStreams,
		"manage_topics": global.ManageTopics, "read_topics": global.ReadTopics,
		"poll_messages": global.PollMessages, "send_messages": global.SendMessages,
	}
}
