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
	"reflect"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// TestMarshalBinary_DoesNotMutateReceiver verifies that calling MarshalBinary
// does not modify any field on the receiver for every command type.
func TestMarshalBinary_DoesNotMutateReceiver(t *testing.T) {
	streamId, _ := iggcon.NewIdentifier(uint32(1))
	topicId, _ := iggcon.NewIdentifier("test-topic")
	userId, _ := iggcon.NewIdentifier(uint32(42))
	groupId, _ := iggcon.NewIdentifier(uint32(10))

	consumerId, _ := iggcon.NewIdentifier(uint32(1))
	consumer := iggcon.Consumer{Kind: iggcon.ConsumerKindSingle, Id: consumerId}

	partitionId := uint32(3)
	username := "admin"
	status := iggcon.Active
	replicationFactor := uint8(1)

	tests := []struct {
		name    string
		makeCMD func() (cmd interface{ MarshalBinary() ([]byte, error) }, snapshot interface{})
	}{
		{
			name: "CreatePersonalAccessToken",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &CreatePersonalAccessToken{Name: "token", Expiry: 3600}
				snap := CreatePersonalAccessToken{Name: "token", Expiry: 3600}
				return cmd, snap
			},
		},
		{
			name: "DeletePersonalAccessToken",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeletePersonalAccessToken{Name: "token"}
				snap := DeletePersonalAccessToken{Name: "token"}
				return cmd, snap
			},
		},
		{
			name: "GetPersonalAccessTokens",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetPersonalAccessTokens{}
				snap := GetPersonalAccessTokens{}
				return cmd, snap
			},
		},
		{
			name: "CreateUser",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				p1 := &iggcon.Permissions{Global: iggcon.GlobalPermissions{ManageServers: true, ReadServers: true}}
				p2 := &iggcon.Permissions{Global: iggcon.GlobalPermissions{ManageServers: true, ReadServers: true}}
				cmd := &CreateUser{Username: "user", Password: "pass", Status: iggcon.Active, Permissions: p1}
				snap := CreateUser{Username: "user", Password: "pass", Status: iggcon.Active, Permissions: p2}
				return cmd, snap
			},
		},
		{
			name: "CreateUser_NilPermissions",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &CreateUser{Username: "user", Password: "pass", Status: iggcon.Active, Permissions: nil}
				snap := CreateUser{Username: "user", Password: "pass", Status: iggcon.Active, Permissions: nil}
				return cmd, snap
			},
		},
		{
			name: "GetUser",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetUser{Id: userId}
				snap := GetUser{Id: userId}
				return cmd, snap
			},
		},
		{
			name: "GetUsers",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetUsers{}
				snap := GetUsers{}
				return cmd, snap
			},
		},
		{
			name: "UpdatePermissions_WithPermissions",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				p1 := &iggcon.Permissions{Global: iggcon.GlobalPermissions{ManageServers: true, ReadServers: true}}
				p2 := &iggcon.Permissions{Global: iggcon.GlobalPermissions{ManageServers: true, ReadServers: true}}
				cmd := &UpdatePermissions{UserID: userId, Permissions: p1}
				snap := UpdatePermissions{UserID: userId, Permissions: p2}
				return cmd, snap
			},
		},
		{
			name: "UpdatePermissions_NilPermissions",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &UpdatePermissions{UserID: userId, Permissions: nil}
				snap := UpdatePermissions{UserID: userId, Permissions: nil}
				return cmd, snap
			},
		},
		{
			name: "ChangePassword",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &ChangePassword{UserID: userId, CurrentPassword: "old", NewPassword: "new"}
				snap := ChangePassword{UserID: userId, CurrentPassword: "old", NewPassword: "new"}
				return cmd, snap
			},
		},
		{
			name: "DeleteUser",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteUser{Id: userId}
				snap := DeleteUser{Id: userId}
				return cmd, snap
			},
		},
		{
			name: "UpdateUser_BothFields",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				u1, u2 := username, username
				s1, s2 := status, status
				cmd := &UpdateUser{UserID: userId, Username: &u1, Status: &s1}
				snap := UpdateUser{UserID: userId, Username: &u2, Status: &s2}
				return cmd, snap
			},
		},
		{
			name: "UpdateUser_NilUsername",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				s1, s2 := status, status
				cmd := &UpdateUser{UserID: userId, Username: nil, Status: &s1}
				snap := UpdateUser{UserID: userId, Username: nil, Status: &s2}
				return cmd, snap
			},
		},
		{
			name: "UpdateUser_NilStatus",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				u1, u2 := username, username
				cmd := &UpdateUser{UserID: userId, Username: &u1, Status: nil}
				snap := UpdateUser{UserID: userId, Username: &u2, Status: nil}
				return cmd, snap
			},
		},
		{
			name: "UpdateUser_BothNil",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &UpdateUser{UserID: userId, Username: nil, Status: nil}
				snap := UpdateUser{UserID: userId, Username: nil, Status: nil}
				return cmd, snap
			},
		},
		{
			name: "LoginUser",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &LoginUser{Username: "user", Password: "pass"}
				snap := LoginUser{Username: "user", Password: "pass"}
				return cmd, snap
			},
		},
		{
			name: "LoginWithPersonalAccessToken",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &LoginWithPersonalAccessToken{Token: "tok"}
				snap := LoginWithPersonalAccessToken{Token: "tok"}
				return cmd, snap
			},
		},
		{
			name: "LogoutUser",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &LogoutUser{}
				snap := LogoutUser{}
				return cmd, snap
			},
		},
		{
			name: "CreateStream",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &CreateStream{Name: "stream"}
				snap := CreateStream{Name: "stream"}
				return cmd, snap
			},
		},
		{
			name: "GetStream",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetStream{StreamId: streamId}
				snap := GetStream{StreamId: streamId}
				return cmd, snap
			},
		},
		{
			name: "GetStreams",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetStreams{}
				snap := GetStreams{}
				return cmd, snap
			},
		},
		{
			name: "UpdateStream",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &UpdateStream{StreamId: streamId, Name: "new"}
				snap := UpdateStream{StreamId: streamId, Name: "new"}
				return cmd, snap
			},
		},
		{
			name: "DeleteStream",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteStream{StreamId: streamId}
				snap := DeleteStream{StreamId: streamId}
				return cmd, snap
			},
		},
		{
			name: "CreateTopic_NilReplicationFactor",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &CreateTopic{StreamId: streamId, PartitionsCount: 2, Name: "t", ReplicationFactor: nil}
				snap := CreateTopic{StreamId: streamId, PartitionsCount: 2, Name: "t", ReplicationFactor: nil}
				return cmd, snap
			},
		},
		{
			name: "CreateTopic_WithReplicationFactor",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				rf1, rf2 := replicationFactor, replicationFactor
				cmd := &CreateTopic{StreamId: streamId, PartitionsCount: 2, Name: "t", ReplicationFactor: &rf1}
				snap := CreateTopic{StreamId: streamId, PartitionsCount: 2, Name: "t", ReplicationFactor: &rf2}
				return cmd, snap
			},
		},
		{
			name: "UpdateTopic_NilReplicationFactor",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &UpdateTopic{StreamId: streamId, TopicId: topicId, Name: "t", ReplicationFactor: nil}
				snap := UpdateTopic{StreamId: streamId, TopicId: topicId, Name: "t", ReplicationFactor: nil}
				return cmd, snap
			},
		},
		{
			name: "UpdateTopic_WithReplicationFactor",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				rf1, rf2 := replicationFactor, replicationFactor
				cmd := &UpdateTopic{StreamId: streamId, TopicId: topicId, Name: "t", ReplicationFactor: &rf1}
				snap := UpdateTopic{StreamId: streamId, TopicId: topicId, Name: "t", ReplicationFactor: &rf2}
				return cmd, snap
			},
		},
		{
			name: "GetTopic",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetTopic{StreamId: streamId, TopicId: topicId}
				snap := GetTopic{StreamId: streamId, TopicId: topicId}
				return cmd, snap
			},
		},
		{
			name: "GetTopics",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetTopics{StreamId: streamId}
				snap := GetTopics{StreamId: streamId}
				return cmd, snap
			},
		},
		{
			name: "DeleteTopic",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteTopic{StreamId: streamId, TopicId: topicId}
				snap := DeleteTopic{StreamId: streamId, TopicId: topicId}
				return cmd, snap
			},
		},
		{
			name: "CreatePartitions",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &CreatePartitions{StreamId: streamId, TopicId: topicId, PartitionsCount: 5}
				snap := CreatePartitions{StreamId: streamId, TopicId: topicId, PartitionsCount: 5}
				return cmd, snap
			},
		},
		{
			name: "DeletePartitions",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeletePartitions{StreamId: streamId, TopicId: topicId, PartitionsCount: 2}
				snap := DeletePartitions{StreamId: streamId, TopicId: topicId, PartitionsCount: 2}
				return cmd, snap
			},
		},
		{
			name: "CreateConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &CreateConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, Name: "cg"}
				snap := CreateConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, Name: "cg"}
				return cmd, snap
			},
		},
		{
			name: "GetConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, GroupId: groupId}
				snap := GetConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, GroupId: groupId}
				return cmd, snap
			},
		},
		{
			name: "GetConsumerGroups",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetConsumerGroups{StreamId: streamId, TopicId: topicId}
				snap := GetConsumerGroups{StreamId: streamId, TopicId: topicId}
				return cmd, snap
			},
		},
		{
			name: "JoinConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &JoinConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, GroupId: groupId}
				snap := JoinConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, GroupId: groupId}
				return cmd, snap
			},
		},
		{
			name: "LeaveConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &LeaveConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, GroupId: groupId}
				snap := LeaveConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, GroupId: groupId}
				return cmd, snap
			},
		},
		{
			name: "DeleteConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, GroupId: groupId}
				snap := DeleteConsumerGroup{TopicPath: TopicPath{StreamId: streamId, TopicId: topicId}, GroupId: groupId}
				return cmd, snap
			},
		},
		{
			name: "StoreConsumerOffsetRequest_WithPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				p1, p2 := partitionId, partitionId
				cmd := &StoreConsumerOffsetRequest{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: &p1, Offset: 100}
				snap := StoreConsumerOffsetRequest{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: &p2, Offset: 100}
				return cmd, snap
			},
		},
		{
			name: "StoreConsumerOffsetRequest_NilPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &StoreConsumerOffsetRequest{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: nil, Offset: 100}
				snap := StoreConsumerOffsetRequest{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: nil, Offset: 100}
				return cmd, snap
			},
		},
		{
			name: "GetConsumerOffset_WithPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				p1, p2 := partitionId, partitionId
				cmd := &GetConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: &p1}
				snap := GetConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: &p2}
				return cmd, snap
			},
		},
		{
			name: "GetConsumerOffset_NilPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: nil}
				snap := GetConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: nil}
				return cmd, snap
			},
		},
		{
			name: "DeleteConsumerOffset_WithPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				p1, p2 := partitionId, partitionId
				cmd := &DeleteConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: &p1}
				snap := DeleteConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: &p2}
				return cmd, snap
			},
		},
		{
			name: "DeleteConsumerOffset_NilPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: nil}
				snap := DeleteConsumerOffset{StreamId: streamId, TopicId: topicId, Consumer: consumer, PartitionId: nil}
				return cmd, snap
			},
		},
		{
			name: "DeleteSegments",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteSegments{StreamId: streamId, TopicId: topicId, PartitionId: 1, SegmentsCount: 5}
				snap := DeleteSegments{StreamId: streamId, TopicId: topicId, PartitionId: 1, SegmentsCount: 5}
				return cmd, snap
			},
		},
		{
			name: "Ping",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &Ping{}
				snap := Ping{}
				return cmd, snap
			},
		},
		{
			name: "GetStats",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetStats{}
				snap := GetStats{}
				return cmd, snap
			},
		},
		{
			name: "GetClients",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetClients{}
				snap := GetClients{}
				return cmd, snap
			},
		},
		{
			name: "GetClusterMetadata",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetClusterMetadata{}
				snap := GetClusterMetadata{}
				return cmd, snap
			},
		},
		{
			name: "GetClient",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetClient{ClientID: 99}
				snap := GetClient{ClientID: 99}
				return cmd, snap
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd, snapshot := tt.makeCMD()
			_, err := cmd.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() returned error: %v", err)
			}
			// Dereference the pointer to compare the struct value
			actual := reflect.ValueOf(cmd).Elem().Interface()
			if !reflect.DeepEqual(actual, snapshot) {
				t.Errorf("MarshalBinary() mutated the receiver.\nBefore:\t%+v\nAfter:\t%+v", snapshot, actual)
			}
		})
	}
}
