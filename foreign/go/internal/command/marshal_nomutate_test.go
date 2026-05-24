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
	// Helper functions that create fresh Identifier instances each call,
	// so cmd and snap never share the same underlying []byte.
	numId := func(v uint32) iggcon.Identifier {
		id, _ := iggcon.NewIdentifier(v)
		return id
	}
	strId := func(v string) iggcon.Identifier {
		id, _ := iggcon.NewIdentifier(v)
		return id
	}
	newConsumer := func() iggcon.Consumer {
		return iggcon.Consumer{Kind: iggcon.ConsumerKindSingle, Id: numId(1)}
	}

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
				cmd := &GetUser{Id: numId(42)}
				snap := GetUser{Id: numId(42)}
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
				cmd := &UpdatePermissions{UserID: numId(42), Permissions: p1}
				snap := UpdatePermissions{UserID: numId(42), Permissions: p2}
				return cmd, snap
			},
		},
		{
			name: "UpdatePermissions_NilPermissions",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &UpdatePermissions{UserID: numId(42), Permissions: nil}
				snap := UpdatePermissions{UserID: numId(42), Permissions: nil}
				return cmd, snap
			},
		},
		{
			name: "ChangePassword",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &ChangePassword{UserID: numId(42), CurrentPassword: "old", NewPassword: "new"}
				snap := ChangePassword{UserID: numId(42), CurrentPassword: "old", NewPassword: "new"}
				return cmd, snap
			},
		},
		{
			name: "DeleteUser",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteUser{Id: numId(42)}
				snap := DeleteUser{Id: numId(42)}
				return cmd, snap
			},
		},
		{
			name: "UpdateUser_BothFields",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				u1, u2 := username, username
				s1, s2 := status, status
				cmd := &UpdateUser{UserID: numId(42), Username: &u1, Status: &s1}
				snap := UpdateUser{UserID: numId(42), Username: &u2, Status: &s2}
				return cmd, snap
			},
		},
		{
			name: "UpdateUser_NilUsername",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				s1, s2 := status, status
				cmd := &UpdateUser{UserID: numId(42), Username: nil, Status: &s1}
				snap := UpdateUser{UserID: numId(42), Username: nil, Status: &s2}
				return cmd, snap
			},
		},
		{
			name: "UpdateUser_NilStatus",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				u1, u2 := username, username
				cmd := &UpdateUser{UserID: numId(42), Username: &u1, Status: nil}
				snap := UpdateUser{UserID: numId(42), Username: &u2, Status: nil}
				return cmd, snap
			},
		},
		{
			name: "UpdateUser_BothNil",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &UpdateUser{UserID: numId(42), Username: nil, Status: nil}
				snap := UpdateUser{UserID: numId(42), Username: nil, Status: nil}
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
				cmd := &GetStream{StreamId: numId(1)}
				snap := GetStream{StreamId: numId(1)}
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
				cmd := &UpdateStream{StreamId: numId(1), Name: "new"}
				snap := UpdateStream{StreamId: numId(1), Name: "new"}
				return cmd, snap
			},
		},
		{
			name: "DeleteStream",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteStream{StreamId: numId(1)}
				snap := DeleteStream{StreamId: numId(1)}
				return cmd, snap
			},
		},
		{
			name: "CreateTopic_NilReplicationFactor",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &CreateTopic{StreamId: numId(1), PartitionsCount: 2, Name: "t", ReplicationFactor: nil}
				snap := CreateTopic{StreamId: numId(1), PartitionsCount: 2, Name: "t", ReplicationFactor: nil}
				return cmd, snap
			},
		},
		{
			name: "CreateTopic_WithReplicationFactor",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				rf1, rf2 := replicationFactor, replicationFactor
				cmd := &CreateTopic{StreamId: numId(1), PartitionsCount: 2, Name: "t", ReplicationFactor: &rf1}
				snap := CreateTopic{StreamId: numId(1), PartitionsCount: 2, Name: "t", ReplicationFactor: &rf2}
				return cmd, snap
			},
		},
		{
			name: "UpdateTopic_NilReplicationFactor",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &UpdateTopic{StreamId: numId(1), TopicId: strId("test-topic"), Name: "t", ReplicationFactor: nil}
				snap := UpdateTopic{StreamId: numId(1), TopicId: strId("test-topic"), Name: "t", ReplicationFactor: nil}
				return cmd, snap
			},
		},
		{
			name: "UpdateTopic_WithReplicationFactor",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				rf1, rf2 := replicationFactor, replicationFactor
				cmd := &UpdateTopic{StreamId: numId(1), TopicId: strId("test-topic"), Name: "t", ReplicationFactor: &rf1}
				snap := UpdateTopic{StreamId: numId(1), TopicId: strId("test-topic"), Name: "t", ReplicationFactor: &rf2}
				return cmd, snap
			},
		},
		{
			name: "GetTopic",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetTopic{StreamId: numId(1), TopicId: strId("test-topic")}
				snap := GetTopic{StreamId: numId(1), TopicId: strId("test-topic")}
				return cmd, snap
			},
		},
		{
			name: "GetTopics",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetTopics{StreamId: numId(1)}
				snap := GetTopics{StreamId: numId(1)}
				return cmd, snap
			},
		},
		{
			name: "DeleteTopic",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteTopic{StreamId: numId(1), TopicId: strId("test-topic")}
				snap := DeleteTopic{StreamId: numId(1), TopicId: strId("test-topic")}
				return cmd, snap
			},
		},
		{
			name: "CreatePartitions",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &CreatePartitions{StreamId: numId(1), TopicId: strId("test-topic"), PartitionsCount: 5}
				snap := CreatePartitions{StreamId: numId(1), TopicId: strId("test-topic"), PartitionsCount: 5}
				return cmd, snap
			},
		},
		{
			name: "DeletePartitions",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeletePartitions{StreamId: numId(1), TopicId: strId("test-topic"), PartitionsCount: 2}
				snap := DeletePartitions{StreamId: numId(1), TopicId: strId("test-topic"), PartitionsCount: 2}
				return cmd, snap
			},
		},
		{
			name: "CreateConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &CreateConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, Name: "cg"}
				snap := CreateConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, Name: "cg"}
				return cmd, snap
			},
		},
		{
			name: "GetConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, GroupId: numId(10)}
				snap := GetConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, GroupId: numId(10)}
				return cmd, snap
			},
		},
		{
			name: "GetConsumerGroups",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetConsumerGroups{StreamId: numId(1), TopicId: strId("test-topic")}
				snap := GetConsumerGroups{StreamId: numId(1), TopicId: strId("test-topic")}
				return cmd, snap
			},
		},
		{
			name: "JoinConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &JoinConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, GroupId: numId(10)}
				snap := JoinConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, GroupId: numId(10)}
				return cmd, snap
			},
		},
		{
			name: "LeaveConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &LeaveConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, GroupId: numId(10)}
				snap := LeaveConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, GroupId: numId(10)}
				return cmd, snap
			},
		},
		{
			name: "DeleteConsumerGroup",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, GroupId: numId(10)}
				snap := DeleteConsumerGroup{TopicPath: TopicPath{StreamId: numId(1), TopicId: strId("test-topic")}, GroupId: numId(10)}
				return cmd, snap
			},
		},
		{
			name: "StoreConsumerOffsetRequest_WithPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				p1, p2 := partitionId, partitionId
				cmd := &StoreConsumerOffsetRequest{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: &p1, Offset: 100}
				snap := StoreConsumerOffsetRequest{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: &p2, Offset: 100}
				return cmd, snap
			},
		},
		{
			name: "StoreConsumerOffsetRequest_NilPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &StoreConsumerOffsetRequest{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: nil, Offset: 100}
				snap := StoreConsumerOffsetRequest{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: nil, Offset: 100}
				return cmd, snap
			},
		},
		{
			name: "GetConsumerOffset_WithPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				p1, p2 := partitionId, partitionId
				cmd := &GetConsumerOffset{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: &p1}
				snap := GetConsumerOffset{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: &p2}
				return cmd, snap
			},
		},
		{
			name: "GetConsumerOffset_NilPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &GetConsumerOffset{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: nil}
				snap := GetConsumerOffset{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: nil}
				return cmd, snap
			},
		},
		{
			name: "DeleteConsumerOffset_WithPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				p1, p2 := partitionId, partitionId
				cmd := &DeleteConsumerOffset{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: &p1}
				snap := DeleteConsumerOffset{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: &p2}
				return cmd, snap
			},
		},
		{
			name: "DeleteConsumerOffset_NilPartition",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteConsumerOffset{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: nil}
				snap := DeleteConsumerOffset{StreamId: numId(1), TopicId: strId("test-topic"), Consumer: newConsumer(), PartitionId: nil}
				return cmd, snap
			},
		},
		{
			name: "DeleteSegments",
			makeCMD: func() (interface{ MarshalBinary() ([]byte, error) }, interface{}) {
				cmd := &DeleteSegments{StreamId: numId(1), TopicId: strId("test-topic"), PartitionId: 1, SegmentsCount: 5}
				snap := DeleteSegments{StreamId: numId(1), TopicId: strId("test-topic"), PartitionId: 1, SegmentsCount: 5}
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
