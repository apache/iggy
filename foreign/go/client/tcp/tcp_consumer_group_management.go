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

package tcp

import (
	"context"
	"time"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
)

func (c *IggyTcpClient) GetConsumerGroups(ctx context.Context, streamId, topicId iggcon.Identifier) ([]iggcon.ConsumerGroup, error) {
	buffer, err := c.do(ctx, &command.GetConsumerGroups{
		StreamId: streamId,
		TopicId:  topicId,
	})
	if err != nil {
		return nil, err
	}

	return binaryserialization.DeserializeConsumerGroups(buffer), err
}

func (c *IggyTcpClient) GetConsumerGroup(ctx context.Context, streamId, topicId, groupId iggcon.Identifier) (*iggcon.ConsumerGroupDetails, error) {
	buffer, err := c.do(ctx, &command.GetConsumerGroup{
		TopicPath: command.TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	})
	if err != nil {
		return nil, err
	}
	if len(buffer) == 0 {
		return nil, ierror.ErrConsumerGroupIdNotFound
	}

	consumerGroupDetails := binaryserialization.DeserializeConsumerGroup(buffer)
	return consumerGroupDetails, err
}

func (c *IggyTcpClient) CreateConsumerGroup(ctx context.Context, streamId iggcon.Identifier, topicId iggcon.Identifier, name string) (*iggcon.ConsumerGroupDetails, error) {
	if MaxStringLength < len(name) || len(name) == 0 {
		return nil, ierror.ErrInvalidConsumerGroupName
	}
	buffer, err := c.do(ctx, &command.CreateConsumerGroup{
		TopicPath: command.TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		Name: name,
	})
	if err != nil {
		return nil, err
	}
	consumerGroup := binaryserialization.DeserializeConsumerGroup(buffer)
	return consumerGroup, err
}

func (c *IggyTcpClient) DeleteConsumerGroup(ctx context.Context, streamId iggcon.Identifier, topicId iggcon.Identifier, groupId iggcon.Identifier) error {
	_, err := c.do(ctx, &command.DeleteConsumerGroup{
		TopicPath: command.TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	})
	return err
}

func (c *IggyTcpClient) JoinConsumerGroup(ctx context.Context, streamId iggcon.Identifier, topicId iggcon.Identifier, groupId iggcon.Identifier) error {
	_, err := c.do(ctx, &command.JoinConsumerGroup{
		TopicPath: command.TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	})
	return err
}

func (c *IggyTcpClient) LeaveConsumerGroup(ctx context.Context, streamId iggcon.Identifier, topicId iggcon.Identifier, groupId iggcon.Identifier) error {
	_, err := c.do(ctx, &command.LeaveConsumerGroup{
		TopicPath: command.TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	})
	return err
}

// refreshMembership applies the committed request before its lane is released,
// including raw requests and operations completed through ResolveOperation.
func (c *IggyTcpClient) refreshMembership(code command.Code, body []byte) {
	if code != command.JoinGroupCode && code != command.LeaveGroupCode && code != command.DeleteGroupCode {
		return
	}
	key, ok := requestGroupKey(body)
	if !ok {
		return
	}
	if code == command.JoinGroupCode {
		c.groups.markJoined(key)
	} else {
		c.groups.markLeft(key)
	}
}

// refreshAssignment caches the assignment of a sync reply of the current
// session. A handover installs the owner of a partition in a later op than the
// join, and a primary that has not applied it refuses the read as not owned.
// So the reads of a changed assignment wait for the commit of its reply.
func (c *IggyTcpClient) refreshAssignment(operation *pendingOperation, header *[vsr.HeaderSize]byte, body []byte) {
	if operation.code != command.SyncGroupCode {
		return
	}
	key, ok := requestGroupKey(operation.frame.data[vsr.HeaderSize:])
	synced, err := binaryserialization.DeserializeConsumerGroupAssignment(body)
	if !ok || err != nil {
		return
	}
	assignment := groupAssignment{generation: synced.Generation, partitions: synced.Partitions, fetchedAt: time.Now()}
	c.mtx.Lock()
	defer c.mtx.Unlock()
	if c.sessionFence == operation.fence && c.groups.putSynced(key, assignment) {
		c.raiseMetadataWatermark(vsr.ReadReplyCommit(header))
	}
}

// requestGroupKey is the key of the group that a join, leave, delete or sync
// request body names.
func requestGroupKey(body []byte) (groupKey, bool) {
	var key groupKey
	for _, field := range []*string{&key.stream, &key.topic, &key.group} {
		if len(body) < fieldPrefixSize {
			return groupKey{}, false
		}
		end := fieldPrefixSize + int(body[1])
		if end > len(body) {
			return groupKey{}, false
		}
		*field, body = string(body[:end]), body[end:]
	}
	return key, len(body) == 0
}

func (c *IggyTcpClient) SyncConsumerGroup(
	ctx context.Context,
	streamId iggcon.Identifier,
	topicId iggcon.Identifier,
	groupId iggcon.Identifier,
) (*iggcon.ConsumerGroupAssignment, error) {
	buffer, err := c.do(ctx, &command.SyncConsumerGroup{
		TopicPath: command.TopicPath{
			StreamId: streamId,
			TopicId:  topicId,
		},
		GroupId: groupId,
	})
	if err != nil {
		return nil, err
	}
	return binaryserialization.DeserializeConsumerGroupAssignment(buffer)
}
