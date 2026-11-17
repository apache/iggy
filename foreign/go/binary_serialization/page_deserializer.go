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
	"fmt"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

// DeserializeStreamsPage reads a GetStreams reply.
func DeserializeStreamsPage(payload []byte) (*iggcon.Page[iggcon.Stream], error) {
	return deserializePage(payload, DeserializeToStream)
}

// DeserializeTopicsPage reads a GetTopics reply.
func DeserializeTopicsPage(payload []byte) (*iggcon.Page[iggcon.Topic], error) {
	return deserializePage(payload, DeserializeToTopic)
}

// DeserializePartitionsPage reads a GetPartitions reply.
func DeserializePartitionsPage(payload []byte) (*iggcon.Page[iggcon.PartitionContract], error) {
	return deserializePage(payload, DeserializePartition)
}

// DeserializeConsumerGroupsPage reads a GetConsumerGroups reply.
func DeserializeConsumerGroupsPage(payload []byte) (*iggcon.Page[iggcon.ConsumerGroup], error) {
	return deserializePage(payload, DeserializeToConsumerGroup)
}

// DeserializeUsersPage reads a GetUsers reply.
func DeserializeUsersPage(payload []byte) (*iggcon.Page[iggcon.UserInfo], error) {
	return deserializePage(payload, func(payload []byte, position int) (iggcon.UserInfo, int, error) {
		user, size, err := deserializeToUser(payload, position)
		if err != nil {
			return iggcon.UserInfo{}, 0, err
		}
		return *user, size, nil
	})
}

// deserializePage reads the reply of a list command:
// [next_len:u8][next][count:u32][item]*. A body short of count items, or with
// bytes after them, is refused with ierror.ErrInvalidFormat. The next cursor
// keeps its bytes as they came, because only the server reads them.
func deserializePage[T any](payload []byte, readItem func([]byte, int) (T, int, error)) (*iggcon.Page[T], error) {
	reader := &changeReader{data: payload}
	next := reader.take(int(reader.readUint8()))
	count := reader.readUint32()
	if reader.err != nil {
		return nil, reader.err
	}
	// A server never sends more than MaxPageLimit items, so a larger count
	// does not size the allocation.
	page := &iggcon.Page[T]{Items: make([]T, 0, min(count, iggcon.MaxPageLimit))}
	if len(next) > 0 {
		page.Next = &iggcon.PageCursor{}
		if err := page.Next.UnmarshalBinary(next); err != nil {
			return nil, err
		}
	}
	for index := range count {
		item, size, err := readItem(payload, reader.position)
		if err != nil {
			return nil, fmt.Errorf("%w: item %d of %d of a page: %w", ierror.ErrInvalidFormat, index, count, err)
		}
		page.Items = append(page.Items, item)
		reader.position += size
	}
	if trailing := reader.remaining(); trailing != 0 {
		return nil, fmt.Errorf("%w: %d trailing bytes after a page", ierror.ErrInvalidFormat, trailing)
	}
	return page, nil
}
