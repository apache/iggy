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

package tests_test

import (
	"context"
	"slices"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cursor texts of core/common/fixtures/list_pages.json. The text of both is
// valid, so only the server can refuse them.
const (
	// idSevenCursor is a cursor of the id walk.
	idSevenCursor = "pg1_AQcAAAAAAAAA"
	// unknownKindCursor holds a cursor kind that no server knows.
	unknownKindCursor = "pg1_AwcAAAAAAAAA"
)

func TestE2E_ListPages(t *testing.T) {
	t.Run("given_three_streams_when_reading_pages_of_two_should_return_each_once_in_the_order_asked", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		prefix := uniqueName("pages")
		var ids []uint32
		for _, suffix := range []string{"a", "b", "c"} {
			stream, err := connected.CreateStream(ctx, prefix+"-"+suffix, nil)
			require.NoError(t, err)
			scratchStream(t, connected, stream.Id)
			ids = append(ids, stream.Id)
		}
		// A create takes the lowest free id, so a delete by another client can
		// give a later stream a lower id.
		ascendingIds := slices.Sorted(slices.Values(ids))
		descendingIds := slices.Clone(ascendingIds)
		slices.Reverse(descendingIds)

		walks := []struct {
			order      iggcon.PageOrder
			descending bool
			want       []uint32
		}{
			{iggcon.PageOrderID, false, ascendingIds},
			{iggcon.PageOrderID, true, descendingIds},
			{iggcon.PageOrderCreated, false, ids},
		}
		for _, walk := range walks {
			query := pageQueryWithName(prefix)
			query.Order, query.Descending, query.Limit = walk.order, walk.descending, 2
			first, err := connected.GetStreamsPage(ctx, query)
			require.NoError(t, err)
			require.Len(t, first.Items, 2, "%+v", walk)
			require.NotNil(t, first.Next, "%+v", walk)

			query.Cursor = first.Next
			second, err := connected.GetStreamsPage(ctx, query)
			require.NoError(t, err)
			assert.Nil(t, second.Next, "%+v", walk)

			var walked []uint32
			for _, stream := range append(first.Items, second.Items...) {
				walked = append(walked, stream.Id)
			}
			assert.Equal(t, walk.want, walked, "%+v", walk)
		}

		streams, err := connected.GetStreams(ctx)
		require.NoError(t, err)
		for _, id := range ids {
			assert.True(t, slices.ContainsFunc(streams, func(stream iggcon.Stream) bool { return stream.Id == id }),
				"the whole list holds stream %d", id)
		}
	})

	t.Run("given_a_topic_with_three_partitions_when_reading_pages_of_two_should_return_each_partition_once", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		streamId, topicId := scratchTopic(t, connected, 3)

		query := iggcon.NewPageQuery()
		query.Limit = 2
		first, err := connected.GetPartitionsPage(ctx, streamId, topicId, query)
		require.NoError(t, err)
		require.Len(t, first.Items, 2)
		require.NotNil(t, first.Next)

		query.Cursor = first.Next
		second, err := connected.GetPartitionsPage(ctx, streamId, topicId, query)
		require.NoError(t, err)
		assert.Nil(t, second.Next)

		var ids []uint32
		for _, partition := range append(first.Items, second.Items...) {
			ids = append(ids, partition.Id)
		}
		assert.Equal(t, []uint32{0, 1, 2}, ids)
	})

	t.Run("given_a_topic_a_group_and_a_user_when_filtering_by_name_should_return_only_the_match", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		streamId, topicId := scratchTopic(t, connected, 1)
		partitionsCount := uint32(1)
		topic, err := connected.CreateTopic(ctx, streamId, "sensor-readings", &iggcon.TopicOptions{PartitionsCount: &partitionsCount})
		require.NoError(t, err)
		group, err := connected.CreateConsumerGroup(ctx, streamId, topicId, "billing")
		require.NoError(t, err)
		_, err = connected.CreateConsumerGroup(ctx, streamId, topicId, "audit")
		require.NoError(t, err)
		username := uniqueUsername("pages")
		user := scratchUser(t, connected, username)

		topics, err := connected.GetTopicsPage(ctx, streamId, pageQueryWithName("sensor"))
		require.NoError(t, err)
		require.Len(t, topics.Items, 1)
		assert.Equal(t, topic.Id, topics.Items[0].Id)
		assert.Nil(t, topics.Next)

		groups, err := connected.GetConsumerGroupsPage(ctx, streamId, topicId, pageQueryWithName("bill"))
		require.NoError(t, err)
		require.Len(t, groups.Items, 1)
		assert.Equal(t, group.Id, groups.Items[0].Id)
		assert.Nil(t, groups.Next)

		users, err := connected.GetUsersPage(ctx, pageQueryWithName(username))
		require.NoError(t, err)
		require.Len(t, users.Items, 1)
		assert.Equal(t, user.Id, users.Items[0].Id)
		assert.Nil(t, users.Next)
	})

	t.Run("given_page_requests_the_server_cannot_serve_when_reading_should_be_refused_as_invalid_command", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()

		unknownKind, err := iggcon.ParsePageCursor(unknownKindCursor)
		require.NoError(t, err)
		query := iggcon.NamedPageQuery{PageQuery: iggcon.NewPageQuery()}
		query.Cursor = &unknownKind
		_, err = connected.GetStreamsPage(ctx, query)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand, "a cursor of an unknown kind")

		idCursor, err := iggcon.ParsePageCursor(idSevenCursor)
		require.NoError(t, err)
		query = iggcon.NamedPageQuery{PageQuery: iggcon.NewPageQuery()}
		query.Order, query.Cursor = iggcon.PageOrderCreated, &idCursor
		_, err = connected.GetStreamsPage(ctx, query)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand, "a cursor of the id walk in a created walk")

		_, err = connected.GetStreamsPage(ctx, iggcon.NamedPageQuery{PageQuery: iggcon.NewPageQuery()})
		require.NoError(t, err, "the connection stays usable after refused pages")
	})
}

func pageQueryWithName(name string) iggcon.NamedPageQuery {
	return iggcon.NamedPageQuery{PageQuery: iggcon.NewPageQuery(), Name: &name}
}
