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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"maps"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// A two phase delete commits its record after the call returns, so a
	// follow waits for it within this budget.
	changesFollowBudget   = 15 * time.Second
	changesFollowInterval = 100 * time.Millisecond
)

func TestE2E_MetadataChangesFollow(t *testing.T) {
	t.Run("given_latest_cursor_when_creating_renaming_and_deleting_a_stream_should_follow_its_records", func(t *testing.T) {
		connected, actor := connectAs(t, rootUsername, rootPassword)
		ctx := context.Background()
		cursor := latestChangeCursor(t, connected, iggcon.MetadataChangeGroupStreams)

		name := uniqueName("changes-follow")
		created, err := connected.CreateStream(ctx, name, nil)
		require.NoError(t, err)
		streamId := scratchStream(t, connected, created.Id)
		renamed := uniqueName("changes-renamed")
		require.NoError(t, connected.UpdateStream(ctx, streamId, iggcon.StreamUpdate{Name: &renamed}))
		require.NoError(t, connected.DeleteStream(ctx, streamId))

		records, _ := followStreamRecords(t, connected, cursor, created.Id, 3, iggcon.MetadataChangeGroupStreams)
		assert.Equal(t, []iggcon.MetadataChangeKind{
			iggcon.MetadataChangeKindStreamCreated,
			iggcon.MetadataChangeKindStreamUpdated,
			iggcon.MetadataChangeKindStreamDeleted,
		}, recordKinds(records))
		for _, record := range records {
			require.NotNil(t, record.ActorUserID, "%+v", record)
			assert.Equal(t, actor, *record.ActorUserID)
			assert.Equal(t, iggcon.MetadataChangeGroupStreams, record.Group)
			assert.False(t, record.Checksum.IsZero())
			assert.Positive(t, record.Timestamp)
		}

		createdBody := records[0].Body.(*iggcon.MetadataStreamChange)
		updatedBody := records[1].Body.(*iggcon.MetadataStreamChange)
		deletedBody := records[2].Body.(*iggcon.MetadataStreamDeleted)
		assert.Equal(t, name, createdBody.Name)
		assert.Equal(t, renamed, updatedBody.Name)
		assert.Equal(t, renamed, deletedBody.Name)
		assert.Equal(t, createdBody.MetadataStreamKey, updatedBody.MetadataStreamKey)
		assert.Equal(t, createdBody.MetadataStreamKey, deletedBody.MetadataStreamKey)
		assert.Equal(t, records[0].Op, createdBody.StreamCreatedOp)
		assert.False(t, createdBody.Options.Unreadable)
		assert.Less(t, records[0].Op, records[1].Op)
		assert.Less(t, records[1].Op, records[2].Op)
	})
}

func TestE2E_MetadataChangesGroupFilter(t *testing.T) {
	t.Run("given_topics_group_when_creating_a_stream_and_topic_should_return_only_the_topic_record", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		cursor := latestChangeCursor(t, connected, iggcon.MetadataChangeGroupTopics)

		stream, err := connected.CreateStream(ctx, uniqueName("changes-filter"), nil)
		require.NoError(t, err)
		streamId := scratchStream(t, connected, stream.Id)
		partitions := uint32(2)
		topic, err := connected.CreateTopic(ctx, streamId, "events", &iggcon.TopicOptions{PartitionsCount: &partitions})
		require.NoError(t, err)

		records, _ := followStreamRecords(t, connected, cursor, stream.Id, 1, iggcon.MetadataChangeGroupTopics)
		require.Equal(t, []iggcon.MetadataChangeKind{iggcon.MetadataChangeKindTopicCreated}, recordKinds(records))
		body := records[0].Body.(*iggcon.MetadataTopicChange)
		assert.Equal(t, topic.Id, body.TopicID)
		assert.Equal(t, "events", body.Name)
		assert.Equal(t, partitions, body.PartitionsCount)
		assert.Equal(t, records[0].Op, body.TopicCreatedOp)
	})
}

func TestE2E_MetadataChangesLimitPaging(t *testing.T) {
	t.Run("given_limit_of_one_when_following_three_streams_should_page_one_record_at_a_time", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		cursor := latestChangeCursor(t, connected, iggcon.MetadataChangeGroupStreams)

		created := make(map[uint32]string)
		for range 3 {
			name := uniqueName("changes-page")
			stream, err := connected.CreateStream(ctx, name, nil)
			require.NoError(t, err)
			scratchStream(t, connected, stream.Id)
			created[stream.Id] = name
		}

		seen := make(map[uint32]string)
		deadline := time.Now().Add(changesFollowBudget)
		for len(seen) < len(created) {
			require.False(t, time.Now().After(deadline), "paged %v of %v", seen, created)
			query := iggcon.NewMetadataChangesQuery(iggcon.AfterMetadataChangeStart(cursor), iggcon.MetadataChangeGroupStreams)
			query.Limit = 1
			changes, err := connected.GetMetadataChanges(ctx, query)
			require.NoError(t, err)
			require.Equal(t, iggcon.MetadataChangeStatusOk, changes.Status)
			require.LessOrEqual(t, len(changes.Records), 1)
			require.NotNil(t, changes.Next)
			for _, record := range changes.Records {
				if body, ok := record.Body.(*iggcon.MetadataStreamChange); ok && created[body.StreamID] == body.Name {
					seen[body.StreamID] = body.Name
				}
				assert.Equal(t, record.Op, changes.Next.Op)
				assert.Equal(t, record.Ordinal, changes.Next.Ordinal)
			}
			assert.GreaterOrEqual(t, changes.Next.Op, cursor.Op)
			cursor = *changes.Next
			if !changes.HasMore && len(seen) < len(created) {
				time.Sleep(changesFollowInterval)
			}
		}
		assert.Equal(t, created, seen)
	})
}

func TestE2E_MetadataChangesCurrentState(t *testing.T) {
	t.Run("given_streams_when_paging_current_state_one_item_at_a_time_should_list_the_same_streams", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		for range 2 {
			stream, err := connected.CreateStream(ctx, uniqueName("changes-current"), nil)
			require.NoError(t, err)
			scratchStream(t, connected, stream.Id)
		}

		// A deferred delete of an earlier test may land between the two reads.
		deadline := time.Now().Add(changesFollowBudget)
		for {
			projected, anchor := currentStreams(t, connected)
			listed, err := connected.GetStreams(ctx)
			require.NoError(t, err)
			names := make(map[uint32]string, len(listed))
			for _, stream := range listed {
				names[stream.Id] = stream.Name
			}
			assert.Positive(t, anchor.Op)
			if maps.Equal(projected, names) {
				break
			}
			require.False(t, time.Now().After(deadline), "current state %v, stream list %v", projected, names)
			time.Sleep(changesFollowInterval)
		}
	})
}

func TestE2E_MetadataChangesRefusesInvalidQueries(t *testing.T) {
	t.Run("given_queries_breaking_the_request_rules_when_reading_should_refuse_before_sending", func(t *testing.T) {
		connected := connect(t)
		ctx := context.Background()
		token := currentPageToken(t, connected)
		later, earlier := uint64(2), uint64(1)

		refused := map[string]iggcon.MetadataChangesQuery{
			"limit_zero":           {Start: iggcon.EarliestMetadataChangeStart()},
			"limit_above_maximum":  {Start: iggcon.EarliestMetadataChangeStart(), Limit: iggcon.MaxMetadataChangesLimit + 1},
			"since_above_until":    {Start: iggcon.EarliestMetadataChangeStart(), Since: &later, Until: &earlier, Limit: 1},
			"time_bound_latest":    {Start: iggcon.LatestMetadataChangeStart(), Since: &earlier, Limit: 1},
			"time_bound_current":   {Start: iggcon.CurrentMetadataChangeStart(nil), Until: &earlier, Limit: 1},
			"missing_parent_group": iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(nil), iggcon.MetadataChangeGroupTopics),
			"token_of_other_groups": iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(token),
				iggcon.MetadataChangeGroupStreams, iggcon.MetadataChangeGroupUsers),
		}
		for name, query := range refused {
			_, err := connected.GetMetadataChanges(ctx, query)
			assert.ErrorIs(t, err, ierror.ErrInvalidCommand, name)
		}

		changes, err := connected.GetMetadataChanges(ctx, iggcon.NewMetadataChangesQuery(iggcon.LatestMetadataChangeStart()))
		require.NoError(t, err, "the connection stays usable after refused queries")
		assert.Equal(t, iggcon.MetadataChangeStatusOk, changes.Status)
	})
}

func TestE2E_MetadataChangesPermission(t *testing.T) {
	t.Run("given_user_without_manage_servers_when_reading_changes_should_be_refused_as_unauthorized", func(t *testing.T) {
		root := connect(t)
		ctx := context.Background()
		username := uniqueUsername("changes-reader")
		password := "changes-secret"
		user, err := root.CreateUser(ctx, username, password, iggcon.Active,
			&iggcon.Permissions{Global: iggcon.GlobalPermissions{ReadServers: true, ReadStreams: true, ReadUsers: true}}, nil)
		require.NoError(t, err)
		userId, err := iggcon.NewIdentifier(user.Id)
		require.NoError(t, err)
		t.Cleanup(func() { _ = root.DeleteUser(context.Background(), userId) })

		reader, _ := connectAs(t, username, password)
		_, err = reader.GetMetadataChanges(ctx, iggcon.NewMetadataChangesQuery(iggcon.EarliestMetadataChangeStart()))
		assert.ErrorIs(t, err, ierror.ErrUnauthorized)
		var iggyError ierror.IggyError
		require.ErrorAs(t, err, &iggyError)
		assert.Equal(t, ierror.Code(41), iggyError.Code())
	})
}

// checkKeyedStreamChange runs inside TestE2E_StreamKeyProjection, which the
// shipped-default lane skips because a client key needs a server key.
func checkKeyedStreamChange(t *testing.T) {
	connected := connect(t)
	ctx := context.Background()
	cursor := latestChangeCursor(t, connected, iggcon.MetadataChangeGroupStreams)
	key := randomStreamKey(t)
	fingerprint, err := iggcon.EncryptionKeyFingerprint(key)
	require.NoError(t, err)

	created, err := connected.CreateStream(ctx, uniqueName("changes-keyed"), &iggcon.StreamOptions{EncryptionKey: key})
	require.NoError(t, err)
	scratchStream(t, connected, created.Id)

	records, _ := followStreamRecords(t, connected, cursor, created.Id, 1, iggcon.MetadataChangeGroupStreams)
	require.Equal(t, []iggcon.MetadataChangeKind{iggcon.MetadataChangeKindStreamCreated}, recordKinds(records))
	options := records[0].Body.(*iggcon.MetadataStreamChange).Options
	require.False(t, options.Unreadable)
	projected := iggcon.Stream{Options: options.Options, DerivedOptions: options.DerivedOptions}
	requireProjection(t, "change", projected, iggcon.StreamEncryptionSourceStream)
	got, ok := projected.EncryptionKeyFingerprint()
	require.True(t, ok)
	assert.Equal(t, fingerprint, got)
	assert.Equal(t, created.EncryptionKekFingerprints(), projected.EncryptionKekFingerprints())
	requireNoKeyMaterial(t, "change", projected, key)
	for _, values := range []map[string]iggcon.HeaderValue{options.Options, options.DerivedOptions} {
		for name, value := range values {
			assert.False(t, bytes.Contains(value.Value, []byte(hex.EncodeToString(key))), name)
			assert.NotContains(t, string(value.Value), base64.RawURLEncoding.EncodeToString(key), name)
		}
	}
}

// connectAs signs a new client in and returns the signed-in user id.
func connectAs(t *testing.T, username, password string) (iggcon.Client, uint32) {
	t.Helper()
	connected := newClient(t)
	identity, err := connected.LoginUser(context.Background(), username, password)
	require.NoError(t, err)
	return connected, identity.UserId
}

func latestChangeCursor(t *testing.T, connected iggcon.Client, groups ...iggcon.MetadataChangeGroup) iggcon.MetadataChangeCursor {
	t.Helper()
	changes, err := connected.GetMetadataChanges(context.Background(),
		iggcon.NewMetadataChangesQuery(iggcon.LatestMetadataChangeStart(), groups...))
	require.NoError(t, err)
	require.Equal(t, iggcon.MetadataChangeStatusOk, changes.Status)
	require.Empty(t, changes.Records)
	require.NotNil(t, changes.LogID)
	require.NotNil(t, changes.Next)
	assert.Equal(t, *changes.LogID, changes.Next.LogID)
	return *changes.Next
}

// followStreamRecords follows the change log after cursor until want records
// name the stream, and checks that no record leaves the requested groups.
func followStreamRecords(
	t *testing.T,
	connected iggcon.Client,
	cursor iggcon.MetadataChangeCursor,
	streamID uint32,
	want int,
	groups ...iggcon.MetadataChangeGroup,
) ([]iggcon.MetadataChangeRecord, iggcon.MetadataChangeCursor) {
	t.Helper()
	var records []iggcon.MetadataChangeRecord
	deadline := time.Now().Add(changesFollowBudget)
	for {
		query := iggcon.NewMetadataChangesQuery(iggcon.AfterMetadataChangeStart(cursor), groups...)
		query.Limit = iggcon.MaxMetadataChangesLimit
		changes, err := connected.GetMetadataChanges(context.Background(), query)
		require.NoError(t, err)
		require.Equal(t, iggcon.MetadataChangeStatusOk, changes.Status)
		require.Equal(t, iggcon.MetadataChangePhaseChanges, changes.Phase)
		require.Empty(t, changes.Items)
		require.Nil(t, changes.CurrentPageToken)
		require.NotNil(t, changes.Next)
		cursor = *changes.Next
		for _, record := range changes.Records {
			assert.Contains(t, groups, record.Group, "%+v", record)
			if id, ok := recordStreamID(record); ok && id == streamID {
				records = append(records, record)
			}
		}
		if changes.HasMore {
			continue
		}
		if len(records) >= want || time.Now().After(deadline) {
			break
		}
		time.Sleep(changesFollowInterval)
	}
	require.Len(t, records, want)
	return records, cursor
}

// currentStreams pages the streams state one item at a time and returns the
// names by id with the scan's anchor.
func currentStreams(t *testing.T, connected iggcon.Client) (map[uint32]string, iggcon.MetadataChangeCursor) {
	t.Helper()
	streams := make(map[uint32]string)
	var token *iggcon.MetadataCurrentPageToken
	for page := 0; ; page++ {
		require.Less(t, page, 10_000, "the current state scan does not end")
		query := iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(token), iggcon.MetadataChangeGroupStreams)
		query.Limit = 1
		changes, err := connected.GetMetadataChanges(context.Background(), query)
		require.NoError(t, err)
		require.Equal(t, iggcon.MetadataChangeStatusOk, changes.Status)
		require.Equal(t, iggcon.MetadataChangePhaseCurrent, changes.Phase)
		require.Empty(t, changes.Records)
		require.LessOrEqual(t, len(changes.Items), 1)
		for _, item := range changes.Items {
			stream, ok := item.Body.(*iggcon.MetadataStreamChange)
			require.True(t, ok, "%T", item.Body)
			streams[stream.StreamID] = stream.Name
		}
		if changes.CurrentPageToken == nil {
			require.NotNil(t, changes.Next, "the last page returns the anchor")
			return streams, *changes.Next
		}
		assert.True(t, changes.HasMore)
		token = changes.CurrentPageToken
	}
}

// currentPageToken returns a token of a streams scan that has a next page.
func currentPageToken(t *testing.T, connected iggcon.Client) *iggcon.MetadataCurrentPageToken {
	t.Helper()
	for range 2 {
		stream, err := connected.CreateStream(context.Background(), uniqueName("changes-token"), nil)
		require.NoError(t, err)
		scratchStream(t, connected, stream.Id)
	}
	query := iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(nil), iggcon.MetadataChangeGroupStreams)
	query.Limit = 1
	changes, err := connected.GetMetadataChanges(context.Background(), query)
	require.NoError(t, err)
	require.NotNil(t, changes.CurrentPageToken)
	return changes.CurrentPageToken
}

func recordKinds(records []iggcon.MetadataChangeRecord) []iggcon.MetadataChangeKind {
	kinds := make([]iggcon.MetadataChangeKind, 0, len(records))
	for _, record := range records {
		kinds = append(kinds, record.Kind)
	}
	return kinds
}

// recordStreamID names the stream a stream, topic or partitions record
// belongs to.
func recordStreamID(record iggcon.MetadataChangeRecord) (uint32, bool) {
	switch body := record.Body.(type) {
	case *iggcon.MetadataStreamChange:
		return body.StreamID, true
	case *iggcon.MetadataStreamDeleted:
		return body.StreamID, true
	case *iggcon.MetadataTopicChange:
		return body.StreamID, true
	case *iggcon.MetadataTopicDeleted:
		return body.StreamID, true
	case *iggcon.MetadataPartitionsChanged:
		return body.StreamID, true
	default:
		return 0, false
	}
}
