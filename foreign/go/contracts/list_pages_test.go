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

package iggcon_test

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var listPageOrders = map[string]iggcon.PageOrder{
	"id":      iggcon.PageOrderID,
	"created": iggcon.PageOrderCreated,
}

type listPagesFixture struct {
	SchemaVersion   int                `json:"schema_version"`
	Constants       listPagesConstants `json:"constants"`
	Cursors         []listPageCursor   `json:"cursors"`
	OpaqueCursors   []listPageCursor   `json:"opaque_cursors"`
	Requests        []listPageRequest  `json:"requests"`
	Responses       []listPageResponse `json:"responses"`
	OpaqueResponses []listPageResponse `json:"opaque_responses"`
	Refused         listPagesRefused   `json:"refused"`
}

type listPagesConstants struct {
	Codes          map[string]uint32 `json:"codes"`
	Orders         map[string]uint8  `json:"orders"`
	CursorPrefix   string            `json:"cursor_prefix"`
	MaxCursorBytes int               `json:"max_cursor_bytes"`
	DefaultLimit   uint32            `json:"default_limit"`
	MaxLimit       uint32            `json:"max_limit"`
	MaxNameBytes   int               `json:"max_name_bytes"`
	Errors         map[string]uint32 `json:"errors"`
}

type listPageCursor struct {
	Case string `json:"case"`
	Text string `json:"text"`
	Hex  string `json:"hex"`
}

type listPageIdentifier struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

type listPageInput struct {
	StreamID   *listPageIdentifier `json:"stream_id"`
	TopicID    *listPageIdentifier `json:"topic_id"`
	Order      string              `json:"order"`
	Descending bool                `json:"descending"`
	Limit      uint32              `json:"limit"`
	Cursor     *string             `json:"cursor"`
	Name       *string             `json:"name"`
}

type listPageRequest struct {
	Case       string        `json:"case"`
	Command    string        `json:"command"`
	Input      listPageInput `json:"input"`
	RequestHex string        `json:"request_hex"`
}

type listPageItem struct {
	ID   uint32  `json:"id"`
	Name *string `json:"name"`
}

type listPageResponse struct {
	Case        string         `json:"case"`
	Command     string         `json:"command"`
	ResponseHex string         `json:"response_hex"`
	Next        *string        `json:"next"`
	Items       []listPageItem `json:"items"`
}

type listPagesRefused struct {
	Cursors []struct {
		Case string `json:"case"`
		Text string `json:"text"`
	} `json:"cursors"`
	Queries []struct {
		Case  string        `json:"case"`
		Input listPageInput `json:"input"`
	} `json:"queries"`
	Responses []struct {
		Case        string `json:"case"`
		Command     string `json:"command"`
		ResponseHex string `json:"response_hex"`
	} `json:"responses"`
}

// listPageView is what a reply vector pins: the next text and the id and
// name of each item.
type listPageView struct {
	Next  *string
	Items []listPageItem
}

func TestListPages_SharedVectors(t *testing.T) {
	fixture := loadListPagesFixture(t)

	t.Run("given_shared_vectors_when_encoding_and_decoding_should_match_server_contract", func(t *testing.T) {
		t.Run("constants", func(t *testing.T) { checkListPagesConstants(t, fixture.Constants) })

		// The cursor check reads only the text, so the opaque cursors that
		// the server refuses pass it too.
		for _, vector := range append(fixture.Cursors, fixture.OpaqueCursors...) {
			t.Run("cursors/"+vector.Case, func(t *testing.T) {
				parsed, err := iggcon.ParsePageCursor(vector.Text)
				require.NoError(t, err)
				assert.Equal(t, vector.Text, parsed.String())
				var fromBytes iggcon.PageCursor
				require.NoError(t, fromBytes.UnmarshalBinary(decodeHex(t, vector.Hex)))
				assert.Equal(t, parsed, fromBytes)
			})
		}
		// The Go SDK has no HTTP transport, so http_query has no encoder to
		// check.
		for _, vector := range fixture.Requests {
			t.Run("requests/"+vector.Case, func(t *testing.T) {
				body, err := encodeListPageRequest(t, vector.Command, vector.Input)
				require.NoError(t, err)
				assert.Equal(t, vector.RequestHex, hex.EncodeToString(body))
			})
		}
		for _, vector := range append(fixture.Responses, fixture.OpaqueResponses...) {
			t.Run("responses/"+vector.Case, func(t *testing.T) {
				view, err := decodeListPage(t, vector.Command, decodeHex(t, vector.ResponseHex))
				require.NoError(t, err)
				assert.Equal(t, listPageView{Next: vector.Next, Items: vector.Items}, view)
			})
		}
	})

	t.Run("given_an_opaque_next_when_sending_it_back_should_keep_its_bytes", func(t *testing.T) {
		for _, vector := range fixture.OpaqueResponses {
			t.Run(vector.Case, func(t *testing.T) {
				reply := decodeHex(t, vector.ResponseHex)
				page, err := binaryserialization.DeserializeStreamsPage(reply)
				require.NoError(t, err)
				require.NotNil(t, page.Next)

				query := iggcon.NamedPageQuery{PageQuery: iggcon.PageQuery{Limit: iggcon.DefaultPageLimit, Cursor: page.Next}}
				body, err := (&command.GetStreams{Query: query}).MarshalBinary()
				require.NoError(t, err)
				// [order][descending][limit:u32][cursor_len][cursor][name_len],
				// with the next of the reply, [next_len][next], as the cursor.
				next := reply[:1+int(reply[0])]
				want := append([]byte{byte(iggcon.PageOrderID), 0}, binary.LittleEndian.AppendUint32(nil, iggcon.DefaultPageLimit)...)
				want = append(want, next...)
				assert.Equal(t, append(want, 0), body)
			})
		}
	})

	t.Run("given_refused_vectors_when_parsing_validating_or_decoding_should_refuse", func(t *testing.T) {
		for _, vector := range fixture.Refused.Cursors {
			t.Run("cursors/"+vector.Case, func(t *testing.T) {
				_, err := iggcon.ParsePageCursor(vector.Text)
				assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
			})
		}
		parentID, err := iggcon.NewIdentifier(uint32(1))
		require.NoError(t, err)
		for _, vector := range fixture.Refused.Queries {
			t.Run("queries/"+vector.Case, func(t *testing.T) {
				named := namedListPageQuery(t, vector.Input)
				assert.ErrorIs(t, named.Validate(), ierror.ErrInvalidCommand)
				body, err := (&command.GetStreams{Query: named}).MarshalBinary()
				assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
				assert.Empty(t, body)
				if vector.Input.Name == nil {
					query := listPageQuery(t, vector.Input)
					assert.ErrorIs(t, query.Validate(), ierror.ErrInvalidCommand)
					body, err = (&command.GetPartitions{StreamId: parentID, TopicId: parentID, Query: query}).MarshalBinary()
					assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
					assert.Empty(t, body)
				}
			})
		}
		for _, vector := range fixture.Refused.Responses {
			t.Run("responses/"+vector.Case, func(t *testing.T) {
				_, err := decodeListPage(t, vector.Command, decodeHex(t, vector.ResponseHex))
				assert.ErrorIs(t, err, ierror.ErrInvalidFormat)
			})
		}
	})
}

func TestPageQuery_RefusesValuesTheFixtureCannotHold(t *testing.T) {
	t.Run("given_an_unknown_order_an_empty_cursor_or_a_name_that_is_not_utf8_when_validating_should_refuse", func(t *testing.T) {
		notUTF8 := string([]byte{0xff})
		refused := map[string]error{
			"unknown_order":      iggcon.PageQuery{Order: iggcon.PageOrderCreated + 1, Limit: 1}.Validate(),
			"cursor_of_no_bytes": iggcon.PageQuery{Limit: 1, Cursor: &iggcon.PageCursor{}}.Validate(),
			"name_not_utf8":      iggcon.NamedPageQuery{PageQuery: iggcon.PageQuery{Limit: 1}, Name: &notUTF8}.Validate(),
		}
		for name, err := range refused {
			assert.ErrorIs(t, err, ierror.ErrInvalidCommand, name)
		}
	})
}

func TestPageCursor_JSON(t *testing.T) {
	fixture := loadListPagesFixture(t)
	text := fixture.Cursors[0].Text

	t.Run("given_a_page_and_a_query_with_a_cursor_when_round_tripping_through_json_should_keep_its_text_form", func(t *testing.T) {
		cursor, err := iggcon.ParsePageCursor(text)
		require.NoError(t, err)
		page := iggcon.Page[uint32]{Items: []uint32{7}, Next: &cursor}
		query := iggcon.PageQuery{Limit: 1, Cursor: &cursor}

		encodedPage, err := json.Marshal(page)
		require.NoError(t, err)
		assert.Contains(t, string(encodedPage), `"Next":"`+text+`"`)
		encodedQuery, err := json.Marshal(query)
		require.NoError(t, err)
		assert.Contains(t, string(encodedQuery), `"Cursor":"`+text+`"`)

		var decodedPage iggcon.Page[uint32]
		require.NoError(t, json.Unmarshal(encodedPage, &decodedPage))
		assert.Equal(t, page, decodedPage)
		var decodedQuery iggcon.PageQuery
		require.NoError(t, json.Unmarshal(encodedQuery, &decodedQuery))
		assert.Equal(t, query, decodedQuery)
	})

	t.Run("given_a_last_page_when_round_tripping_through_json_should_keep_no_next", func(t *testing.T) {
		encoded, err := json.Marshal(iggcon.Page[uint32]{Items: []uint32{}})
		require.NoError(t, err)
		assert.Contains(t, string(encoded), `"Next":null`)
		var decoded iggcon.Page[uint32]
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		assert.Nil(t, decoded.Next)
	})

	t.Run("given_non_canonical_cursor_text_when_unmarshaling_json_should_refuse", func(t *testing.T) {
		var cursor iggcon.PageCursor
		err := json.Unmarshal([]byte(`"`+text+`="`), &cursor)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	})
}

func loadListPagesFixture(t *testing.T) listPagesFixture {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("testdata", "list_pages.json"))
	require.NoError(t, err)
	var fixture listPagesFixture
	require.NoError(t, json.Unmarshal(content, &fixture))
	require.Equal(t, 1, fixture.SchemaVersion)
	return fixture
}

func checkListPagesConstants(t *testing.T, constants listPagesConstants) {
	assert.Equal(t, map[string]uint32{
		"get_users":           uint32(command.GetUsersCode),
		"get_streams":         uint32(command.GetStreamsCode),
		"get_topics":          uint32(command.GetTopicsCode),
		"get_partitions":      uint32(command.GetPartitionsCode),
		"get_consumer_groups": uint32(command.GetGroupsCode),
	}, constants.Codes)
	for name, code := range constants.Codes {
		assert.Equal(t, vsr.OperationNonReplicated, vsr.OperationForCode(code), name)
	}
	orders := make(map[string]uint8, len(listPageOrders))
	for name, order := range listPageOrders {
		orders[name] = uint8(order)
	}
	assert.Equal(t, orders, constants.Orders)
	assert.Equal(t, iggcon.PageCursorPrefix, constants.CursorPrefix)
	assert.Equal(t, iggcon.MaxPageCursorSize, constants.MaxCursorBytes)
	assert.Equal(t, iggcon.DefaultPageLimit, constants.DefaultLimit)
	assert.Equal(t, iggcon.NewPageQuery().Limit, constants.DefaultLimit)
	assert.Equal(t, iggcon.MaxPageLimit, constants.MaxLimit)
	assert.Equal(t, iggcon.MaxPageNameSize, constants.MaxNameBytes)
	assert.Equal(t, map[string]uint32{
		"invalid_command": uint32(ierror.InvalidCommandCode),
		"reply_too_large": uint32(ierror.ReplyTooLargeCode),
	}, constants.Errors)
}

func listPageQuery(t *testing.T, input listPageInput) iggcon.PageQuery {
	t.Helper()
	order, known := listPageOrders[input.Order]
	require.True(t, known, "order %q", input.Order)
	query := iggcon.PageQuery{Order: order, Descending: input.Descending, Limit: input.Limit}
	if input.Cursor != nil {
		cursor, err := iggcon.ParsePageCursor(*input.Cursor)
		require.NoError(t, err)
		query.Cursor = &cursor
	}
	return query
}

func namedListPageQuery(t *testing.T, input listPageInput) iggcon.NamedPageQuery {
	t.Helper()
	return iggcon.NamedPageQuery{PageQuery: listPageQuery(t, input), Name: input.Name}
}

func listPageParent(t *testing.T, id *listPageIdentifier) iggcon.Identifier {
	t.Helper()
	require.NotNil(t, id, "the command needs this parent id")
	var identifier iggcon.Identifier
	var err error
	switch id.Kind {
	case "numeric":
		var value uint32
		require.NoError(t, json.Unmarshal(id.Value, &value))
		identifier, err = iggcon.NewIdentifier(value)
	case "named":
		var value string
		require.NoError(t, json.Unmarshal(id.Value, &value))
		identifier, err = iggcon.NewIdentifier(value)
	default:
		require.FailNow(t, "unknown identifier kind", id.Kind)
	}
	require.NoError(t, err)
	return identifier
}

func encodeListPageRequest(t *testing.T, name string, input listPageInput) ([]byte, error) {
	t.Helper()
	switch name {
	case "get_users":
		return (&command.GetUsers{Query: namedListPageQuery(t, input)}).MarshalBinary()
	case "get_streams":
		return (&command.GetStreams{Query: namedListPageQuery(t, input)}).MarshalBinary()
	case "get_topics":
		return (&command.GetTopics{StreamId: listPageParent(t, input.StreamID), Query: namedListPageQuery(t, input)}).MarshalBinary()
	case "get_partitions":
		require.Nil(t, input.Name, "a partitions query has no name")
		return (&command.GetPartitions{
			StreamId: listPageParent(t, input.StreamID),
			TopicId:  listPageParent(t, input.TopicID),
			Query:    listPageQuery(t, input),
		}).MarshalBinary()
	case "get_consumer_groups":
		return (&command.GetConsumerGroups{
			StreamId: listPageParent(t, input.StreamID),
			TopicId:  listPageParent(t, input.TopicID),
			Query:    namedListPageQuery(t, input),
		}).MarshalBinary()
	}
	require.FailNow(t, "unknown list command", name)
	return nil, nil
}

func decodeListPage(t *testing.T, name string, payload []byte) (listPageView, error) {
	t.Helper()
	switch name {
	case "get_users":
		page, err := binaryserialization.DeserializeUsersPage(payload)
		return viewListPage(page, err, func(user iggcon.UserInfo) listPageItem {
			return namedListPageItem(user.Id, user.Username)
		})
	case "get_streams":
		page, err := binaryserialization.DeserializeStreamsPage(payload)
		return viewListPage(page, err, func(stream iggcon.Stream) listPageItem {
			return namedListPageItem(stream.Id, stream.Name)
		})
	case "get_topics":
		page, err := binaryserialization.DeserializeTopicsPage(payload)
		return viewListPage(page, err, func(topic iggcon.Topic) listPageItem {
			return namedListPageItem(topic.Id, topic.Name)
		})
	case "get_partitions":
		page, err := binaryserialization.DeserializePartitionsPage(payload)
		return viewListPage(page, err, func(partition iggcon.PartitionContract) listPageItem {
			return listPageItem{ID: partition.Id}
		})
	case "get_consumer_groups":
		page, err := binaryserialization.DeserializeConsumerGroupsPage(payload)
		return viewListPage(page, err, func(group iggcon.ConsumerGroup) listPageItem {
			return namedListPageItem(group.Id, group.Name)
		})
	}
	require.FailNow(t, "unknown list command", name)
	return listPageView{}, nil
}

func viewListPage[T any](page *iggcon.Page[T], err error, item func(T) listPageItem) (listPageView, error) {
	if err != nil {
		return listPageView{}, err
	}
	view := listPageView{Items: make([]listPageItem, 0, len(page.Items))}
	if page.Next != nil {
		next := page.Next.String()
		view.Next = &next
	}
	for _, entry := range page.Items {
		view.Items = append(view.Items, item(entry))
	}
	return view, nil
}

func namedListPageItem(id uint32, name string) listPageItem {
	return listPageItem{ID: id, Name: &name}
}
