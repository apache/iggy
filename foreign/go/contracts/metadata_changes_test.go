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
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	binaryserialization "github.com/apache/iggy/foreign/go/binary_serialization"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadataChanges_SharedVectors(t *testing.T) {
	fixture := loadMetadataChangesFixture(t)

	t.Run("given_shared_vectors_when_decoding_should_match_server_contract", func(t *testing.T) {
		t.Run("constants", func(t *testing.T) { checkMetadataChangesConstants(t, fixture.Constants) })

		for _, vector := range fixture.Records {
			t.Run("records/"+vector.Case, func(t *testing.T) {
				require.True(t, strings.HasSuffix(vector.RecordHex, vector.BodyHex))
				page := decodeChangesPage(t, decodeHex(t, vector.RecordHex))
				require.Len(t, page.Records, 1)
				record := page.Records[0]
				assert.Equal(t, vector.Group, uint8(record.Group))
				assert.Equal(t, vector.Kind, uint8(record.Kind))
				assert.JSONEq(t, string(vector.Expected), marshalView(t, recordView(t, record)))
			})
		}
		for _, vector := range fixture.Items {
			t.Run("items/"+vector.Case, func(t *testing.T) {
				require.True(t, strings.HasSuffix(vector.ItemHex, vector.BodyHex))
				page := decodeCurrentPage(t, decodeHex(t, vector.ItemHex))
				require.Len(t, page.Items, 1)
				assert.Equal(t, vector.Group, uint8(page.Items[0].Group))
				assert.JSONEq(t, string(vector.Expected), marshalView(t, itemView(t, page.Items[0])))
			})
		}
		for _, vector := range fixture.Cursors {
			t.Run("cursors/"+vector.Case, func(t *testing.T) { checkCursorVector(t, vector) })
		}
		for _, vector := range fixture.PageTokens {
			t.Run("page_tokens/"+vector.Case, func(t *testing.T) { checkPageTokenVector(t, vector) })
		}
		// The Go SDK has no HTTP transport, so http_query and the refused HTTP
		// queries have no encoder to check.
		for _, vector := range fixture.Requests {
			t.Run("requests/"+vector.Case, func(t *testing.T) {
				body, err := (&command.GetMetadataChanges{Query: fixtureQuery(t, vector.Input)}).MarshalBinary()
				require.NoError(t, err)
				assert.Equal(t, vector.RequestHex, hex.EncodeToString(body))
			})
		}
		for _, vector := range fixture.Responses {
			t.Run("responses/"+vector.Case, func(t *testing.T) {
				changes, err := binaryserialization.DeserializeMetadataChanges(decodeHex(t, vector.ResponseHex))
				require.NoError(t, err)
				assert.JSONEq(t, string(vector.Expected), marshalView(t, changesView(t, changes)))
			})
		}
		t.Run("large_values", func(t *testing.T) { checkLargeValues(t, fixture.LargeValues) })
	})

	t.Run("given_refused_vectors_when_decoding_or_validating_should_refuse", func(t *testing.T) {
		for _, vector := range fixture.Refused.Cursors {
			t.Run("cursors/"+vector.Case, func(t *testing.T) {
				_, err := iggcon.ParseMetadataChangeCursor(vector.Text)
				assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
			})
		}
		for _, vector := range fixture.Refused.PageTokens {
			t.Run("page_tokens/"+vector.Case, func(t *testing.T) {
				_, err := iggcon.ParseMetadataCurrentPageToken(vector.Text)
				assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
			})
		}
		for _, vector := range fixture.Refused.RecordBodies {
			t.Run("record_bodies/"+vector.Case, func(t *testing.T) {
				record := recordEnvelope(vector.Group, vector.Kind, decodeHex(t, vector.BodyHex))
				_, err := binaryserialization.DeserializeMetadataChanges(changesPagePayload(record))
				assert.ErrorIs(t, err, ierror.ErrInvalidFormat)
			})
		}
		for _, vector := range fixture.Refused.Requests {
			t.Run("requests/"+vector.Case, func(t *testing.T) { checkRefusedRequest(t, vector.Case, decodeHex(t, vector.Hex)) })
		}
		for _, vector := range fixture.Refused.Responses {
			t.Run("responses/"+vector.Case, func(t *testing.T) {
				_, err := binaryserialization.DeserializeMetadataChanges(decodeHex(t, vector.Hex))
				assert.ErrorIs(t, err, ierror.ErrInvalidFormat)
			})
		}
		for _, vector := range fixture.Refused.Queries {
			t.Run("queries/"+vector.Case, func(t *testing.T) {
				query := fixtureQuery(t, vector.Input)
				assert.ErrorIs(t, query.Validate(), ierror.ErrInvalidCommand)
				body, err := (&command.GetMetadataChanges{Query: query}).MarshalBinary()
				assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
				assert.Empty(t, body)
			})
		}
	})
}

func TestMetadataChanges_Positions(t *testing.T) {
	fixture := loadMetadataChangesFixture(t)
	tokenText := fixture.PageTokens[0].Text

	t.Run("given_line_breaks_inside_a_token_when_parsing_should_refuse", func(t *testing.T) {
		broken := tokenText[:20] + "\n" + tokenText[20:]
		_, err := iggcon.ParseMetadataCurrentPageToken(broken)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	})

	t.Run("given_token_longer_than_the_longest_valid_when_parsing_should_refuse", func(t *testing.T) {
		longest := fixture.PageTokens[len(fixture.PageTokens)-1].Text
		_, err := iggcon.ParseMetadataCurrentPageToken(longest)
		require.NoError(t, err)
		_, err = iggcon.ParseMetadataCurrentPageToken(longest + "AAAA")
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	})

	t.Run("given_parsed_token_when_changing_its_last_key_copy_should_keep_the_token", func(t *testing.T) {
		token, err := iggcon.ParseMetadataCurrentPageToken(tokenText)
		require.NoError(t, err)
		key := token.LastKey()
		key[0] ^= 0xff
		assert.Equal(t, tokenText, token.String())
	})

	t.Run("given_page_token_when_round_tripping_through_json_should_keep_its_text_form", func(t *testing.T) {
		token, err := iggcon.ParseMetadataCurrentPageToken(tokenText)
		require.NoError(t, err)
		encoded, err := json.Marshal(iggcon.MetadataChanges{CurrentPageToken: token})
		require.NoError(t, err)
		assert.Contains(t, string(encoded), `"CurrentPageToken":"`+tokenText+`"`)
		var decoded iggcon.MetadataChanges
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		assert.Equal(t, token, decoded.CurrentPageToken)
	})

	t.Run("given_non_canonical_page_token_text_when_unmarshaling_json_should_refuse", func(t *testing.T) {
		var token iggcon.MetadataCurrentPageToken
		err := json.Unmarshal([]byte(`"`+tokenText+`="`), &token)
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	})

	t.Run("given_cursor_with_last_character_carrying_extra_bits_when_parsing_should_refuse", func(t *testing.T) {
		text := fixture.Cursors[0].Text
		last := text[len(text)-1]
		require.Equal(t, byte('A'), last)
		_, err := iggcon.ParseMetadataChangeCursor(text[:len(text)-1] + "B")
		assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	})

	t.Run("given_wrong_cursor_length_when_unmarshaling_should_refuse", func(t *testing.T) {
		var cursor iggcon.MetadataChangeCursor
		assert.ErrorIs(t, cursor.UnmarshalBinary(make([]byte, iggcon.MetadataChangeCursorSize-1)), ierror.ErrInvalidFormat)
		assert.ErrorIs(t, cursor.UnmarshalBinary(make([]byte, iggcon.MetadataChangeCursorSize+1)), ierror.ErrInvalidFormat)
		assert.NoError(t, cursor.UnmarshalBinary(make([]byte, iggcon.MetadataChangeCursorSize)))
	})
}

func TestMetadataChanges_Validate(t *testing.T) {
	fixture := loadMetadataChangesFixture(t)
	streamsToken, err := iggcon.ParseMetadataCurrentPageToken(fixture.PageTokens[0].Text)
	require.NoError(t, err)
	require.Equal(t, []iggcon.MetadataChangeGroup{iggcon.MetadataChangeGroupStreams}, streamsToken.Groups())
	cursor, err := iggcon.ParseMetadataChangeCursor(fixture.Cursors[2].Text)
	require.NoError(t, err)
	one := uint64(1)

	accepted := map[string]iggcon.MetadataChangesQuery{
		"limit_one":                   {Start: iggcon.EarliestMetadataChangeStart(), Limit: 1},
		"limit_maximum":               {Start: iggcon.EarliestMetadataChangeStart(), Limit: iggcon.MaxMetadataChangesLimit},
		"equal_time_bounds":           {Start: iggcon.AfterMetadataChangeStart(cursor), Since: &one, Until: &one, Limit: 1},
		"zero_cursor":                 iggcon.NewMetadataChangesQuery(iggcon.AfterMetadataChangeStart(iggcon.MetadataChangeCursor{})),
		"duplicate_groups":            iggcon.NewMetadataChangesQuery(iggcon.LatestMetadataChangeStart(), iggcon.MetadataChangeGroupUsers, iggcon.MetadataChangeGroupUsers),
		"current_token_of_same_group": iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(streamsToken), iggcon.MetadataChangeGroupStreams),
		"current_every_group":         iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(nil)),
		"current_members_with_parents": iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(nil),
			iggcon.MetadataChangeGroupConsumerGroupMembers, iggcon.MetadataChangeGroupConsumerGroups,
			iggcon.MetadataChangeGroupPartitions, iggcon.MetadataChangeGroupTopics, iggcon.MetadataChangeGroupStreams),
	}
	for name, query := range accepted {
		t.Run("given_"+name+"_when_validating_should_accept", func(t *testing.T) {
			assert.NoError(t, query.Validate())
		})
	}

	refused := map[string]iggcon.MetadataChangesQuery{
		"zero_value":                {},
		"unknown_group":             iggcon.NewMetadataChangesQuery(iggcon.EarliestMetadataChangeStart(), iggcon.MetadataChangeGroup(9)),
		"group_zero":                iggcon.NewMetadataChangesQuery(iggcon.EarliestMetadataChangeStart(), iggcon.MetadataChangeGroup(0)),
		"unknown_start":             iggcon.NewMetadataChangesQuery(iggcon.MetadataChangeStart{Mode: 4}),
		"current_sessions_alone":    iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(nil), iggcon.MetadataChangeGroupSessions),
		"current_token_of_all":      iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(streamsToken)),
		"current_zero_value_token":  iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(&iggcon.MetadataCurrentPageToken{}), iggcon.MetadataChangeGroupStreams),
		"after_op_zero_with_checks": iggcon.NewMetadataChangesQuery(iggcon.AfterMetadataChangeStart(iggcon.MetadataChangeCursor{Ordinal: 1})),
	}
	for name, query := range refused {
		t.Run("given_"+name+"_when_validating_should_refuse", func(t *testing.T) {
			assert.ErrorIs(t, query.Validate(), ierror.ErrInvalidCommand)
		})
	}

	t.Run("given_every_group_with_its_parents_when_scanning_current_state_should_accept_only_the_full_set", func(t *testing.T) {
		for name, entry := range fixture.Constants.Groups {
			group := mustGroup(t, name)
			alone := iggcon.NewMetadataChangesQuery(iggcon.CurrentMetadataChangeStart(nil), group)
			withParents := alone
			for _, parent := range entry.Parents {
				withParents.Groups = append(slices.Clone(withParents.Groups), mustGroup(t, parent))
			}
			if len(entry.Parents) == 0 {
				assert.NoError(t, alone.Validate(), name)
				continue
			}
			assert.ErrorIs(t, alone.Validate(), ierror.ErrInvalidCommand, name)
			assert.NoError(t, withParents.Validate(), name)
		}
	})
}

func TestMetadataChanges_Decoding(t *testing.T) {
	fixture := loadMetadataChangesFixture(t)

	t.Run("given_every_fixture_response_when_truncating_should_refuse_every_prefix", func(t *testing.T) {
		for _, vector := range fixture.Responses {
			response := decodeHex(t, vector.ResponseHex)
			for length := range len(response) {
				_, err := binaryserialization.DeserializeMetadataChanges(response[:length])
				require.ErrorIs(t, err, ierror.ErrInvalidFormat, "%s truncated to %d bytes", vector.Case, length)
			}
		}
	})

	t.Run("given_every_record_body_when_truncating_or_extending_should_refuse", func(t *testing.T) {
		for _, vector := range fixture.Records {
			body := decodeHex(t, vector.BodyHex)
			for length := range len(body) {
				payload := changesPagePayload(recordEnvelope(vector.Group, vector.Kind, body[:length]))
				_, err := binaryserialization.DeserializeMetadataChanges(payload)
				require.ErrorIs(t, err, ierror.ErrInvalidFormat, "%s truncated to %d bytes", vector.Case, length)
			}
			payload := changesPagePayload(recordEnvelope(vector.Group, vector.Kind, append(slices.Clone(body), 0)))
			_, err := binaryserialization.DeserializeMetadataChanges(payload)
			require.ErrorIs(t, err, ierror.ErrInvalidFormat, "%s with a trailing byte", vector.Case)
		}
	})

	t.Run("given_huge_record_count_when_decoding_should_refuse_without_allocating_it", func(t *testing.T) {
		payload := changesPagePayload()
		countAt := len(payload) - 4 - 2 - 4
		binary.LittleEndian.PutUint32(payload[countAt:], ^uint32(0))
		_, err := binaryserialization.DeserializeMetadataChanges(payload)
		assert.ErrorIs(t, err, ierror.ErrInvalidFormat)
	})

	t.Run("given_unknown_kind_of_a_known_group_when_decoding_should_keep_the_raw_body", func(t *testing.T) {
		body := []byte{0xde, 0xad}
		page := decodeChangesPage(t, recordEnvelope(uint8(iggcon.MetadataChangeGroupStreams), 9, body))
		require.Len(t, page.Records, 1)
		assert.Equal(t, iggcon.MetadataChangeKind(9), page.Records[0].Kind)
		assert.Equal(t, &iggcon.MetadataUnknownBody{Body: body}, page.Records[0].Body)
		assert.Equal(t, "unknown(9)", page.Records[0].Kind.String())
	})

	t.Run("given_unknown_item_group_when_decoding_should_keep_the_raw_body", func(t *testing.T) {
		item := append([]byte{200}, binary.LittleEndian.AppendUint32(nil, 3)...)
		page := decodeCurrentPage(t, append(item, 1, 2, 3))
		require.Len(t, page.Items, 1)
		assert.Equal(t, iggcon.MetadataChangeGroup(200), page.Items[0].Group)
		assert.Equal(t, &iggcon.MetadataUnknownBody{Body: []byte{1, 2, 3}}, page.Items[0].Body)
	})
}

type metadataChangesFixture struct {
	SchemaVersion int                      `json:"schema_version"`
	Constants     metadataChangesConstants `json:"constants"`
	Records       []struct {
		Case      string          `json:"case"`
		Group     uint8           `json:"group"`
		Kind      uint8           `json:"kind"`
		BodyHex   string          `json:"body_hex"`
		RecordHex string          `json:"record_hex"`
		Expected  json.RawMessage `json:"expected"`
	} `json:"records"`
	Items []struct {
		Case     string          `json:"case"`
		Group    uint8           `json:"group"`
		BodyHex  string          `json:"body_hex"`
		ItemHex  string          `json:"item_hex"`
		Expected json.RawMessage `json:"expected"`
	} `json:"items"`
	Cursors    []cursorVector    `json:"cursors"`
	PageTokens []pageTokenVector `json:"page_tokens"`
	Requests   []struct {
		Case       string                    `json:"case"`
		Input      metadataChangesQueryInput `json:"input"`
		RequestHex string                    `json:"request_hex"`
	} `json:"requests"`
	Responses []struct {
		Case        string          `json:"case"`
		ResponseHex string          `json:"response_hex"`
		Expected    json.RawMessage `json:"expected"`
	} `json:"responses"`
	LargeValues largeValuesVector `json:"large_values"`
	Refused     struct {
		Cursors      []textVector `json:"cursors"`
		PageTokens   []textVector `json:"page_tokens"`
		RecordBodies []struct {
			Case    string `json:"case"`
			Group   uint8  `json:"group"`
			Kind    uint8  `json:"kind"`
			BodyHex string `json:"body_hex"`
		} `json:"record_bodies"`
		Requests  []hexVector `json:"requests"`
		Responses []hexVector `json:"responses"`
		Queries   []struct {
			Case  string                    `json:"case"`
			Input metadataChangesQueryInput `json:"input"`
		} `json:"queries"`
	} `json:"refused"`
}

type metadataChangesConstants struct {
	CommandCode      uint32 `json:"command_code"`
	CursorPrefix     string `json:"cursor_prefix"`
	PageTokenPrefix  string `json:"page_token_prefix"`
	CursorSize       int    `json:"cursor_size"`
	CursorTextLength int    `json:"cursor_text_length"`
	MaxPageTokenSize int    `json:"max_page_token_size"`
	DefaultLimit     uint32 `json:"default_limit"`
	MaxLimit         uint32 `json:"max_limit"`
	Groups           map[string]struct {
		Code    uint8    `json:"code"`
		Parents []string `json:"parents"`
	} `json:"groups"`
	Kinds map[string]struct {
		Code  uint8  `json:"code"`
		Group string `json:"group"`
	} `json:"kinds"`
	Statuses                  map[string]uint8 `json:"statuses"`
	Phases                    map[string]uint8 `json:"phases"`
	StartModes                map[string]uint8 `json:"start_modes"`
	MemberLeftReasons         map[string]uint8 `json:"member_left_reasons"`
	TokenDeletedReasons       map[string]uint8 `json:"token_deleted_reasons"`
	SessionClosedReasons      map[string]uint8 `json:"session_closed_reasons"`
	PermissionsUpdatedReasons map[string]uint8 `json:"permissions_updated_reasons"`
}

type cursorVector struct {
	Case     string `json:"case"`
	Text     string `json:"text"`
	Hex      string `json:"hex"`
	LogID    string `json:"log_id"`
	Op       uint64 `json:"op"`
	Checksum string `json:"checksum"`
	Ordinal  uint32 `json:"ordinal"`
}

type pageTokenVector struct {
	Case         string `json:"case"`
	Text         string `json:"text"`
	Hex          string `json:"hex"`
	Anchor       string `json:"anchor"`
	RequiredHead uint64 `json:"required_head"`
	GroupsMask   uint8  `json:"groups_mask"`
	Group        string `json:"group"`
	LastKeyHex   string `json:"last_key_hex"`
}

type metadataChangesQueryInput struct {
	Start     string   `json:"start"`
	Cursor    *string  `json:"cursor"`
	PageToken *string  `json:"page_token"`
	Groups    []string `json:"groups"`
	Since     *uint64  `json:"since"`
	Until     *uint64  `json:"until"`
	Limit     uint32   `json:"limit"`
}

type largeValuesVector struct {
	RecordHex     string `json:"record_hex"`
	Op            string `json:"op"`
	Ordinal       uint32 `json:"ordinal"`
	Timestamp     string `json:"timestamp"`
	Checksum      string `json:"checksum"`
	UserCreatedAt string `json:"user_created_at"`
	Session       string `json:"session"`
	ClientID      string `json:"client_id"`
}

type textVector struct {
	Case string `json:"case"`
	Text string `json:"text"`
}

type hexVector struct {
	Case string `json:"case"`
	Hex  string `json:"hex"`
}

func loadMetadataChangesFixture(t *testing.T) metadataChangesFixture {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("testdata", "metadata_changes.json"))
	require.NoError(t, err)
	var fixture metadataChangesFixture
	require.NoError(t, json.Unmarshal(content, &fixture))
	require.Equal(t, 1, fixture.SchemaVersion)
	return fixture
}

func checkMetadataChangesConstants(t *testing.T, constants metadataChangesConstants) {
	assert.Equal(t, constants.CommandCode, uint32(command.GetMetadataChangesCode))
	assert.Equal(t, vsr.OperationNonReplicated, vsr.OperationForCode(constants.CommandCode))
	assert.Equal(t, constants.CursorPrefix, iggcon.MetadataChangeCursorPrefix)
	assert.Equal(t, constants.PageTokenPrefix, iggcon.MetadataCurrentPageTokenPrefix)
	assert.Equal(t, constants.CursorSize, iggcon.MetadataChangeCursorSize)
	assert.Len(t, iggcon.MetadataChangeCursor{}.String(), constants.CursorTextLength)
	assert.Equal(t, constants.DefaultLimit, iggcon.DefaultMetadataChangesLimit)
	assert.Equal(t, constants.DefaultLimit, iggcon.NewMetadataChangesQuery(iggcon.EarliestMetadataChangeStart()).Limit)
	assert.Equal(t, constants.MaxLimit, iggcon.MaxMetadataChangesLimit)

	for name, entry := range constants.Groups {
		group := mustGroup(t, name)
		assert.Equal(t, entry.Code, uint8(group), name)
		assert.Equal(t, name, group.String())
		assert.True(t, group.IsKnown(), name)
	}
	for code := range 256 {
		group := iggcon.MetadataChangeGroup(code)
		_, listed := constants.Groups[group.String()]
		assert.Equal(t, listed, group.IsKnown(), "group code %d", code)
	}
	for name, entry := range constants.Kinds {
		kind, err := iggcon.ParseMetadataChangeKind(name)
		require.NoError(t, err, name)
		assert.Equal(t, entry.Code, uint8(kind), name)
		assert.Equal(t, name, kind.String())
		assert.Equal(t, entry.Group, kind.Group().String(), name)
	}
	known := 0
	for code := range 256 {
		if iggcon.MetadataChangeKind(code).Group().IsKnown() {
			known++
		}
	}
	assert.Equal(t, len(constants.Kinds), known, "every kind this build knows is listed")
	_, err := iggcon.ParseMetadataChangeKind("stream_renamed")
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)
	_, err = iggcon.ParseMetadataChangeGroup("everything")
	assert.ErrorIs(t, err, ierror.ErrInvalidCommand)

	checkNames(t, constants.Statuses, func(code uint8) string { return iggcon.MetadataChangeStatus(code).String() })
	checkNames(t, constants.Phases, func(code uint8) string { return iggcon.MetadataChangePhase(code).String() })
	checkNames(t, constants.MemberLeftReasons, func(code uint8) string { return iggcon.MetadataMemberLeftReason(code).String() })
	checkNames(t, constants.TokenDeletedReasons, func(code uint8) string { return iggcon.MetadataTokenDeletedReason(code).String() })
	checkNames(t, constants.SessionClosedReasons, func(code uint8) string { return iggcon.MetadataSessionClosedReason(code).String() })
	checkNames(t, constants.PermissionsUpdatedReasons, func(code uint8) string {
		return iggcon.MetadataPermissionsUpdatedReason(code).String()
	})
	assert.Equal(t, map[string]uint8{
		"earliest": uint8(iggcon.MetadataChangeStartEarliest),
		"latest":   uint8(iggcon.MetadataChangeStartLatest),
		"after":    uint8(iggcon.MetadataChangeStartAfter),
		"current":  uint8(iggcon.MetadataChangeStartCurrent),
	}, constants.StartModes)
}

func checkNames(t *testing.T, names map[string]uint8, name func(uint8) string) {
	t.Helper()
	for want, code := range names {
		assert.Equal(t, want, name(code), "code %d", code)
	}
	assert.Equal(t, "unknown("+strconv.Itoa(len(names))+")", name(uint8(len(names))))
}

func checkCursorVector(t *testing.T, vector cursorVector) {
	cursor, err := iggcon.ParseMetadataChangeCursor(vector.Text)
	require.NoError(t, err)
	assert.Equal(t, vector.LogID, cursor.LogID.String())
	assert.Equal(t, vector.Op, cursor.Op)
	assert.Equal(t, vector.Checksum, cursor.Checksum.String())
	assert.Equal(t, vector.Ordinal, cursor.Ordinal)
	assert.Equal(t, vector.Text, cursor.String())

	encoded, err := cursor.AppendBinary(nil)
	require.NoError(t, err)
	assert.Equal(t, vector.Hex, hex.EncodeToString(encoded))
	var decoded iggcon.MetadataChangeCursor
	require.NoError(t, decoded.UnmarshalBinary(decodeHex(t, vector.Hex)))
	assert.Equal(t, cursor, decoded)
}

func checkPageTokenVector(t *testing.T, vector pageTokenVector) {
	token, err := iggcon.ParseMetadataCurrentPageToken(vector.Text)
	require.NoError(t, err)
	assert.Equal(t, vector.Anchor, token.Anchor().String())
	assert.Equal(t, vector.RequiredHead, token.RequiredHead())
	var mask uint8
	for _, group := range token.Groups() {
		mask |= 1 << (uint8(group) - 1)
	}
	assert.Equal(t, vector.GroupsMask, mask)
	assert.Equal(t, vector.Group, token.LastGroup().String())
	assert.Equal(t, vector.LastKeyHex, hex.EncodeToString(token.LastKey()))
	assert.Equal(t, vector.Text, token.String())

	encoded, err := token.AppendBinary(nil)
	require.NoError(t, err)
	assert.Equal(t, vector.Hex, hex.EncodeToString(encoded))
	decoded := &iggcon.MetadataCurrentPageToken{}
	require.NoError(t, decoded.UnmarshalBinary(decodeHex(t, vector.Hex)))
	assert.Equal(t, token, decoded)
}

func checkLargeValues(t *testing.T, vector largeValuesVector) {
	page := decodeChangesPage(t, decodeHex(t, vector.RecordHex))
	require.Len(t, page.Records, 1)
	record := page.Records[0]
	session, ok := record.Body.(*iggcon.MetadataSession)
	require.True(t, ok, "%T", record.Body)
	assert.Equal(t, vector.Op, strconv.FormatUint(record.Op, 10))
	assert.Equal(t, vector.Ordinal, record.Ordinal)
	assert.Equal(t, vector.Timestamp, strconv.FormatUint(record.Timestamp, 10))
	assert.Equal(t, vector.Checksum, record.Checksum.String())
	assert.Equal(t, vector.UserCreatedAt, strconv.FormatUint(session.UserCreatedAt, 10))
	assert.Equal(t, vector.Session, strconv.FormatUint(session.Session, 10))
	assert.Equal(t, vector.ClientID, session.ClientID.String())
}

// checkRefusedRequest proves the encoder cannot produce a request the server
// refuses: each vector names a shape the query either never encodes or
// refuses before sending.
func checkRefusedRequest(t *testing.T, name string, request []byte) {
	t.Helper()
	earliest := iggcon.NewMetadataChangesQuery(iggcon.EarliestMetadataChangeStart())
	switch name {
	case "cursor_bytes_without_after":
		var cursor iggcon.MetadataChangeCursor
		require.NoError(t, cursor.UnmarshalBinary(request[1:1+iggcon.MetadataChangeCursorSize]))
		earliest.Start.Cursor = cursor
		body, err := (&command.GetMetadataChanges{Query: earliest}).MarshalBinary()
		require.NoError(t, err)
		assert.NotEqual(t, request, body)
		assert.Equal(t, make([]byte, iggcon.MetadataChangeCursorSize), body[1:1+iggcon.MetadataChangeCursorSize])
	case "trailing_byte":
		body, err := (&command.GetMetadataChanges{Query: earliest}).MarshalBinary()
		require.NoError(t, err)
		assert.Equal(t, request[:len(request)-1], body)
	case "unknown_start_mode":
		query := earliest
		query.Start.Mode = iggcon.MetadataChangeStartMode(request[0])
		assert.ErrorIs(t, query.Validate(), ierror.ErrInvalidCommand)
	case "non_canonical_cursor":
		var cursor iggcon.MetadataChangeCursor
		assert.ErrorIs(t, cursor.UnmarshalBinary(request[1:1+iggcon.MetadataChangeCursorSize]), ierror.ErrInvalidFormat)
		query := earliest
		query.Start = iggcon.AfterMetadataChangeStart(iggcon.MetadataChangeCursor{
			LogID:    iggcon.Unsigned128{Low: binary.LittleEndian.Uint64(request[1:9]), High: binary.LittleEndian.Uint64(request[9:17])},
			Op:       binary.LittleEndian.Uint64(request[17:25]),
			Checksum: iggcon.Unsigned128{Low: binary.LittleEndian.Uint64(request[25:33]), High: binary.LittleEndian.Uint64(request[33:41])},
			Ordinal:  binary.LittleEndian.Uint32(request[41:45]),
		})
		assert.ErrorIs(t, query.Validate(), ierror.ErrInvalidCommand)
	default:
		require.FailNow(t, "a refused request vector this test does not cover", name)
	}
}

func fixtureQuery(t *testing.T, input metadataChangesQueryInput) iggcon.MetadataChangesQuery {
	t.Helper()
	query := iggcon.MetadataChangesQuery{Since: input.Since, Until: input.Until, Limit: input.Limit}
	for _, name := range input.Groups {
		query.Groups = append(query.Groups, mustGroup(t, name))
	}
	switch input.Start {
	case "earliest":
		query.Start = iggcon.EarliestMetadataChangeStart()
	case "latest":
		query.Start = iggcon.LatestMetadataChangeStart()
	case "after":
		require.NotNil(t, input.Cursor)
		cursor, err := iggcon.ParseMetadataChangeCursor(*input.Cursor)
		require.NoError(t, err)
		query.Start = iggcon.AfterMetadataChangeStart(cursor)
	case "current":
		var token *iggcon.MetadataCurrentPageToken
		if input.PageToken != nil {
			parsed, err := iggcon.ParseMetadataCurrentPageToken(*input.PageToken)
			require.NoError(t, err)
			token = parsed
		}
		query.Start = iggcon.CurrentMetadataChangeStart(token)
	default:
		require.FailNow(t, "unknown start", input.Start)
	}
	return query
}

func mustGroup(t *testing.T, name string) iggcon.MetadataChangeGroup {
	t.Helper()
	group, err := iggcon.ParseMetadataChangeGroup(name)
	require.NoError(t, err)
	return group
}

// recordEnvelope wraps a body as a record of op 1 by root.
func recordEnvelope(group, kind uint8, body []byte) []byte {
	record := binary.LittleEndian.AppendUint64(nil, 1)
	record = binary.LittleEndian.AppendUint32(record, 0)
	record = append(record, make([]byte, 16)...)
	record = binary.LittleEndian.AppendUint64(record, 7)
	record = append(record, 1, 0, 0, 0, 0, group, kind)
	record = binary.LittleEndian.AppendUint32(record, uint32(len(body)))
	return append(record, body...)
}

// changesPagePayload builds an ok changes page holding records.
func changesPagePayload(records ...[]byte) []byte {
	payload := pageHeader(iggcon.MetadataChangePhaseChanges)
	payload = binary.LittleEndian.AppendUint32(payload, uint32(len(records)))
	for _, record := range records {
		payload = append(payload, record...)
	}
	payload = binary.LittleEndian.AppendUint32(payload, 0)
	return binary.LittleEndian.AppendUint16(payload, 0)
}

// pageHeader is an ok page without a log id or cursor.
func pageHeader(phase iggcon.MetadataChangePhase) []byte {
	payload := []byte{byte(iggcon.MetadataChangeStatusOk), byte(phase), 0}
	payload = append(payload, make([]byte, 24)...)
	return append(payload, 0, 0)
}

func decodeChangesPage(t *testing.T, records ...[]byte) *iggcon.MetadataChanges {
	t.Helper()
	changes, err := binaryserialization.DeserializeMetadataChanges(changesPagePayload(records...))
	require.NoError(t, err)
	return changes
}

func decodeCurrentPage(t *testing.T, item []byte) *iggcon.MetadataChanges {
	t.Helper()
	payload := pageHeader(iggcon.MetadataChangePhaseCurrent)
	payload = binary.LittleEndian.AppendUint32(payload, 0)
	payload = binary.LittleEndian.AppendUint32(payload, 1)
	payload = append(payload, item...)
	payload = binary.LittleEndian.AppendUint16(payload, 0)
	changes, err := binaryserialization.DeserializeMetadataChanges(payload)
	require.NoError(t, err)
	return changes
}

func marshalView(t *testing.T, view any) string {
	t.Helper()
	encoded, err := json.Marshal(view)
	require.NoError(t, err)
	return string(encoded)
}

// changesView renders an answer in the HTTP JSON shape the fixture expects.
func changesView(t *testing.T, changes *iggcon.MetadataChanges) map[string]any {
	t.Helper()
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
		records = append(records, recordView(t, record))
	}
	items := make([]map[string]any, 0, len(changes.Items))
	for _, item := range changes.Items {
		items = append(items, itemView(t, item))
	}
	view["records"] = records
	view["items"] = items
	return view
}

func recordView(t *testing.T, record iggcon.MetadataChangeRecord) map[string]any {
	t.Helper()
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
		return view
	}
	addBodyView(t, record.Body, view)
	return view
}

func itemView(t *testing.T, item iggcon.MetadataCurrentItem) map[string]any {
	t.Helper()
	view := map[string]any{"group": item.Group.String()}
	if unknown, ok := item.Body.(*iggcon.MetadataUnknownBody); ok {
		view["group"] = strconv.Itoa(int(item.Group))
		view["body"] = base64.StdEncoding.EncodeToString(unknown.Body)
		return view
	}
	addBodyView(t, item.Body, view)
	return view
}

func addBodyView(t *testing.T, body iggcon.MetadataChangeBody, view map[string]any) {
	t.Helper()
	switch body := body.(type) {
	case *iggcon.MetadataStreamChange:
		addStreamKey(view, body.MetadataStreamKey)
		addFields(view, "created_at", body.CreatedAt, "name", body.Name)
		addOptions(t, view, body.Options)
	case *iggcon.MetadataStreamDeleted:
		addStreamKey(view, body.MetadataStreamKey)
		addFields(view, "created_at", body.CreatedAt, "name", body.Name)
		addOptional(view, "requested_op", body.RequestedOp)
	case *iggcon.MetadataTopicChange:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "created_at", body.CreatedAt, "name", body.Name, "partitions_count", body.PartitionsCount)
		addOptions(t, view, body.Options)
	case *iggcon.MetadataTopicItem:
		addTopicKey(view, body.MetadataTopicKey)
		addFields(view, "created_at", body.CreatedAt, "name", body.Name)
		addOptions(t, view, body.Options)
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
		addFields(view, "username", body.Username, "status", userStatusName(t, body.Status),
			"permissions", permissionsView(body.Permissions))
		addOptions(t, view, body.Options)
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
		require.FailNow(t, "unexpected body", "%T", body)
	}
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
	for index := 0; index < len(pairs); index += 2 {
		view[pairs[index].(string)] = pairs[index+1]
	}
}

func addOptional[T any](view map[string]any, name string, value *T) {
	if value != nil {
		view[name] = *value
	}
}

func addOptions(t *testing.T, view map[string]any, options iggcon.MetadataOptions) {
	t.Helper()
	if options.Unreadable {
		assert.Nil(t, options.Options)
		assert.Nil(t, options.DerivedOptions)
		view["options"] = nil
		view["options_unreadable"] = true
		return
	}
	view["options"] = optionViews(t, options.Options, options.DerivedOptions)
}

func userStatusName(t *testing.T, status iggcon.UserStatus) string {
	t.Helper()
	names := map[iggcon.UserStatus]string{iggcon.Active: "active", iggcon.Inactive: "inactive"}
	name, ok := names[status]
	require.True(t, ok, "unexpected status %d", status)
	return name
}

// permissionsView renders permissions in the get_user JSON shape.
func permissionsView(permissions *iggcon.Permissions) any {
	if permissions == nil {
		return nil
	}
	global := permissions.Global
	view := map[string]any{
		"global": map[string]bool{
			"manage_servers": global.ManageServers, "read_servers": global.ReadServers,
			"manage_users": global.ManageUsers, "read_users": global.ReadUsers,
			"manage_streams": global.ManageStreams, "read_streams": global.ReadStreams,
			"manage_topics": global.ManageTopics, "read_topics": global.ReadTopics,
			"poll_messages": global.PollMessages, "send_messages": global.SendMessages,
		},
		"streams": nil,
	}
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
