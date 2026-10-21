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
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
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

func TestResourceOptions_SharedVectors(t *testing.T) {
	fixture := loadFixture(t)

	t.Run("given_shared_vectors_when_encoding_should_match_server_contract", func(t *testing.T) {
		t.Run("constants", func(t *testing.T) { checkConstants(t, fixture.Constants) })
		t.Run("catalog", func(t *testing.T) { checkCatalog(t, fixture.Catalog) })
		t.Run("typed_surface", func(t *testing.T) { checkTypedSurface(t, fixture.Catalog) })
		t.Run("fingerprint", func(t *testing.T) { checkFingerprint(t, fixture.StreamKey) })

		for _, vector := range fixture.DescribeOptions {
			t.Run("describe_options/"+vector.Scope, func(t *testing.T) {
				checkDescribeOptions(t, fixture.Constants, vector)
			})
		}
		for _, vector := range fixture.UpdateResource {
			t.Run("update_resource/"+vector.Case, func(t *testing.T) {
				body, err := updateCommand(t, vector.Scope, vector.Target, vector.Input).MarshalBinary()
				require.NoError(t, err)
				assert.Equal(t, vector.BodyHex, hex.EncodeToString(body))
			})
		}
		for _, vector := range fixture.UpdateResourceErrors {
			t.Run("update_resource_errors/"+vector.Case, func(t *testing.T) {
				_, err := updateCommand(t, vector.Scope, vector.Target, vector.Input).MarshalBinary()
				requireIggyError(t, vector.Error, err)
			})
		}
		for _, vector := range fixture.CreateStream {
			t.Run("create_stream/"+vector.Case, func(t *testing.T) {
				body, err := createStreamCommand(t, vector.Input).MarshalBinary()
				require.NoError(t, err)
				assert.Equal(t, vector.BodyHex, hex.EncodeToString(body))
			})
		}
		for _, vector := range fixture.CreateTopic {
			t.Run("create_topic/"+vector.Case, func(t *testing.T) {
				body, err := createTopicCommand(t, vector.Input).MarshalBinary()
				require.NoError(t, err)
				assert.Equal(t, vector.BodyHex, hex.EncodeToString(body))
			})
		}
		for _, vector := range fixture.CreateUser {
			t.Run("create_user/"+vector.Case, func(t *testing.T) {
				body, err := createUserCommand(t, vector.Input).MarshalBinary()
				require.NoError(t, err)
				assert.Equal(t, vector.BodyHex, hex.EncodeToString(body))
			})
		}
		for _, vector := range fixture.StreamResponses {
			t.Run("stream_responses/"+vector.Case, func(t *testing.T) {
				checkStreamResponse(t, fixture.Constants, vector)
			})
		}
		for _, vector := range fixture.TopicResponses {
			t.Run("topic_responses/"+vector.Case, func(t *testing.T) {
				checkTopicResponse(t, fixture.Constants, vector)
			})
		}
		for _, vector := range fixture.UserResponses {
			t.Run("user_responses/"+vector.Case, func(t *testing.T) {
				checkUserResponse(t, fixture.Constants, vector)
			})
		}
	})
}

func TestResourceOptions_Diagnostics(t *testing.T) {
	t.Run("given_raw_entry_of_a_key_newer_than_this_build_when_printing_should_redact_its_value", func(t *testing.T) {
		secret := "future-secret-value"
		raw := []iggcon.HeaderEntry{{
			Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("future_secret")},
			Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(secret)},
		}}
		name := "orders"
		values := []any{
			iggcon.StreamOptions{Raw: raw},
			iggcon.TopicOptions{Raw: raw},
			iggcon.UserOptions{Raw: raw},
			iggcon.StreamUpdate{Name: &name, Options: raw},
			iggcon.TopicUpdate{Name: &name, Options: raw},
			iggcon.UserUpdate{Username: &name, Options: raw},
		}
		for _, value := range values {
			printed := fmt.Sprintf("%+v", value)
			assert.NotContains(t, printed, secret, "%T", value)
			assert.Contains(t, printed, "future_secret", "%T", value)
			encoded, err := json.Marshal(value)
			require.NoError(t, err)
			assert.NotContains(t, string(encoded), secret, "JSON of %T", value)
			assert.Contains(t, string(encoded), "future_secret", "JSON of %T", value)
		}
	})

	t.Run("given_typed_fields_when_printing_should_show_every_field", func(t *testing.T) {
		size := uint64(4096)
		durability := iggcon.DurabilityPersisted
		printed := fmt.Sprintf("%v", iggcon.TopicOptions{MaxTopicSize: &size, Durability: &durability})
		assert.Contains(t, printed, "MaxTopicSize:4096")
		assert.Contains(t, printed, "Durability:persisted")
		assert.Contains(t, printed, "SegmentSize:<nil>")
	})
}

func TestResourceOptions_InvalidKeys(t *testing.T) {
	fixture := loadFixture(t)

	t.Run("given_invalid_key_when_encoding_should_reject_before_network_io", func(t *testing.T) {
		for _, vector := range fixture.StreamKey.InvalidKeys {
			t.Run(vector.Case, func(t *testing.T) {
				if vector.Input.RawHex != "" {
					raw := decodeHex(t, vector.Input.RawHex)
					_, err := iggcon.EncryptionKeyFingerprint(raw)
					requireIggyError(t, vector.Error, err)
					_, err = iggcon.EncryptionKeyOption(raw)
					requireIggyError(t, vector.Error, err)
					_, err = (&command.CreateStream{Name: "orders", Options: iggcon.StreamOptions{EncryptionKey: raw}}).MarshalBinary()
					requireIggyError(t, vector.Error, err)
					return
				}
				_, err := iggcon.DecodeEncryptionKey(vector.Input.Base64)
				requireIggyError(t, vector.Error, err)
			})
		}

		t.Run("empty_key_is_not_absent", func(t *testing.T) {
			_, err := (&command.CreateStream{Name: "orders", Options: iggcon.StreamOptions{EncryptionKey: []byte{}}}).MarshalBinary()
			assert.ErrorIs(t, err, ierror.ErrInvalidEncryptionKey)
		})

		t.Run("exactly_32_bytes_are_accepted", func(t *testing.T) {
			key := decodeHex(t, fixture.StreamKey.KeyHex)
			decoded, err := iggcon.DecodeEncryptionKey(fixture.StreamKey.KeyBase64)
			require.NoError(t, err)
			assert.Equal(t, key, decoded)
			entry, err := iggcon.EncryptionKeyOption(key)
			require.NoError(t, err)
			assert.Equal(t, iggcon.HeaderEntry{
				Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("encryption_key")},
				Value: iggcon.HeaderValue{Kind: iggcon.Raw, Value: key},
			}, entry)
			_, err = (&command.CreateStream{Name: "orders", Options: iggcon.StreamOptions{EncryptionKey: key}}).MarshalBinary()
			require.NoError(t, err)
		})

		t.Run("diagnostics_never_show_the_key", func(t *testing.T) {
			key := decodeHex(t, fixture.StreamKey.KeyHex)
			options := iggcon.StreamOptions{
				EncryptionKey: key,
				Raw:           []iggcon.HeaderEntry{{Key: iggcon.HeaderKey{Kind: iggcon.String, Value: []byte("encryption_key")}, Value: iggcon.HeaderValue{Kind: iggcon.Raw, Value: key}}},
			}
			request := &command.CreateStream{Name: "orders", Options: options}
			secrets := []string{fixture.StreamKey.KeyHex, fixture.StreamKey.KeyBase64, hex.EncodeToString(key), string(key)}
			for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%x", "%d", "%q"} {
				for _, value := range []any{options, &options, request, *request} {
					printed := fmt.Sprintf(verb, value)
					for _, secret := range secrets {
						assert.NotContains(t, printed, secret, "%s of %T", verb, value)
					}
				}
			}
			for _, value := range []any{options, &options, request} {
				encoded, err := json.Marshal(value)
				require.NoError(t, err)
				for _, secret := range secrets {
					assert.NotContains(t, string(encoded), secret, "JSON of %T", value)
				}
			}
			var logged bytes.Buffer
			slog.New(slog.NewJSONHandler(&logged, nil)).Info("create", "options", options, "request", request)
			slog.New(slog.NewTextHandler(&logged, nil)).Info("create", "options", options, "request", request)
			for _, secret := range secrets {
				assert.NotContains(t, logged.String(), secret, "slog output")
			}
			short := key[:31]
			_, err := (&command.CreateStream{Name: "orders", Options: iggcon.StreamOptions{EncryptionKey: short}}).MarshalBinary()
			require.Error(t, err)
			assert.NotContains(t, err.Error(), hex.EncodeToString(short))
		})
	})
}

type fixtureFile struct {
	SchemaVersion        int                    `json:"schema_version"`
	Constants            fixtureConstants       `json:"constants"`
	Catalog              []catalogRow           `json:"catalog"`
	DescribeOptions      []describeVector       `json:"describe_options"`
	StreamKey            streamKeyVector        `json:"stream_key"`
	UpdateResource       []updateVector         `json:"update_resource"`
	UpdateResourceErrors []updateErrorVector    `json:"update_resource_errors"`
	CreateStream         []createVector         `json:"create_stream"`
	CreateTopic          []createVector         `json:"create_topic"`
	CreateUser           []createVector         `json:"create_user"`
	StreamResponses      []streamResponseVector `json:"stream_responses"`
	TopicResponses       []topicResponseVector  `json:"topic_responses"`
	UserResponses        []userResponseVector   `json:"user_responses"`
}

type fixtureConstants struct {
	ProtocolVersion     protocolVersion           `json:"protocol_version"`
	CommandCodes        map[string]commandCode    `json:"command_codes"`
	ErrorCodes          map[string]uint32         `json:"error_codes"`
	RetiredCommandCodes map[string]uint32         `json:"retired_command_codes"`
	Operations          map[string]operationEntry `json:"operations"`
	RetiredOperations   map[string]uint8          `json:"retired_operations"`
	ScopeCodes          map[string]uint8          `json:"scope_codes"`
	HeaderKindCodes     map[string]int            `json:"header_kind_codes"`
	OptionFlags         map[string]uint8          `json:"option_flags"`
	StreamKeyLength     int                       `json:"stream_key_length"`
	NeverReturnedKeys   []string                  `json:"never_returned_keys"`
}

type protocolVersion struct {
	Packed uint32 `json:"packed"`
	Text   string `json:"text"`
}

type commandCode struct {
	Code      uint32 `json:"code"`
	Operation *uint8 `json:"operation"`
}

type operationEntry struct {
	Value            uint8 `json:"value"`
	Internal         bool  `json:"internal"`
	RequestOperation uint8 `json:"request_operation"`
}

type catalogRow struct {
	Key        string   `json:"key"`
	Scopes     []string `json:"scopes"`
	Mutability string   `json:"mutability"`
	Visibility string   `json:"visibility"`
	Storage    string   `json:"storage"`
	Flags      uint8    `json:"flags"`
	Described  bool     `json:"described"`
}

type describeVector struct {
	Scope       string          `json:"scope"`
	RequestHex  string          `json:"request_hex"`
	ResponseHex string          `json:"response_hex"`
	Entries     []describeEntry `json:"entries"`
}

type describeEntry struct {
	Key     string  `json:"key"`
	Kind    string  `json:"kind"`
	Flags   uint8   `json:"flags"`
	Default *string `json:"default"`
}

type streamKeyVector struct {
	KeyHex      string       `json:"key_hex"`
	KeyBase64   string       `json:"key_base64"`
	Fingerprint string       `json:"fingerprint"`
	InvalidKeys []invalidKey `json:"invalid_keys"`
}

type invalidKey struct {
	Case  string `json:"case"`
	Input struct {
		RawHex string `json:"raw_hex"`
		Base64 string `json:"base64"`
	} `json:"input"`
	Error fixtureIggyCode `json:"error"`
}

type fixtureIggyCode struct {
	Name string `json:"name"`
	Code uint32 `json:"code"`
}

type updateVector struct {
	Case    string                     `json:"case"`
	Scope   string                     `json:"scope"`
	Target  map[string]json.RawMessage `json:"target"`
	Input   map[string]json.RawMessage `json:"input"`
	BodyHex string                     `json:"body_hex"`
}

type updateErrorVector struct {
	Case   string                     `json:"case"`
	Scope  string                     `json:"scope"`
	Target map[string]json.RawMessage `json:"target"`
	Input  map[string]json.RawMessage `json:"input"`
	Error  fixtureIggyCode            `json:"error"`
}

type createVector struct {
	Case    string                     `json:"case"`
	Input   map[string]json.RawMessage `json:"input"`
	BodyHex string                     `json:"body_hex"`
}

type optionView struct {
	Value    string `json:"value"`
	Kind     string `json:"kind"`
	Explicit bool   `json:"explicit"`
}

type streamResponseVector struct {
	Case        string `json:"case"`
	ResponseHex string `json:"response_hex"`
	Expected    struct {
		Id                        uint32                `json:"id"`
		CreatedAt                 uint64                `json:"created_at"`
		Name                      string                `json:"name"`
		TopicsCount               uint32                `json:"topics_count"`
		SizeBytes                 uint64                `json:"size_bytes"`
		MessagesCount             uint64                `json:"messages_count"`
		Options                   map[string]optionView `json:"options"`
		EncryptionKeySource       string                `json:"encryption_key_source"`
		EncryptionKeyFingerprint  *string               `json:"encryption_key_fingerprint"`
		EncryptionKekFingerprints []string              `json:"encryption_kek_fingerprints"`
	} `json:"expected"`
}

type topicResponseVector struct {
	Case        string `json:"case"`
	ResponseHex string `json:"response_hex"`
	Expected    struct {
		Id                   uint32                `json:"id"`
		CreatedAt            uint64                `json:"created_at"`
		Name                 string                `json:"name"`
		PartitionsCount      uint32                `json:"partitions_count"`
		SizeBytes            uint64                `json:"size_bytes"`
		MessagesCount        uint64                `json:"messages_count"`
		Options              map[string]optionView `json:"options"`
		CompressionAlgorithm string                `json:"compression_algorithm"`
		MessageExpiry        string                `json:"message_expiry"`
		MaxTopicSize         string                `json:"max_topic_size"`
	} `json:"expected"`
}

type userResponseVector struct {
	Case        string `json:"case"`
	ResponseHex string `json:"response_hex"`
	Expected    struct {
		Id        uint32                `json:"id"`
		CreatedAt uint64                `json:"created_at"`
		Status    string                `json:"status"`
		Username  string                `json:"username"`
		Options   map[string]optionView `json:"options"`
	} `json:"expected"`
}

func loadFixture(t *testing.T) fixtureFile {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("testdata", "resource_options.json"))
	require.NoError(t, err)
	var fixture fixtureFile
	require.NoError(t, json.Unmarshal(content, &fixture))
	require.Equal(t, 1, fixture.SchemaVersion)
	return fixture
}

func checkConstants(t *testing.T, constants fixtureConstants) {
	assert.Equal(t, constants.ProtocolVersion.Packed, vsr.ProtocolVersion, constants.ProtocolVersion.Text)
	assert.Equal(t, uint32(ierror.CannotModifyRootUserCode), constants.ErrorCodes["cannot_modify_root_user"])

	codes := map[string]command.Code{
		"create_stream":    command.CreateStreamCode,
		"create_topic":     command.CreateTopicCode,
		"create_user":      command.CreateUserCode,
		"describe_options": command.DescribeOptionsCode,
		"get_stream":       command.GetStreamCode,
		"get_streams":      command.GetStreamsCode,
		"get_topic":        command.GetTopicCode,
		"get_topics":       command.GetTopicsCode,
		"get_user":         command.GetUserCode,
		"get_users":        command.GetUsersCode,
		"update_resource":  command.UpdateResourceCode,
	}
	require.Len(t, codes, len(constants.CommandCodes))
	for name, want := range constants.CommandCodes {
		code, ok := codes[name]
		require.True(t, ok, "the SDK has no command code for %s", name)
		assert.Equal(t, want.Code, uint32(code), name)
		if want.Operation == nil {
			assert.Equal(t, vsr.OperationNonReplicated, vsr.OperationForCode(want.Code), name)
			continue
		}
		assert.Equal(t, vsr.Operation(*want.Operation), vsr.OperationForCode(want.Code), name)
	}
	for name, code := range constants.RetiredCommandCodes {
		_, replicated := vsr.ReplicatedOperation(code)
		assert.False(t, replicated, "retired %s code %d", name, code)
	}

	operations := map[string]vsr.Operation{
		"rewrap_stream_keys":     vsr.OperationRewrapStreamKeys,
		"update_resource":        vsr.OperationUpdateResource,
		"update_stream_resolved": vsr.OperationUpdateStreamResolved,
		"update_topic_resolved":  vsr.OperationUpdateTopicResolved,
		"update_user_resolved":   vsr.OperationUpdateUserResolved,
	}
	require.Len(t, operations, len(constants.Operations))
	for name, want := range constants.Operations {
		operation := operations[name]
		assert.Equal(t, want.Value, uint8(operation), name)
		assert.Equal(t, want.Internal, vsr.IsInternal(operation), name)
		assert.True(t, vsr.IsKnownOperation(operation), name)
		assert.True(t, vsr.ReplyOperationMatches(vsr.Operation(want.RequestOperation), operation), name)
	}
	for name, value := range constants.RetiredOperations {
		assert.False(t, vsr.IsKnownOperation(vsr.Operation(value)), "retired %s operation %d", name, value)
	}

	assert.Equal(t, map[string]uint8{
		"topic":  uint8(iggcon.OptionsScopeTopic),
		"stream": uint8(iggcon.OptionsScopeStream),
		"user":   uint8(iggcon.OptionsScopeUser),
	}, constants.ScopeCodes)
	for name, code := range constants.HeaderKindCodes {
		assert.Equal(t, code, int(headerKind(t, name)), name)
	}
	assert.Equal(t, constants.StreamKeyLength, iggcon.StreamEncryptionKeyLength)
}

// checkCatalog proves the typed key constants name exactly the described keys
// of each scope, and that a reset of any catalog key fails before sending with
// the category server admission would return.
func checkCatalog(t *testing.T, catalog []catalogRow) {
	typed := map[string][]string{
		"stream": {
			string(iggcon.StreamOptionName),
			string(iggcon.StreamOptionEncryptionKey),
			string(iggcon.StreamOptionEncryptionKeySource),
			string(iggcon.StreamOptionEncryptionKeyFingerprint),
			string(iggcon.StreamOptionEncryptionKekFingerprint),
		},
		"topic": {
			string(iggcon.TopicOptionName),
			string(iggcon.TopicOptionCompressionAlgorithm),
			string(iggcon.TopicOptionMessageExpiry),
			string(iggcon.TopicOptionMaxTopicSize),
			string(iggcon.TopicOptionSegmentSize),
			string(iggcon.TopicOptionDurability),
			string(iggcon.TopicOptionConsumerOffsetDurability),
			string(iggcon.TopicOptionPartitionResizePolicy),
			string(iggcon.TopicOptionMessagesRequiredToSave),
			string(iggcon.TopicOptionSizeOfMessagesRequiredToSave),
			string(iggcon.TopicOptionPreallocateSegments),
		},
		"user": {
			string(iggcon.UserOptionUsername),
			string(iggcon.UserOptionStatus),
		},
	}
	described := map[string][]string{}
	for _, row := range catalog {
		if !row.Described {
			continue
		}
		for _, scope := range row.Scopes {
			described[scope] = append(described[scope], row.Key)
		}
	}
	for scope, keys := range typed {
		assert.ElementsMatch(t, described[scope], keys, scope)
	}

	targets := map[string]map[string]json.RawMessage{
		"stream": {"stream_id": json.RawMessage("1")},
		"topic":  {"stream_id": json.RawMessage("1"), "topic_id": json.RawMessage("2")},
		"user":   {"user_id": json.RawMessage("1")},
	}
	for _, row := range catalog {
		for scope, target := range targets {
			t.Run(scope+"/reset_"+row.Key, func(t *testing.T) {
				input := map[string]json.RawMessage{"reset": json.RawMessage(`["` + row.Key + `"]`)}
				_, err := updateCommand(t, scope, target, input).MarshalBinary()
				switch {
				case !slices.Contains(row.Scopes, scope):
					assert.ErrorIs(t, err, ierror.ErrUnsupportedOptionKey)
				case row.Visibility == "internal" || row.Visibility == "read_only":
					assert.ErrorIs(t, err, ierror.ErrReadOnlyOption)
				case row.Mutability != "updatable":
					assert.ErrorIs(t, err, ierror.ErrOptionNotUpdatable)
				case row.Flags&2 == 0:
					assert.ErrorIs(t, err, ierror.ErrOptionNotResettable)
				default:
					assert.NoError(t, err)
				}
			})
		}
	}
}

// checkTypedSurface proves the typed API follows each catalog row: a create
// field for every client-settable map key, an update field for every
// updatable key, a reset constant for every resettable key and an accessor
// for every read-only key. A row whose mutability changes fails here until
// the typed API follows it.
func checkTypedSurface(t *testing.T, catalog []catalogRow) {
	type surface struct {
		create, update, reset, accessor string
	}
	typed := map[string]map[string]surface{
		"stream": {
			"name":                       {update: "Name"},
			"encryption_key":             {create: "EncryptionKey"},
			"encryption_key_source":      {accessor: "EncryptionKeySource"},
			"encryption_key_fingerprint": {accessor: "EncryptionKeyFingerprint"},
			"encryption_kek_fingerprint": {accessor: "EncryptionKekFingerprints"},
		},
		"topic": {
			"name":                              {update: "Name"},
			"compression_algorithm":             {create: "CompressionAlgorithm"},
			"message_expiry":                    {create: "MessageExpiry", update: "MessageExpiry", reset: string(iggcon.TopicOptionMessageExpiry)},
			"max_topic_size":                    {create: "MaxTopicSize", update: "MaxTopicSize", reset: string(iggcon.TopicOptionMaxTopicSize)},
			"segment_size":                      {create: "SegmentSize"},
			"durability":                        {create: "Durability"},
			"consumer_offset_durability":        {create: "ConsumerOffsetDurability"},
			"partition_resize_policy":           {create: "PartitionResizePolicy"},
			"messages_required_to_save":         {create: "MessagesRequiredToSave"},
			"size_of_messages_required_to_save": {create: "SizeOfMessagesRequiredToSave"},
			"preallocate_segments":              {create: "PreallocateSegments"},
		},
		"user": {
			"username": {update: "Username"},
			"status":   {update: "Status"},
		},
	}
	createTypes := map[string]reflect.Type{
		"stream": reflect.TypeFor[iggcon.StreamOptions](),
		"topic":  reflect.TypeFor[iggcon.TopicOptions](),
		"user":   reflect.TypeFor[iggcon.UserOptions](),
	}
	updateTypes := map[string]reflect.Type{
		"stream": reflect.TypeFor[iggcon.StreamUpdate](),
		"topic":  reflect.TypeFor[iggcon.TopicUpdate](),
		"user":   reflect.TypeFor[iggcon.UserUpdate](),
	}
	accessorTypes := map[string]reflect.Type{"stream": reflect.TypeFor[iggcon.Stream]()}
	for _, row := range catalog {
		if row.Visibility == "internal" {
			continue
		}
		for _, scope := range row.Scopes {
			name := scope + "/" + row.Key
			expected, known := typed[scope][row.Key]
			if !assert.True(t, known, "%s has no typed surface", name) {
				continue
			}
			clientSettable := row.Storage == "map" && row.Mutability != "read_only"
			assert.Equal(t, clientSettable, expected.create != "", "%s create field", name)
			if expected.create != "" {
				_, found := createTypes[scope].FieldByName(expected.create)
				assert.True(t, found, "%s create field %s", name, expected.create)
			}
			assert.Equal(t, row.Mutability == "updatable", expected.update != "", "%s update field", name)
			if expected.update != "" {
				_, found := updateTypes[scope].FieldByName(expected.update)
				assert.True(t, found, "%s update field %s", name, expected.update)
			}
			assert.Equal(t, row.Flags&2 != 0, expected.reset != "", "%s reset constant", name)
			if expected.reset != "" {
				assert.Equal(t, row.Key, expected.reset, "%s reset constant", name)
			}
			assert.Equal(t, row.Mutability == "read_only", expected.accessor != "", "%s accessor", name)
			if expected.accessor != "" {
				_, found := accessorTypes[scope].MethodByName(expected.accessor)
				assert.True(t, found, "%s accessor %s", name, expected.accessor)
			}
		}
	}
	for scope, keys := range typed {
		for key := range keys {
			assert.True(t, slices.ContainsFunc(catalog, func(row catalogRow) bool {
				return row.Key == key && slices.Contains(row.Scopes, scope)
			}), "%s/%s is not in the catalog", scope, key)
		}
	}
}

func checkFingerprint(t *testing.T, vector streamKeyVector) {
	key := decodeHex(t, vector.KeyHex)
	fingerprint, err := iggcon.EncryptionKeyFingerprint(key)
	require.NoError(t, err)
	assert.Equal(t, vector.Fingerprint, fingerprint)

	decoded, err := iggcon.DecodeEncryptionKey(vector.KeyBase64)
	require.NoError(t, err)
	fingerprint, err = iggcon.EncryptionKeyFingerprint(decoded)
	require.NoError(t, err)
	assert.Equal(t, vector.Fingerprint, fingerprint)
}

func checkDescribeOptions(t *testing.T, constants fixtureConstants, vector describeVector) {
	request, err := (&command.DescribeOptions{Scope: optionsScope(t, vector.Scope)}).MarshalBinary()
	require.NoError(t, err)
	assert.Equal(t, vector.RequestHex, hex.EncodeToString(request))

	specs, err := binaryserialization.DeserializeOptionSpecs(decodeHex(t, vector.ResponseHex))
	require.NoError(t, err)
	got := make([]describeEntry, 0, len(specs))
	for _, spec := range specs {
		var flags uint8
		for name, set := range map[string]bool{
			"updatable":  spec.Updatable,
			"resettable": spec.Resettable,
			"secret":     spec.Secret,
			"read_only":  spec.ReadOnly,
			"critical":   spec.Critical,
		} {
			if set {
				flags |= constants.OptionFlags[name]
			}
		}
		var defaultText *string
		if len(spec.DefaultValue.Value) > 0 {
			text := valueText(t, spec.DefaultValue)
			defaultText = &text
		}
		assert.NotEmpty(t, spec.Description, spec.Key)
		got = append(got, describeEntry{Key: spec.Key, Kind: kindName(t, spec.DefaultValue.Kind), Flags: flags, Default: defaultText})
	}
	assert.Equal(t, vector.Entries, got)
}

func checkStreamResponse(t *testing.T, constants fixtureConstants, vector streamResponseVector) {
	response := decodeHex(t, vector.ResponseHex)
	details, err := binaryserialization.DeserializeStream(response)
	require.NoError(t, err)
	streams, err := binaryserialization.DeserializeStreams(response)
	require.NoError(t, err)
	require.Len(t, streams, 1)
	assert.Equal(t, details.Stream, streams[0])

	stream := details.Stream
	expected := vector.Expected
	assert.Equal(t, expected.Id, stream.Id)
	assert.Equal(t, expected.CreatedAt, stream.CreatedAt)
	assert.Equal(t, expected.Name, stream.Name)
	assert.Equal(t, expected.TopicsCount, stream.TopicsCount)
	assert.Equal(t, expected.SizeBytes, stream.SizeBytes)
	assert.Equal(t, expected.MessagesCount, stream.MessagesCount)
	assert.Equal(t, expected.Options, optionViews(t, stream.Options, stream.DerivedOptions))
	requireNoSecret(t, constants, stream.Options, stream.DerivedOptions)

	source, ok := stream.EncryptionKeySource()
	assert.True(t, ok)
	assert.Equal(t, iggcon.StreamEncryptionSource(expected.EncryptionKeySource), source)
	fingerprint, ok := details.EncryptionKeyFingerprint()
	if expected.EncryptionKeyFingerprint == nil {
		assert.False(t, ok)
	} else {
		assert.True(t, ok)
		assert.Equal(t, *expected.EncryptionKeyFingerprint, fingerprint)
	}
	if len(expected.EncryptionKekFingerprints) == 0 {
		assert.Empty(t, stream.EncryptionKekFingerprints())
	} else {
		assert.Equal(t, expected.EncryptionKekFingerprints, stream.EncryptionKekFingerprints())
	}
}

func checkTopicResponse(t *testing.T, constants fixtureConstants, vector topicResponseVector) {
	details, err := binaryserialization.DeserializeTopic(decodeHex(t, vector.ResponseHex))
	require.NoError(t, err)
	topic := details.Topic
	expected := vector.Expected
	assert.Equal(t, expected.Id, topic.Id)
	assert.Equal(t, expected.CreatedAt, topic.CreatedAt)
	assert.Equal(t, expected.Name, topic.Name)
	assert.Equal(t, expected.PartitionsCount, topic.PartitionsCount)
	assert.Equal(t, expected.SizeBytes, topic.Size)
	assert.Equal(t, expected.MessagesCount, topic.MessagesCount)
	assert.Equal(t, expected.Options, optionViews(t, topic.Options, topic.DerivedOptions))
	requireNoSecret(t, constants, topic.Options, topic.DerivedOptions)

	compression := map[string]iggcon.CompressionAlgorithm{"none": iggcon.CompressionAlgorithmNone, "gzip": iggcon.CompressionAlgorithmGzip}
	assert.Equal(t, uint8(compression[expected.CompressionAlgorithm]), topic.CompressionAlgorithm)
	assert.Equal(t, parseUint64(t, expected.MessageExpiry), uint64(topic.MessageExpiry))
	assert.Equal(t, parseUint64(t, expected.MaxTopicSize), topic.MaxTopicSize)
}

func checkUserResponse(t *testing.T, constants fixtureConstants, vector userResponseVector) {
	details, err := binaryserialization.DeserializeUser(decodeHex(t, vector.ResponseHex))
	require.NoError(t, err)
	user := details.UserInfo
	expected := vector.Expected
	assert.Equal(t, expected.Id, user.Id)
	assert.Equal(t, expected.CreatedAt, user.CreatedAt)
	assert.Equal(t, userStatus(t, expected.Status), user.Status)
	assert.Equal(t, expected.Username, user.Username)
	assert.Equal(t, expected.Options, optionViews(t, user.Options, user.DerivedOptions))
	requireNoSecret(t, constants, user.Options, user.DerivedOptions)
	assert.Nil(t, details.Permissions)
}

func updateCommand(t *testing.T, scope string, target, input map[string]json.RawMessage) command.Command {
	t.Helper()
	var reset []string
	if raw, ok := input["reset"]; ok {
		require.NoError(t, json.Unmarshal(raw, &reset))
	}
	switch scope {
	case "topic":
		update := &command.UpdateTopic{
			StreamId: identifier(t, target["stream_id"]),
			TopicId:  identifier(t, target["topic_id"]),
			Name:     optionalText(t, input["name"]),
		}
		if raw, ok := input["message_expiry"]; ok {
			expiry := iggcon.Duration(parseUint64(t, text(t, raw)))
			update.MessageExpiry = &expiry
		}
		if raw, ok := input["max_topic_size"]; ok {
			size := parseUint64(t, text(t, raw))
			update.MaxTopicSize = &size
		}
		for _, key := range reset {
			update.Reset = append(update.Reset, iggcon.TopicOptionKey(key))
		}
		return update
	case "stream":
		update := &command.UpdateStream{
			StreamId: identifier(t, target["stream_id"]),
			Name:     optionalText(t, input["name"]),
		}
		for _, key := range reset {
			update.Reset = append(update.Reset, iggcon.StreamOptionKey(key))
		}
		return update
	case "user":
		update := &command.UpdateUser{
			UserID:   identifier(t, target["user_id"]),
			Username: optionalText(t, input["username"]),
		}
		if raw, ok := input["status"]; ok {
			status := userStatus(t, text(t, raw))
			update.Status = &status
		}
		for _, key := range reset {
			update.Reset = append(update.Reset, iggcon.UserOptionKey(key))
		}
		return update
	default:
		require.FailNow(t, "unknown scope", scope)
		return nil
	}
}

func createStreamCommand(t *testing.T, input map[string]json.RawMessage) *command.CreateStream {
	t.Helper()
	request := &command.CreateStream{Name: text(t, input["name"])}
	if raw, ok := input["encryption_key"]; ok {
		var key struct {
			RawHex string `json:"raw_hex"`
		}
		require.NoError(t, json.Unmarshal(raw, &key))
		request.Options.EncryptionKey = decodeHex(t, key.RawHex)
	}
	if raw, ok := input["raw"]; ok {
		request.Options.Raw = rawEntries(t, raw)
	}
	return request
}

func createTopicCommand(t *testing.T, input map[string]json.RawMessage) *command.CreateTopic {
	t.Helper()
	partitions := uint32(parseUint64(t, string(input["partitions_count"])))
	compression := map[string]iggcon.CompressionAlgorithm{"none": iggcon.CompressionAlgorithmNone, "gzip": iggcon.CompressionAlgorithmGzip}[text(t, input["compression_algorithm"])]
	expiry := iggcon.Duration(parseUint64(t, text(t, input["message_expiry"])))
	maxTopicSize := parseUint64(t, text(t, input["max_topic_size"]))
	segmentSize := parseUint64(t, text(t, input["segment_size"]))
	durability := iggcon.Durability(text(t, input["durability"]))
	consumerOffsetDurability := iggcon.Durability(text(t, input["consumer_offset_durability"]))
	return &command.CreateTopic{
		StreamId: identifier(t, input["stream_id"]),
		Name:     text(t, input["name"]),
		Options: iggcon.TopicOptions{
			PartitionsCount:          &partitions,
			CompressionAlgorithm:     &compression,
			MessageExpiry:            &expiry,
			MaxTopicSize:             &maxTopicSize,
			SegmentSize:              &segmentSize,
			Durability:               &durability,
			ConsumerOffsetDurability: &consumerOffsetDurability,
		},
	}
}

func createUserCommand(t *testing.T, input map[string]json.RawMessage) *command.CreateUser {
	t.Helper()
	request := &command.CreateUser{
		Username: text(t, input["username"]),
		Password: text(t, input["password"]),
		Status:   userStatus(t, text(t, input["status"])),
	}
	if raw, ok := input["raw"]; ok {
		request.Options.Raw = rawEntries(t, raw)
	}
	return request
}

func rawEntries(t *testing.T, raw json.RawMessage) []iggcon.HeaderEntry {
	t.Helper()
	var values map[string]string
	require.NoError(t, json.Unmarshal(raw, &values))
	entries := make([]iggcon.HeaderEntry, 0, len(values))
	for key, value := range values {
		entries = append(entries, iggcon.HeaderEntry{
			Key:   iggcon.HeaderKey{Kind: iggcon.String, Value: []byte(key)},
			Value: iggcon.HeaderValue{Kind: iggcon.String, Value: []byte(value)},
		})
	}
	return entries
}

// optionViews merges the explicit and derived maps into the fixture's HTTP
// JSON shape, so a whole map compares at once.
func optionViews(t *testing.T, explicit, derived map[string]iggcon.HeaderValue) map[string]optionView {
	t.Helper()
	views := make(map[string]optionView, len(explicit)+len(derived))
	for provenance, options := range map[bool]map[string]iggcon.HeaderValue{true: explicit, false: derived} {
		for key, value := range options {
			_, duplicate := views[key]
			require.False(t, duplicate, "%s is both explicit and derived", key)
			views[key] = optionView{Value: valueText(t, value), Kind: kindName(t, value.Kind), Explicit: provenance}
		}
	}
	return views
}

func requireNoSecret(t *testing.T, constants fixtureConstants, maps ...map[string]iggcon.HeaderValue) {
	t.Helper()
	for _, options := range maps {
		for _, key := range constants.NeverReturnedKeys {
			assert.NotContains(t, options, key)
		}
	}
}

func valueText(t *testing.T, value iggcon.HeaderValue) string {
	t.Helper()
	switch value.Kind {
	case iggcon.String:
		return string(value.Value)
	case iggcon.Bool:
		require.Len(t, value.Value, 1)
		return strconv.FormatBool(value.Value[0] == 1)
	case iggcon.Uint32:
		require.Len(t, value.Value, 4)
		return strconv.FormatUint(uint64(binary.LittleEndian.Uint32(value.Value)), 10)
	case iggcon.Uint64:
		require.Len(t, value.Value, 8)
		return strconv.FormatUint(binary.LittleEndian.Uint64(value.Value), 10)
	case iggcon.Raw:
		return base64.StdEncoding.EncodeToString(value.Value)
	default:
		require.FailNow(t, "unexpected option kind", "%d", value.Kind)
		return ""
	}
}

func kindName(t *testing.T, kind iggcon.HeaderKind) string {
	t.Helper()
	names := map[iggcon.HeaderKind]string{iggcon.Raw: "raw", iggcon.String: "string", iggcon.Bool: "bool", iggcon.Uint32: "uint32", iggcon.Uint64: "uint64"}
	name, ok := names[kind]
	require.True(t, ok, "unexpected kind %d", kind)
	return name
}

func headerKind(t *testing.T, name string) iggcon.HeaderKind {
	t.Helper()
	kinds := map[string]iggcon.HeaderKind{"raw": iggcon.Raw, "string": iggcon.String, "bool": iggcon.Bool, "uint32": iggcon.Uint32, "uint64": iggcon.Uint64}
	kind, ok := kinds[name]
	require.True(t, ok, "unexpected kind %s", name)
	return kind
}

func optionsScope(t *testing.T, name string) iggcon.OptionsScope {
	t.Helper()
	scopes := map[string]iggcon.OptionsScope{"topic": iggcon.OptionsScopeTopic, "stream": iggcon.OptionsScopeStream, "user": iggcon.OptionsScopeUser}
	scope, ok := scopes[name]
	require.True(t, ok, "unexpected scope %s", name)
	return scope
}

func userStatus(t *testing.T, name string) iggcon.UserStatus {
	t.Helper()
	statuses := map[string]iggcon.UserStatus{"active": iggcon.Active, "inactive": iggcon.Inactive}
	status, ok := statuses[name]
	require.True(t, ok, "unexpected status %s", name)
	return status
}

// identifier reads a fixture target: a number is a numeric id, a string a
// named one.
func identifier(t *testing.T, raw json.RawMessage) iggcon.Identifier {
	t.Helper()
	var id iggcon.Identifier
	var err error
	if strings.HasPrefix(string(raw), `"`) {
		id, err = iggcon.NewIdentifier(text(t, raw))
	} else {
		id, err = iggcon.NewIdentifier(uint32(parseUint64(t, string(raw))))
	}
	require.NoError(t, err)
	return id
}

func optionalText(t *testing.T, raw json.RawMessage) *string {
	t.Helper()
	if raw == nil {
		return nil
	}
	value := text(t, raw)
	return &value
}

func text(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var value string
	require.NoError(t, json.Unmarshal(raw, &value))
	return value
}

func parseUint64(t *testing.T, value string) uint64 {
	t.Helper()
	parsed, err := strconv.ParseUint(value, 10, 64)
	require.NoError(t, err)
	return parsed
}

func decodeHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	require.NoError(t, err)
	return decoded
}

func requireIggyError(t *testing.T, want fixtureIggyCode, err error) {
	t.Helper()
	require.Error(t, err)
	var iggyError ierror.IggyError
	require.True(t, errors.As(err, &iggyError), "%v is not an IggyError", err)
	assert.Equal(t, want.Code, uint32(iggyError.Code()), "%s, got %v", want.Name, err)
	assert.ErrorIs(t, err, ierror.FromCode(ierror.Code(want.Code)))
}
