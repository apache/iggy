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
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"testing"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/require"
)

// The replies and constants of the deferred polling vectors that the Rust
// encoders generate, shared with every SDK.
type deferredPollingFixture struct {
	Constants struct {
		MinDeferredPollMaxBytes uint32 `json:"min_deferred_poll_max_bytes"`
		MaxPartitionsPerRequest int    `json:"max_partitions_per_request"`
		MaxRecordSize           int    `json:"max_record_size"`
		MaxRecordReplySize      int    `json:"max_record_reply_size"`
	} `json:"constants"`
	FetchReplies []struct {
		Case     string `json:"case"`
		BodyHex  string `json:"body_hex"`
		Sections []struct {
			PartitionId   uint32 `json:"partition_id"`
			Status        uint32 `json:"status"`
			CurrentOffset uint64 `json:"current_offset"`
			MessagesCount uint32 `json:"messages_count"`
			Context       struct {
				Incarnation     uint64 `json:"incarnation"`
				OwnerGeneration uint64 `json:"owner_generation"`
				MetadataOp      uint64 `json:"metadata_op"`
			} `json:"context"`
			NextOffset uint64 `json:"next_offset"`
			RecordsLen int    `json:"records_len"`
			Messages   []struct {
				Offset          uint64 `json:"offset"`
				Id              string `json:"id"`
				Timestamp       uint64 `json:"timestamp"`
				OriginTimestamp uint64 `json:"origin_timestamp"`
				Payload         string `json:"payload"`
			} `json:"messages"`
		} `json:"sections"`
	} `json:"fetch_replies"`
	Refused struct {
		FetchReplies []struct {
			Case    string `json:"case"`
			BodyHex string `json:"body_hex"`
		} `json:"fetch_replies"`
	} `json:"refused"`
}

func loadDeferredPollingFixture(t *testing.T) deferredPollingFixture {
	t.Helper()
	data, err := os.ReadFile("../contracts/testdata/deferred_polling.json")
	require.NoError(t, err)
	var fixture deferredPollingFixture
	require.NoError(t, json.Unmarshal(data, &fixture))
	return fixture
}

func TestFixture_MatchesTheLimitsOfTheServer(t *testing.T) {
	constants := loadDeferredPollingFixture(t).Constants

	require.Equal(t, constants.MinDeferredPollMaxBytes, uint32(iggcon.MinDeferredPollMaxBytes))
	require.Equal(t, constants.MaxPartitionsPerRequest, iggcon.MaxPartitionsPerRequest)
	require.Equal(t, constants.MaxRecordSize, iggcon.MaxRecordSize)
	require.GreaterOrEqual(t, vsr.MaxFrameSize, constants.MaxRecordReplySize,
		"a reply of the largest record fits a frame of the client")
}

func TestFixture_DecodesEachFetchReplyOfTheSharedVectors(t *testing.T) {
	for _, vector := range loadDeferredPollingFixture(t).FetchReplies {
		t.Run(vector.Case, func(t *testing.T) {
			body, err := hex.DecodeString(vector.BodyHex)
			require.NoError(t, err)

			sections, err := DeserializeFetchSections(body, iggcon.MESSAGE_COMPRESSION_NONE)

			require.NoError(t, err)
			require.Len(t, sections, len(vector.Sections))
			for index, want := range vector.Sections {
				section := sections[index]
				require.Equal(t, want.PartitionId, section.PartitionId)
				require.Equal(t, want.CurrentOffset, section.CurrentOffset)
				require.Equal(t, want.MessagesCount, section.MessageCount)
				require.Equal(t, want.NextOffset, section.NextOffset)
				require.Len(t, section.Records, want.RecordsLen)
				require.Equal(t, iggcon.PartitionContext{Incarnation: want.Context.Incarnation,
					OwnerGeneration: want.Context.OwnerGeneration, MetadataOp: want.Context.MetadataOp}, section.Context)
				if want.Status != 0 {
					require.Error(t, section.Err)
					continue
				}
				require.NoError(t, section.Err)
				polled, err := DeserializeSectionMessages(section)
				require.NoError(t, err)
				require.Len(t, polled.Messages, len(want.Messages))
				for at, message := range want.Messages {
					got := polled.Messages[at]
					require.Equal(t, message.Offset, got.Header.Offset)
					require.Equal(t, message.Id, idString(got.Header.Id))
					require.Equal(t, message.Timestamp, got.Header.Timestamp)
					require.Equal(t, message.OriginTimestamp, got.Header.OriginTimestamp)
					require.Equal(t, message.Payload, string(got.Payload))
				}
			}
		})
	}
}

func TestFixture_RefusesEachRefusedFetchReplyOfTheSharedVectors(t *testing.T) {
	for _, vector := range loadDeferredPollingFixture(t).Refused.FetchReplies {
		t.Run(vector.Case, func(t *testing.T) {
			body, err := hex.DecodeString(vector.BodyHex)
			require.NoError(t, err)

			sections, err := DeserializeFetchSections(body, iggcon.MESSAGE_COMPRESSION_NONE)
			if err == nil {
				for _, section := range sections {
					if _, decodeErr := DeserializeSectionMessages(section); decodeErr != nil && section.Err == nil {
						err = decodeErr
					}
				}
			}

			require.Error(t, err)
		})
	}
}

// idString is the decimal value of a little endian 128-bit message id.
func idString(id iggcon.MessageID) string {
	bigEndian := make([]byte, len(id))
	for index, value := range id {
		bigEndian[len(id)-1-index] = value
	}
	return new(big.Int).SetBytes(bigEndian).String()
}
