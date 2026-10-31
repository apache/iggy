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
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/stretchr/testify/require"
)

// The deferred polling vectors that the Rust encoders generate, shared with
// every SDK, so the wire here is the server's contract and not this SDK's
// encoder run twice.
type fixtureIdentifier struct {
	Kind  string          `json:"kind"`
	Value json.RawMessage `json:"value"`
}

type fixtureStrategy struct {
	Kind  uint8  `json:"kind"`
	Value uint64 `json:"value"`
}

type fixtureRequest struct {
	Consumer struct {
		Kind uint8             `json:"kind"`
		Id   fixtureIdentifier `json:"id"`
	} `json:"consumer"`
	StreamId         fixtureIdentifier `json:"stream_id"`
	TopicId          fixtureIdentifier `json:"topic_id"`
	PartitionId      uint32            `json:"partition_id"`
	Strategy         fixtureStrategy   `json:"strategy"`
	Count            uint32            `json:"count"`
	AutoCommit       bool              `json:"auto_commit"`
	WaitUs           uint64            `json:"wait_us"`
	MinCount         uint32            `json:"min_count"`
	MaxBytes         uint32            `json:"max_bytes"`
	RequestTimeoutUs uint64            `json:"request_timeout_us"`
	Targets          []struct {
		PartitionId     uint32          `json:"partition_id"`
		Strategy        fixtureStrategy `json:"strategy"`
		MaxBytes        uint32          `json:"max_bytes"`
		Incarnation     uint64          `json:"incarnation"`
		OwnerGeneration uint64          `json:"owner_generation"`
	} `json:"targets"`
}

type fixtureVector struct {
	Case    string         `json:"case"`
	Request fixtureRequest `json:"request"`
	BodyHex string         `json:"body_hex"`
}

func loadRequestVectors(t *testing.T) (polls, fetches []fixtureVector) {
	t.Helper()
	data, err := os.ReadFile("../../contracts/testdata/deferred_polling.json")
	require.NoError(t, err)
	var fixture struct {
		DeferredPolls []fixtureVector `json:"deferred_polls"`
		Fetches       []fixtureVector `json:"fetches"`
	}
	require.NoError(t, json.Unmarshal(data, &fixture))
	require.NotEmpty(t, fixture.DeferredPolls)
	require.NotEmpty(t, fixture.Fetches)
	return fixture.DeferredPolls, fixture.Fetches
}

func (i fixtureIdentifier) identifier(t *testing.T) iggcon.Identifier {
	t.Helper()
	var id iggcon.Identifier
	var err error
	if i.Kind == "numeric" {
		var value uint32
		require.NoError(t, json.Unmarshal(i.Value, &value))
		id, err = iggcon.NewIdentifier(value)
	} else {
		var value string
		require.NoError(t, json.Unmarshal(i.Value, &value))
		id, err = iggcon.NewIdentifier(value)
	}
	require.NoError(t, err)
	return id
}

func (r fixtureRequest) consumer(t *testing.T) iggcon.Consumer {
	id := r.Consumer.Id.identifier(t)
	if r.Consumer.Kind == 2 {
		return iggcon.NewGroupConsumer(id)
	}
	return iggcon.NewSingleConsumer(id)
}

func (r fixtureRequest) options() iggcon.DeferredPollOptions {
	return iggcon.DeferredPollOptions{
		MaxWait:        time.Duration(r.WaitUs) * time.Microsecond,
		MinCount:       r.MinCount,
		MaxBytes:       r.MaxBytes,
		RequestTimeout: time.Duration(r.RequestTimeoutUs) * time.Microsecond,
	}
}

func (s fixtureStrategy) strategy() iggcon.PollingStrategy {
	return iggcon.NewPollingStrategy(iggcon.MessagePolling(s.Kind), s.Value)
}

func TestFixture_EncodesEachDeferredPollOfTheSharedVectors(t *testing.T) {
	polls, _ := loadRequestVectors(t)
	for _, vector := range polls {
		t.Run(vector.Case, func(t *testing.T) {
			request := vector.Request
			partition := request.PartitionId
			options := request.options()
			poll := PollMessages{
				StreamId:    request.StreamId.identifier(t),
				TopicId:     request.TopicId.identifier(t),
				Consumer:    request.consumer(t),
				PartitionId: &partition,
				Strategy:    request.Strategy.strategy(),
				Count:       request.Count,
				AutoCommit:  request.AutoCommit,
				Deferred:    &options,
			}

			body, err := poll.MarshalBinary()

			require.NoError(t, err)
			require.Equal(t, PollMessagesDeferredCode, poll.Code())
			require.Equal(t, vector.BodyHex, hex.EncodeToString(body))
		})
	}
}

func TestFixture_EncodesEachFetchOfTheSharedVectors(t *testing.T) {
	_, fetches := loadRequestVectors(t)
	for _, vector := range fetches {
		t.Run(vector.Case, func(t *testing.T) {
			request := vector.Request
			targets := make([]FetchTarget, 0, len(request.Targets))
			for _, target := range request.Targets {
				fence := iggcon.PartitionContext{Incarnation: target.Incarnation, OwnerGeneration: target.OwnerGeneration}
				targets = append(targets, FetchTarget{
					FetchMessagesTarget: iggcon.FetchMessagesTarget{
						PartitionId: target.PartitionId,
						Strategy:    target.Strategy.strategy().WithContext(fence),
					},
					MaxBytes: target.MaxBytes,
				})
			}
			fetch := FetchMessages{
				StreamId:   request.StreamId.identifier(t),
				TopicId:    request.TopicId.identifier(t),
				Consumer:   request.consumer(t),
				Targets:    targets,
				Count:      request.Count,
				AutoCommit: request.AutoCommit,
				Options:    request.options(),
			}

			body, err := fetch.MarshalBinary()

			require.NoError(t, err)
			require.Equal(t, FetchMessagesCode, fetch.Code())
			require.Equal(t, vector.BodyHex, hex.EncodeToString(body))
		})
	}
}
