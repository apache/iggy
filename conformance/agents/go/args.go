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

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// decode reads arguments and refuses a field that the agent does not know,
// so that a new argument shows up as a failure instead of being ignored.
func decode(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

// id is a number for an id or text for a name.
type id struct {
	identifier iggcon.Identifier
}

func (i *id) UnmarshalJSON(raw []byte) error {
	var err error
	var name string
	if json.Unmarshal(raw, &name) == nil {
		i.identifier, err = iggcon.NewIdentifier(name)
		return err
	}
	var number uint32
	if err = json.Unmarshal(raw, &number); err != nil {
		return fmt.Errorf("an identifier is a number or text: %s", raw)
	}
	i.identifier, err = iggcon.NewIdentifier(number)
	return err
}

// keyed reads a one-key object such as {"offset": 3}, or a bare keyword such
// as "first", which has no value.
func keyed(raw []byte) (string, json.RawMessage, error) {
	var keyword string
	if json.Unmarshal(raw, &keyword) == nil {
		return keyword, nil, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || len(object) != 1 {
		return "", nil, fmt.Errorf("expected a keyword or a one-key object: %s", raw)
	}
	for key, value := range object {
		return key, value, nil
	}
	return "", nil, nil
}

type strategy struct {
	value iggcon.PollingStrategy
}

func (s *strategy) UnmarshalJSON(raw []byte) error {
	key, value, err := keyed(raw)
	if err != nil {
		return err
	}
	switch key {
	case "first":
		s.value = iggcon.FirstPollingStrategy()
	case "last":
		s.value = iggcon.LastPollingStrategy()
	case "next":
		s.value = iggcon.NextPollingStrategy()
	case "offset", "timestamp":
		var number uint64
		if err = json.Unmarshal(value, &number); err != nil {
			return err
		}
		if key == "offset" {
			s.value = iggcon.OffsetPollingStrategy(number)
		} else {
			s.value = iggcon.TimestampPollingStrategy(number)
		}
	default:
		return fmt.Errorf("unknown strategy %q", key)
	}
	return nil
}

type partitioning struct {
	value iggcon.Partitioning
}

func (p *partitioning) UnmarshalJSON(raw []byte) error {
	key, value, err := keyed(raw)
	if err != nil {
		return err
	}
	switch key {
	case "balanced":
		p.value = iggcon.None()
	case "partition_id":
		var partition uint32
		if err = json.Unmarshal(value, &partition); err != nil {
			return err
		}
		p.value = iggcon.PartitionId(partition)
	case "messages_key":
		var text string
		if err = json.Unmarshal(value, &text); err != nil {
			return err
		}
		p.value, err = iggcon.EntityIdString(text)
		return err
	default:
		return fmt.Errorf("unknown partitioning %q", key)
	}
	return nil
}

type consumer struct {
	value iggcon.Consumer
}

func (c *consumer) UnmarshalJSON(raw []byte) error {
	key, value, err := keyed(raw)
	if err != nil {
		return err
	}
	var target id
	if err = json.Unmarshal(value, &target); err != nil {
		return err
	}
	switch key {
	case "consumer":
		c.value = iggcon.NewSingleConsumer(target.identifier)
	case "group":
		c.value = iggcon.NewGroupConsumer(target.identifier)
	default:
		return fmt.Errorf("unknown consumer kind %q", key)
	}
	return nil
}

func consumerOrDefault(c *consumer) iggcon.Consumer {
	if c == nil {
		return iggcon.DefaultConsumer()
	}
	return c.value
}

// expiry is never, server_default or microseconds.
type expiry struct {
	value iggcon.Duration
}

func (e *expiry) UnmarshalJSON(raw []byte) error {
	var keyword string
	if json.Unmarshal(raw, &keyword) == nil {
		switch keyword {
		case "never":
			e.value = iggcon.IggyExpiryNeverExpire
		case "server_default":
			e.value = iggcon.IggyExpiryServerDefault
		default:
			return fmt.Errorf("unknown expiry %q", keyword)
		}
		return nil
	}
	var micros uint64
	if err := json.Unmarshal(raw, &micros); err != nil {
		return err
	}
	e.value = iggcon.Duration(micros)
	return nil
}

// topicSize is unlimited, server_default or bytes.
type topicSize struct {
	value uint64
}

func (t *topicSize) UnmarshalJSON(raw []byte) error {
	var keyword string
	if json.Unmarshal(raw, &keyword) == nil {
		switch keyword {
		case "unlimited":
			t.value = math.MaxUint64
		case "server_default":
			t.value = 0
		default:
			return fmt.Errorf("unknown topic size %q", keyword)
		}
		return nil
	}
	return json.Unmarshal(raw, &t.value)
}

func commitPolicy(commit string) (iggcon.CommitPolicy, error) {
	switch commit {
	case "", "manual":
		return iggcon.ManualCommit(), nil
	case "on_read":
		return iggcon.OnReadCommit(), nil
	case "after_processing":
		auto := iggcon.DefaultAutoCommit()
		auto.After = iggcon.CommitAfterProcessing
		return iggcon.AutoCommitPolicy(auto), nil
	default:
		return iggcon.CommitPolicy{}, fmt.Errorf("unknown commit policy %q", commit)
	}
}

// metadataStart is earliest, latest, {after: cursor} or {current: token or null}.
type metadataStart struct {
	value iggcon.MetadataChangeStart
}

func (m *metadataStart) UnmarshalJSON(raw []byte) error {
	key, value, err := keyed(raw)
	if err != nil {
		return err
	}
	switch key {
	case "earliest":
		m.value = iggcon.EarliestMetadataChangeStart()
	case "latest":
		m.value = iggcon.LatestMetadataChangeStart()
	case "after":
		var text string
		if err = json.Unmarshal(value, &text); err != nil {
			return err
		}
		cursor, err := iggcon.ParseMetadataChangeCursor(text)
		if err != nil {
			return err
		}
		m.value = iggcon.AfterMetadataChangeStart(cursor)
	case "current":
		var text *string
		if err = json.Unmarshal(value, &text); err != nil {
			return err
		}
		var token *iggcon.MetadataCurrentPageToken
		if text != nil {
			if token, err = iggcon.ParseMetadataCurrentPageToken(*text); err != nil {
				return err
			}
		}
		m.value = iggcon.CurrentMetadataChangeStart(token)
	default:
		return fmt.Errorf("unknown metadata start %q", key)
	}
	return nil
}

func userStatus(status *string) (iggcon.UserStatus, error) {
	if status == nil || *status == "active" {
		return iggcon.Active, nil
	}
	if *status == "inactive" {
		return iggcon.Inactive, nil
	}
	return iggcon.Active, fmt.Errorf("unknown user status %q", *status)
}

func milliseconds(value *uint64) time.Duration {
	if value == nil {
		return 0
	}
	return time.Duration(*value) * time.Millisecond
}

type headerArg struct {
	Kind  string `json:"kind"`
	Value string `json:"value"`
}

type messageArg struct {
	ID         *string              `json:"id"`
	Payload    *string              `json:"payload"`
	PayloadHex *string              `json:"payload_hex"`
	Headers    map[string]headerArg `json:"headers"`
}

func (m messageArg) message() (iggcon.IggyMessage, error) {
	var payload []byte
	switch {
	case m.Payload != nil && m.PayloadHex == nil:
		payload = []byte(*m.Payload)
	case m.Payload == nil && m.PayloadHex != nil:
		decoded, err := hex.DecodeString(*m.PayloadHex)
		if err != nil {
			return iggcon.IggyMessage{}, err
		}
		payload = decoded
	default:
		return iggcon.IggyMessage{}, fmt.Errorf("a message needs exactly one of payload and payload_hex")
	}
	var options []iggcon.IggyMessageOpt
	if m.ID != nil {
		messageID, err := messageIDFromText(*m.ID)
		if err != nil {
			return iggcon.IggyMessage{}, err
		}
		options = append(options, iggcon.WithID(messageID))
	}
	if len(m.Headers) > 0 {
		entries, err := headerEntries(m.Headers)
		if err != nil {
			return iggcon.IggyMessage{}, err
		}
		options = append(options, iggcon.WithUserHeaders(entries))
	}
	return iggcon.NewIggyMessage(payload, options...)
}
