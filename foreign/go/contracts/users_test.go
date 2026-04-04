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

package iggcon

import (
	"encoding/binary"
	"testing"
)

// requireRemaining checks that there are enough bytes remaining in the slice
func requireRemaining(t *testing.T, data []byte, position, needed int, context string) {
	t.Helper()
	if position+needed > len(data) {
		t.Fatalf("%s: need %d bytes at position %d, but only %d bytes remain (total length: %d)",
			context, needed, position, len(data)-position, len(data))
	}
}

func TestPermissions_MarshalBinary_WithStreamsAndTopics(t *testing.T) {
	// Test case: Permissions with 2 streams, first stream has 2 topics, second has none
	permissions := &Permissions{
		Global: GlobalPermissions{
			ManageServers: true,
			ReadServers:   false,
			ManageUsers:   false,
			ReadUsers:     true,
			ManageStreams: true,
			ReadStreams:   true,
			ManageTopics:  false,
			ReadTopics:    false,
			PollMessages:  true,
			SendMessages:  false,
		},
		Streams: map[int]*StreamPermissions{
			1: {
				ManageStream: true,
				ReadStream:   true,
				ManageTopics: false,
				ReadTopics:   false,
				PollMessages: true,
				SendMessages: false,
				Topics: map[int]*TopicPermissions{
					10: {
						ManageTopic:  true,
						ReadTopic:    false,
						PollMessages: false,
						SendMessages: true,
					},
					20: {
						ManageTopic:  false,
						ReadTopic:    false,
						PollMessages: true,
						SendMessages: true,
					},
				},
			},
			2: {
				ManageStream: false,
				ReadStream:   true,
				ManageTopics: true,
				ReadTopics:   false,
				PollMessages: false,
				SendMessages: true,
				Topics:       nil,
			},
		},
	}

	bytes, err := permissions.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Verify structure and values
	position := 0

	// Verify global permissions (10 bytes)
	requireRemaining(t, bytes, position, 10, "global permissions")
	expectedGlobal := []byte{1, 0, 0, 1, 1, 1, 0, 0, 1, 0}
	for i := 0; i < 10; i++ {
		if bytes[position+i] != expectedGlobal[i] {
			t.Errorf("Global permission byte %d: expected %d, got %d", i, expectedGlobal[i], bytes[position+i])
		}
	}
	position += 10

	// Has streams flag
	requireRemaining(t, bytes, position, 1, "has_streams flag")
	if bytes[position] != 1 {
		t.Errorf("Expected has_streams=1, got %d", bytes[position])
	}
	position++

	// Track streams found (map because order is non-deterministic)
	streamsFound := make(map[uint32]bool)

	// Read and verify streams
	for position+4 <= len(bytes) {
		// Read stream ID
		requireRemaining(t, bytes, position, 4, "stream ID")
		streamID := binary.LittleEndian.Uint32(bytes[position : position+4])
		position += 4

		// Verify stream permissions based on stream ID
		var expectedStreamPerms []byte
		switch streamID {
		case 1:
			expectedStreamPerms = []byte{1, 1, 0, 0, 1, 0}
		case 2:
			expectedStreamPerms = []byte{0, 1, 1, 0, 0, 1}
		default:
			t.Fatalf("Unexpected stream ID: %d", streamID)
		}

		requireRemaining(t, bytes, position, 6, "stream permissions")
		for i := 0; i < 6; i++ {
			if bytes[position+i] != expectedStreamPerms[i] {
				t.Errorf("Stream %d permission byte %d: expected %d, got %d", streamID, i, expectedStreamPerms[i], bytes[position+i])
			}
		}
		position += 6

		// Read has_topics flag
		requireRemaining(t, bytes, position, 1, "has_topics flag")
		hasTopics := bytes[position]
		position++

		// Verify and read topics if present
		if hasTopics == 1 {
			if streamID != 1 {
				t.Errorf("Stream %d should not have topics", streamID)
			}

			topicsFound := make(map[uint32]bool)
			for {
				requireRemaining(t, bytes, position, 4, "topic ID")

				// Read topic ID
				topicID := binary.LittleEndian.Uint32(bytes[position : position+4])
				position += 4

				// Verify topic permissions
				var expectedTopicPerms []byte
				switch topicID {
				case 10:
					expectedTopicPerms = []byte{1, 0, 0, 1}
				case 20:
					expectedTopicPerms = []byte{0, 0, 1, 1}
				default:
					t.Fatalf("Unexpected topic ID: %d", topicID)
				}

				requireRemaining(t, bytes, position, 4, "topic permissions")
				for i := 0; i < 4; i++ {
					if bytes[position+i] != expectedTopicPerms[i] {
						t.Errorf("Topic %d permission byte %d: expected %d, got %d", topicID, i, expectedTopicPerms[i], bytes[position+i])
					}
				}
				position += 4

				topicsFound[topicID] = true

				// Read has_next_topic flag
				requireRemaining(t, bytes, position, 1, "has_next_topic flag")
				hasNextTopic := bytes[position]
				position++

				// Verify continuation flag logic
				if len(topicsFound) == 1 && hasNextTopic != 1 {
					t.Errorf("Expected has_next_topic=1 for first topic, got %d", hasNextTopic)
				}
				if len(topicsFound) == 2 && hasNextTopic != 0 {
					t.Errorf("Expected has_next_topic=0 for last topic, got %d", hasNextTopic)
				}

				if hasNextTopic == 0 {
					break
				}
			}

			// Verify all topics were found
			if len(topicsFound) != 2 {
				t.Errorf("Expected 2 topics, found %d", len(topicsFound))
			}
			if !topicsFound[10] || !topicsFound[20] {
				t.Errorf("Expected topics 10 and 20, found: %v", topicsFound)
			}
		} else if streamID == 1 {
			t.Errorf("Stream 1 should have topics")
		}

		streamsFound[streamID] = true

		// Read has_next_stream flag
		requireRemaining(t, bytes, position, 1, "has_next_stream flag")
		hasNextStream := bytes[position]
		position++

		// Verify continuation flag logic
		if len(streamsFound) == 1 && hasNextStream != 1 {
			t.Errorf("Expected has_next_stream=1 for first stream, got %d", hasNextStream)
		}
		if len(streamsFound) == 2 && hasNextStream != 0 {
			t.Errorf("Expected has_next_stream=0 for last stream, got %d", hasNextStream)
		}

		if hasNextStream == 0 {
			break
		}
	}

	// Verify all streams were found
	if len(streamsFound) != 2 {
		t.Errorf("Expected 2 streams, found %d", len(streamsFound))
	}
	if !streamsFound[1] || !streamsFound[2] {
		t.Errorf("Expected streams 1 and 2, found: %v", streamsFound)
	}

	// Verify exact payload consumption
	if position != len(bytes) {
		t.Errorf("Payload not fully consumed: read %d bytes, but payload length is %d (unread: %d bytes)",
			position, len(bytes), len(bytes)-position)
	}
}

func TestPermissions_MarshalBinary_GlobalPermissions_IndividualFields(t *testing.T) {
	// Test each global permission field individually to ensure correct byte positions
	tests := []struct {
		name     string
		perms    GlobalPermissions
		expected []byte
	}{
		{
			name:     "Only ManageServers",
			perms:    GlobalPermissions{ManageServers: true},
			expected: []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "Only ReadServers",
			perms:    GlobalPermissions{ReadServers: true},
			expected: []byte{0, 1, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "Only ManageUsers",
			perms:    GlobalPermissions{ManageUsers: true},
			expected: []byte{0, 0, 1, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "Only ReadUsers",
			perms:    GlobalPermissions{ReadUsers: true},
			expected: []byte{0, 0, 0, 1, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "Only ManageStreams",
			perms:    GlobalPermissions{ManageStreams: true},
			expected: []byte{0, 0, 0, 0, 1, 0, 0, 0, 0, 0},
		},
		{
			name:     "Only ReadStreams",
			perms:    GlobalPermissions{ReadStreams: true},
			expected: []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0},
		},
		{
			name:     "Only ManageTopics",
			perms:    GlobalPermissions{ManageTopics: true},
			expected: []byte{0, 0, 0, 0, 0, 0, 1, 0, 0, 0},
		},
		{
			name:     "Only ReadTopics",
			perms:    GlobalPermissions{ReadTopics: true},
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 0},
		},
		{
			name:     "Only PollMessages",
			perms:    GlobalPermissions{PollMessages: true},
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 1, 0},
		},
		{
			name:     "Only SendMessages",
			perms:    GlobalPermissions{SendMessages: true},
			expected: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			permissions := &Permissions{
				Global:  tt.perms,
				Streams: nil,
			}

			bytes, err := permissions.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary failed: %v", err)
			}

			// Verify global permissions bytes
			for i := 0; i < 10; i++ {
				if bytes[i] != tt.expected[i] {
					t.Errorf("Byte %d: expected %d, got %d", i, tt.expected[i], bytes[i])
				}
			}

			// Verify has_streams flag is 0
			if bytes[10] != 0 {
				t.Errorf("Expected has_streams=0, got %d", bytes[10])
			}
		})
	}
}

func TestPermissions_MarshalBinary_StreamPermissions_IndividualFields(t *testing.T) {
	// Test each stream permission field individually
	tests := []struct {
		name     string
		perms    StreamPermissions
		expected []byte
	}{
		{
			name:     "Only ManageStream",
			perms:    StreamPermissions{ManageStream: true},
			expected: []byte{1, 0, 0, 0, 0, 0},
		},
		{
			name:     "Only ReadStream",
			perms:    StreamPermissions{ReadStream: true},
			expected: []byte{0, 1, 0, 0, 0, 0},
		},
		{
			name:     "Only ManageTopics",
			perms:    StreamPermissions{ManageTopics: true},
			expected: []byte{0, 0, 1, 0, 0, 0},
		},
		{
			name:     "Only ReadTopics",
			perms:    StreamPermissions{ReadTopics: true},
			expected: []byte{0, 0, 0, 1, 0, 0},
		},
		{
			name:     "Only PollMessages",
			perms:    StreamPermissions{PollMessages: true},
			expected: []byte{0, 0, 0, 0, 1, 0},
		},
		{
			name:     "Only SendMessages",
			perms:    StreamPermissions{SendMessages: true},
			expected: []byte{0, 0, 0, 0, 0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			permissions := &Permissions{
				Global: GlobalPermissions{},
				Streams: map[int]*StreamPermissions{
					1: &tt.perms,
				},
			}

			bytes, err := permissions.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary failed: %v", err)
			}

			// Skip global (10 bytes) + has_streams (1 byte) + stream ID (4 bytes)
			position := 15

			// Verify stream permissions bytes
			for i := 0; i < 6; i++ {
				if bytes[position+i] != tt.expected[i] {
					t.Errorf("Stream permission byte %d: expected %d, got %d", i, tt.expected[i], bytes[position+i])
				}
			}
		})
	}
}

func TestPermissions_MarshalBinary_TopicPermissions_IndividualFields(t *testing.T) {
	// Test each topic permission field individually
	tests := []struct {
		name     string
		perms    TopicPermissions
		expected []byte
	}{
		{
			name:     "Only ManageTopic",
			perms:    TopicPermissions{ManageTopic: true},
			expected: []byte{1, 0, 0, 0},
		},
		{
			name:     "Only ReadTopic",
			perms:    TopicPermissions{ReadTopic: true},
			expected: []byte{0, 1, 0, 0},
		},
		{
			name:     "Only PollMessages",
			perms:    TopicPermissions{PollMessages: true},
			expected: []byte{0, 0, 1, 0},
		},
		{
			name:     "Only SendMessages",
			perms:    TopicPermissions{SendMessages: true},
			expected: []byte{0, 0, 0, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			permissions := &Permissions{
				Global: GlobalPermissions{},
				Streams: map[int]*StreamPermissions{
					1: {
						Topics: map[int]*TopicPermissions{
							1: &tt.perms,
						},
					},
				},
			}

			bytes, err := permissions.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary failed: %v", err)
			}

			// Skip global (10) + has_streams (1) + stream ID (4) + stream perms (6) + has_topics (1) + topic ID (4)
			position := 26

			// Verify topic permissions bytes
			for i := 0; i < 4; i++ {
				if bytes[position+i] != tt.expected[i] {
					t.Errorf("Topic permission byte %d: expected %d, got %d", i, tt.expected[i], bytes[position+i])
				}
			}
		})
	}
}

func TestPermissions_MarshalBinary_SingleStream_HasNextStreamIsZero(t *testing.T) {
	// Test case: Single stream with no topics - verify has_next_stream=0
	permissions := &Permissions{
		Global: GlobalPermissions{
			ManageServers: true,
			ReadUsers:     true,
		},
		Streams: map[int]*StreamPermissions{
			5: {
				ManageStream: true,
				ReadStream:   true,
				ManageTopics: false,
				ReadTopics:   true,
				PollMessages: false,
				SendMessages: true,
				Topics:       nil,
			},
		},
	}

	bytes, err := permissions.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	position := 0

	// Skip global permissions
	requireRemaining(t, bytes, position, 10, "global permissions")
	position += 10

	// Verify has_streams flag
	requireRemaining(t, bytes, position, 1, "has_streams flag")
	if bytes[position] != 1 {
		t.Errorf("Expected has_streams=1, got %d", bytes[position])
	}
	position++

	// Read and verify stream ID
	requireRemaining(t, bytes, position, 4, "stream ID")
	streamID := binary.LittleEndian.Uint32(bytes[position : position+4])
	if streamID != 5 {
		t.Errorf("Expected stream ID 5, got %d", streamID)
	}
	position += 4

	// Verify stream permissions
	requireRemaining(t, bytes, position, 6, "stream permissions")
	expectedPerms := []byte{1, 1, 0, 1, 0, 1}
	for i := 0; i < 6; i++ {
		if bytes[position+i] != expectedPerms[i] {
			t.Errorf("Stream permission byte %d: expected %d, got %d", i, expectedPerms[i], bytes[position+i])
		}
	}
	position += 6

	// Verify has_topics flag
	requireRemaining(t, bytes, position, 1, "has_topics flag")
	if bytes[position] != 0 {
		t.Errorf("Expected has_topics=0, got %d", bytes[position])
	}
	position++

	// Verify has_next_stream flag (should be 0 for single stream)
	requireRemaining(t, bytes, position, 1, "has_next_stream flag")
	if bytes[position] != 0 {
		t.Errorf("Expected has_next_stream=0 for single stream, got %d", bytes[position])
	}
	position++

	// Verify exact payload consumption
	if position != len(bytes) {
		t.Errorf("Payload not fully consumed: read %d bytes, but payload length is %d", position, len(bytes))
	}
}

func TestPermissions_MarshalBinary_SingleTopic_HasNextTopicIsZero(t *testing.T) {
	// Test case: Single stream with single topic - verify has_next_topic=0 and has_next_stream=0
	permissions := &Permissions{
		Global: GlobalPermissions{
			PollMessages: true,
		},
		Streams: map[int]*StreamPermissions{
			3: {
				ReadStream:   true,
				PollMessages: true,
				Topics: map[int]*TopicPermissions{
					7: {
						ReadTopic:    true,
						PollMessages: true,
					},
				},
			},
		},
	}

	bytes, err := permissions.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	position := 0

	// Skip global permissions
	requireRemaining(t, bytes, position, 10, "global permissions")
	position += 10

	// Verify has_streams flag
	requireRemaining(t, bytes, position, 1, "has_streams flag")
	if bytes[position] != 1 {
		t.Errorf("Expected has_streams=1, got %d", bytes[position])
	}
	position++

	// Read and verify stream ID
	requireRemaining(t, bytes, position, 4, "stream ID")
	streamID := binary.LittleEndian.Uint32(bytes[position : position+4])
	if streamID != 3 {
		t.Errorf("Expected stream ID 3, got %d", streamID)
	}
	position += 4

	// Skip stream permissions
	requireRemaining(t, bytes, position, 6, "stream permissions")
	position += 6

	// Verify has_topics flag
	requireRemaining(t, bytes, position, 1, "has_topics flag")
	if bytes[position] != 1 {
		t.Errorf("Expected has_topics=1, got %d", bytes[position])
	}
	position++

	// Read and verify topic ID
	requireRemaining(t, bytes, position, 4, "topic ID")
	topicID := binary.LittleEndian.Uint32(bytes[position : position+4])
	if topicID != 7 {
		t.Errorf("Expected topic ID 7, got %d", topicID)
	}
	position += 4

	// Verify topic permissions
	requireRemaining(t, bytes, position, 4, "topic permissions")
	expectedPerms := []byte{0, 1, 1, 0}
	for i := 0; i < 4; i++ {
		if bytes[position+i] != expectedPerms[i] {
			t.Errorf("Topic permission byte %d: expected %d, got %d", i, expectedPerms[i], bytes[position+i])
		}
	}
	position += 4

	// Verify has_next_topic flag (should be 0 for single topic)
	requireRemaining(t, bytes, position, 1, "has_next_topic flag")
	if bytes[position] != 0 {
		t.Errorf("Expected has_next_topic=0 for single topic, got %d", bytes[position])
	}
	position++

	// Verify has_next_stream flag (should be 0 for single stream)
	requireRemaining(t, bytes, position, 1, "has_next_stream flag")
	if bytes[position] != 0 {
		t.Errorf("Expected has_next_stream=0 for single stream, got %d", bytes[position])
	}
	position++

	// Verify exact payload consumption
	if position != len(bytes) {
		t.Errorf("Payload not fully consumed: read %d bytes, but payload length is %d", position, len(bytes))
	}
}

func TestPermissions_MarshalBinary_WithEmptyStreamsMap(t *testing.T) {
	// Test case: Permissions with empty streams map should be treated as nil
	permissions := &Permissions{
		Global: GlobalPermissions{
			ManageServers: true,
		},
		Streams: map[int]*StreamPermissions{}, // Empty map
	}

	bytes, err := permissions.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Should be 10 bytes (global) + 1 byte (has_streams=0)
	if len(bytes) != 11 {
		t.Errorf("Expected 11 bytes for empty streams, got %d", len(bytes))
	}

	// Check has_streams flag is 0
	if bytes[10] != 0 {
		t.Errorf("Expected has_streams=0 for empty map, got %d", bytes[10])
	}
}

func TestPermissions_MarshalBinary_WithNilStreams(t *testing.T) {
	// Test case: Permissions with nil streams
	permissions := &Permissions{
		Global: GlobalPermissions{
			ManageServers: true,
		},
		Streams: nil,
	}

	bytes, err := permissions.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Should be 10 bytes (global) + 1 byte (has_streams=0)
	if len(bytes) != 11 {
		t.Errorf("Expected 11 bytes for nil streams, got %d", len(bytes))
	}

	// Check has_streams flag is 0
	if bytes[10] != 0 {
		t.Errorf("Expected has_streams=0 for nil, got %d", bytes[10])
	}
}

func TestPermissions_MarshalBinary_WithEmptyTopicsMap(t *testing.T) {
	// Test case: Stream with empty topics map should be treated as nil
	permissions := &Permissions{
		Global: GlobalPermissions{},
		Streams: map[int]*StreamPermissions{
			1: {
				ManageStream: true,
				ReadStream:   false,
				ManageTopics: true,
				ReadTopics:   false,
				PollMessages: false,
				SendMessages: true,
				Topics:       map[int]*TopicPermissions{}, // Empty topics map
			},
		},
	}

	bytes, err := permissions.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	position := 0

	// Skip global permissions
	requireRemaining(t, bytes, position, 10, "global permissions")
	position += 10

	// Verify has_streams flag
	requireRemaining(t, bytes, position, 1, "has_streams flag")
	if bytes[position] != 1 {
		t.Errorf("Expected has_streams=1, got %d", bytes[position])
	}
	position++

	// Read and verify stream ID
	requireRemaining(t, bytes, position, 4, "stream ID")
	streamID := binary.LittleEndian.Uint32(bytes[position : position+4])
	if streamID != 1 {
		t.Errorf("Expected stream ID 1, got %d", streamID)
	}
	position += 4

	// Verify stream permissions
	requireRemaining(t, bytes, position, 6, "stream permissions")
	expectedPerms := []byte{1, 0, 1, 0, 0, 1}
	for i := 0; i < 6; i++ {
		if bytes[position+i] != expectedPerms[i] {
			t.Errorf("Stream permission byte %d: expected %d, got %d", i, expectedPerms[i], bytes[position+i])
		}
	}
	position += 6

	// Verify has_topics flag is 0 for empty map
	requireRemaining(t, bytes, position, 1, "has_topics flag")
	if bytes[position] != 0 {
		t.Errorf("Expected has_topics=0 for empty topics map, got %d", bytes[position])
	}
	position++

	// Verify has_next_stream flag (should be 0 for single stream)
	requireRemaining(t, bytes, position, 1, "has_next_stream flag")
	if bytes[position] != 0 {
		t.Errorf("Expected has_next_stream=0 for single stream, got %d", bytes[position])
	}
	position++

	// Verify exact payload consumption
	if position != len(bytes) {
		t.Errorf("Payload not fully consumed: read %d bytes, but payload length is %d", position, len(bytes))
	}
}
