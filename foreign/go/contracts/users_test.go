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

func TestPermissions_MarshalBinary_WithStreamsAndTopics(t *testing.T) {
	// Test case: Permissions with 2 streams, first stream has 2 topics, second has none
	permissions := &Permissions{
		Global: GlobalPermissions{
			ManageServers: true,
			ReadServers:   false,
			ManageUsers:   true,
			ReadUsers:     false,
			ManageStreams: true,
			ReadStreams:   false,
			ManageTopics:  true,
			ReadTopics:    false,
			PollMessages:  true,
			SendMessages:  false,
		},
		Streams: map[int]*StreamPermissions{
			1: {
				ManageStream: true,
				ReadStream:   false,
				ManageTopics: true,
				ReadTopics:   false,
				PollMessages: true,
				SendMessages: false,
				Topics: map[int]*TopicPermissions{
					10: {
						ManageTopic:  true,
						ReadTopic:    false,
						PollMessages: true,
						SendMessages: false,
					},
					20: {
						ManageTopic:  false,
						ReadTopic:    true,
						PollMessages: false,
						SendMessages: true,
					},
				},
			},
			2: {
				ManageStream: false,
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

	// Verify structure and values
	position := 0

	// Verify global permissions (10 bytes)
	expectedGlobal := []byte{1, 0, 1, 0, 1, 0, 1, 0, 1, 0}
	for i := 0; i < 10; i++ {
		if bytes[position+i] != expectedGlobal[i] {
			t.Errorf("Global permission byte %d: expected %d, got %d", i, expectedGlobal[i], bytes[position+i])
		}
	}
	position += 10

	// Has streams flag
	if bytes[position] != 1 {
		t.Errorf("Expected has_streams=1, got %d", bytes[position])
	}
	position++

	// Track streams found (map because order is non-deterministic)
	streamsFound := make(map[uint32]bool)

	// Read and verify streams
	for position+4 <= len(bytes) {
		// Read stream ID
		streamID := binary.LittleEndian.Uint32(bytes[position : position+4])
		position += 4

		// Verify stream permissions based on stream ID
		var expectedStreamPerms []byte
		switch streamID {
		case 1:
			expectedStreamPerms = []byte{1, 0, 1, 0, 1, 0} // ManageStream=true, ReadStream=false, etc.
		case 2:
			expectedStreamPerms = []byte{0, 1, 0, 1, 0, 1}
		default:
			t.Fatalf("Unexpected stream ID: %d", streamID)
		}

		for i := 0; i < 6; i++ {
			if bytes[position+i] != expectedStreamPerms[i] {
				t.Errorf("Stream %d permission byte %d: expected %d, got %d", streamID, i, expectedStreamPerms[i], bytes[position+i])
			}
		}
		position += 6

		// Read has_topics flag
		hasTopics := bytes[position]
		position++

		// Verify and read topics if present
		if hasTopics == 1 {
			if streamID != 1 {
				t.Errorf("Stream %d should not have topics", streamID)
			}

			topicsFound := make(map[uint32]bool)
			for {
				if position+4 > len(bytes) {
					t.Fatalf("Unexpected end while reading topic ID")
				}

				// Read topic ID
				topicID := binary.LittleEndian.Uint32(bytes[position : position+4])
				position += 4

				// Verify topic permissions
				var expectedTopicPerms []byte
				switch topicID {
				case 10:
					expectedTopicPerms = []byte{1, 0, 1, 0}
				case 20:
					expectedTopicPerms = []byte{0, 1, 0, 1}
				default:
					t.Fatalf("Unexpected topic ID: %d", topicID)
				}

				for i := 0; i < 4; i++ {
					if bytes[position+i] != expectedTopicPerms[i] {
						t.Errorf("Topic %d permission byte %d: expected %d, got %d", topicID, i, expectedTopicPerms[i], bytes[position+i])
					}
				}
				position += 4

				topicsFound[topicID] = true

				// Read has_next_topic flag
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
		if position >= len(bytes) {
			t.Fatalf("Unexpected end while reading stream continuation flag")
		}
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
				Topics:       map[int]*TopicPermissions{}, // Empty topics map
			},
		},
	}

	bytes, err := permissions.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Verify structure
	position := 10 // Skip global permissions
	position++     // Skip has_streams flag

	// Skip stream ID and permissions
	position += 4 // stream ID
	position += 6 // stream permissions

	// Check has_topics flag is 0 for empty map
	if bytes[position] != 0 {
		t.Errorf("Expected has_topics=0 for empty topics map, got %d", bytes[position])
	}
}
