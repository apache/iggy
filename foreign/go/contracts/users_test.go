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

	// Verify structure
	position := 0

	// Global permissions (10 bytes)
	if bytes[position] != 1 {
		t.Errorf("Expected ManageServers=1, got %d", bytes[position])
	}
	position += 10

	// Has streams flag
	if bytes[position] != 1 {
		t.Errorf("Expected has_streams=1, got %d", bytes[position])
	}
	position++

	// Verify continuation flags are present for streams
	// We should have 2 streams, so we need to find stream continuation flags
	streamsFound := 0
	for position < len(bytes) {
		// Each stream has: 4 bytes (ID) + 6 bytes (perms) + 1 byte (has_topics) + topics + 1 byte (has_next_stream)
		if position+4 > len(bytes) {
			break
		}
		streamID := binary.LittleEndian.Uint32(bytes[position : position+4])
		if streamID == 0 {
			break
		}
		position += 4  // stream ID
		position += 6  // stream permissions
		position += 1  // has_topics flag

		// Skip topics if present
		if bytes[position-1] == 1 {
			// Topics exist, need to skip them
			for {
				if position+4 > len(bytes) {
					break
				}
				position += 4  // topic ID
				position += 4  // topic permissions
				if position >= len(bytes) {
					t.Fatalf("Unexpected end of bytes while reading topic continuation flag")
				}
				hasNextTopic := bytes[position]
				position++
				if hasNextTopic == 0 {
					break
				}
			}
		}

		// Check stream continuation flag
		if position >= len(bytes) {
			t.Fatalf("Unexpected end of bytes while reading stream continuation flag")
		}
		hasNextStream := bytes[position]
		position++
		streamsFound++

		if streamsFound == 1 && hasNextStream != 1 {
			t.Errorf("Expected has_next_stream=1 for first stream, got %d", hasNextStream)
		}
		if streamsFound == 2 && hasNextStream != 0 {
			t.Errorf("Expected has_next_stream=0 for second stream, got %d", hasNextStream)
		}

		if hasNextStream == 0 {
			break
		}
	}

	if streamsFound != 2 {
		t.Errorf("Expected 2 streams, found %d", streamsFound)
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
