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

import "testing"

func TestPermissions_Size_NilStreams(t *testing.T) {
	p := &Permissions{
		Global:  GlobalPermissions{},
		Streams: nil,
	}
	size := p.Size()
	if size != 11 {
		t.Fatalf("expected size 11 for nil streams, got %d", size)
	}
}

func TestPermissions_Size_WithStreamsAndTopics(t *testing.T) {
	p := &Permissions{
		Global: GlobalPermissions{},
		Streams: map[int]*StreamPermissions{
			1: {
				Topics: map[int]*TopicPermissions{
					10: {},
				},
			},
		},
	}
	// size = 10 (global) + 1 (streams flag) + 4 (stream id) + 6 (stream perms) + 1 (topics flag) + 1 (has topics) + 9 (1 topic)
	expected := 10 + 1 + 4 + 6 + 1 + 1 + 9
	size := p.Size()
	if size != expected {
		t.Fatalf("expected size %d, got %d", expected, size)
	}
}

func TestPermissions_MarshalBinary_NilStreams(t *testing.T) {
	p := &Permissions{
		Global: GlobalPermissions{
			ReadServers:  true,
			SendMessages: true,
		},
		Streams: nil,
	}
	data, err := p.MarshalBinary()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(data) != p.Size() {
		t.Fatalf("expected length %d, got %d", p.Size(), len(data))
	}
	if data[1] != 1 {
		t.Fatalf("expected ReadServers byte to be 1, got %d", data[1])
	}
	if data[9] != 1 {
		t.Fatalf("expected SendMessages byte to be 1, got %d", data[9])
	}
}
