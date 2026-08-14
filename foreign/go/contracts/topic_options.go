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

import "encoding/binary"

// Topic option keys that CreateTopic has no parameter of its own for.
//
// The constructors below are the spelling to use for them: create admission
// refuses a key outside the server's catalog by name, and refuses a value whose
// kind is neither the kind the catalog gives that key nor a String that key
// parses. DescribeOptions enumerates the catalog one server serves, with the
// bounds each value is checked against. Every key here is create-time only, so
// UpdateTopic refuses it by name.
const (
	topicOptionSegmentSize                  = "segment_size"
	topicOptionEnforceFsync                 = "enforce_fsync"
	topicOptionMessagesRequiredToSave       = "messages_required_to_save"
	topicOptionSizeOfMessagesRequiredToSave = "size_of_messages_required_to_save"
	topicOptionPreallocateSegments          = "preallocate_segments"
)

// SegmentSizeOption sets how large a partition segment grows before it rotates.
// The server takes a 512-byte multiple inside the bounds its catalog reports.
// Zero is not refused: it leaves the key to the server default.
func SegmentSizeOption(bytes uint64) HeaderEntry {
	return uint64Option(topicOptionSegmentSize, bytes)
}

// EnforceFsyncOption makes writes to the topic's partitions fsync.
func EnforceFsyncOption(enabled bool) HeaderEntry {
	return boolOption(topicOptionEnforceFsync, enabled)
}

// MessagesRequiredToSaveOption flushes the journal once it holds this many
// messages. Zero is refused. Pairs with SizeOfMessagesRequiredToSaveOption:
// whichever threshold trips first flushes.
func MessagesRequiredToSaveOption(messages uint32) HeaderEntry {
	return uint32Option(topicOptionMessagesRequiredToSave, messages)
}

// SizeOfMessagesRequiredToSaveOption flushes the journal once it holds this many
// bytes. A threshold above the largest a segment may be is refused, since it
// could never trip. Zero leaves the key to the server default.
func SizeOfMessagesRequiredToSaveOption(bytes uint64) HeaderEntry {
	return uint64Option(topicOptionSizeOfMessagesRequiredToSave, bytes)
}

// PreallocateSegmentsOption reserves each segment's bytes on disk up front. A
// topic reserves segment_size for every partition it has, and the server refuses
// a create whose product crosses its preallocation cap.
func PreallocateSegmentsOption(enabled bool) HeaderEntry {
	return boolOption(topicOptionPreallocateSegments, enabled)
}

func uint64Option(key string, value uint64) HeaderEntry {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, value)
	return HeaderEntry{
		Key:   HeaderKey{Kind: String, Value: []byte(key)},
		Value: HeaderValue{Kind: Uint64, Value: buf},
	}
}

func uint32Option(key string, value uint32) HeaderEntry {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, value)
	return HeaderEntry{
		Key:   HeaderKey{Kind: String, Value: []byte(key)},
		Value: HeaderValue{Kind: Uint32, Value: buf},
	}
}

func boolOption(key string, value bool) HeaderEntry {
	encoded := byte(0)
	if value {
		encoded = 1
	}
	return HeaderEntry{
		Key:   HeaderKey{Kind: String, Value: []byte(key)},
		Value: HeaderValue{Kind: Bool, Value: []byte{encoded}},
	}
}
