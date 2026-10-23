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

namespace Apache.Iggy.Contracts;

/// <summary>
///     The client-visible option keys of a topic, in catalog order. Names a key to reset in a
///     <see cref="TopicUpdate" />: only <see cref="MessageExpiry" /> and <see cref="MaxTopicSize" /> reset to a default.
/// </summary>
public enum TopicOptionKey
{
    /// <summary><c>name</c>, updatable.</summary>
    Name,

    /// <summary><c>compression_algorithm</c>, create-only.</summary>
    CompressionAlgorithm,

    /// <summary><c>message_expiry</c>, updatable and resettable to never expire.</summary>
    MessageExpiry,

    /// <summary><c>max_topic_size</c>, updatable and resettable to unlimited.</summary>
    MaxTopicSize,

    /// <summary><c>segment_size</c>, create-only.</summary>
    SegmentSize,

    /// <summary><c>durability</c>, create-only.</summary>
    Durability,

    /// <summary><c>consumer_offset_durability</c>, create-only.</summary>
    ConsumerOffsetDurability,

    /// <summary><c>partition_resize_policy</c>, create-only.</summary>
    PartitionResizePolicy,

    /// <summary><c>messages_required_to_save</c>, create-only.</summary>
    MessagesRequiredToSave,

    /// <summary><c>size_of_messages_required_to_save</c>, create-only.</summary>
    SizeOfMessagesRequiredToSave,

    /// <summary><c>preallocate_segments</c>, create-only.</summary>
    PreallocateSegments
}
