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

namespace Apache.Iggy.Enums;

/// <summary>
///     Kind of a metadata change record. The values are the wire codes, and a kind always belongs to one
///     <see cref="MetadataChangeGroup" />.
/// </summary>
public enum MetadataChangeKind : byte
{
    /// <summary>A stream was created.</summary>
    StreamCreated = 1,

    /// <summary>A stream's name or options changed.</summary>
    StreamUpdated = 2,

    /// <summary>A stream was removed.</summary>
    StreamDeleted = 3,

    /// <summary>A topic was created.</summary>
    TopicCreated = 10,

    /// <summary>A topic's name or options changed.</summary>
    TopicUpdated = 11,

    /// <summary>A topic was removed.</summary>
    TopicDeleted = 12,

    /// <summary>A topic's partition count changed.</summary>
    PartitionsChanged = 20,

    /// <summary>A consumer group was created.</summary>
    ConsumerGroupCreated = 30,

    /// <summary>A consumer group was removed.</summary>
    ConsumerGroupDeleted = 31,

    /// <summary>A member joined a consumer group.</summary>
    ConsumerGroupMemberJoined = 40,

    /// <summary>A member left a consumer group.</summary>
    ConsumerGroupMemberLeft = 41,

    /// <summary>A partition moved to another member, or to nobody.</summary>
    ConsumerGroupPartitionHandedOver = 42,

    /// <summary>A user was created.</summary>
    UserCreated = 50,

    /// <summary>A user's name, status or options changed.</summary>
    UserUpdated = 51,

    /// <summary>A user was removed.</summary>
    UserDeleted = 52,

    /// <summary>A user's password was replaced.</summary>
    UserPasswordChanged = 53,

    /// <summary>A user's public permission set changed.</summary>
    UserPermissionsUpdated = 54,

    /// <summary>A personal access token was created.</summary>
    PersonalAccessTokenCreated = 60,

    /// <summary>A personal access token was removed.</summary>
    PersonalAccessTokenDeleted = 61,

    /// <summary>A session was registered.</summary>
    SessionRegistered = 70,

    /// <summary>A session was closed.</summary>
    SessionClosed = 71
}
