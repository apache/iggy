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
///     Family of metadata change records and the unit of filtering. The values are the wire codes.
/// </summary>
public enum MetadataChangeGroup : byte
{
    /// <summary>Stream created, updated and deleted.</summary>
    Streams = 1,

    /// <summary>Topic created, updated and deleted.</summary>
    Topics = 2,

    /// <summary>Partition count changes.</summary>
    Partitions = 3,

    /// <summary>Consumer group created and deleted.</summary>
    ConsumerGroups = 4,

    /// <summary>Consumer group members joining, leaving and receiving partitions.</summary>
    ConsumerGroupMembers = 5,

    /// <summary>User created, updated, deleted, password and permission changes.</summary>
    Users = 6,

    /// <summary>Personal access token created and deleted.</summary>
    PersonalAccessTokens = 7,

    /// <summary>Session registered and closed.</summary>
    Sessions = 8
}
