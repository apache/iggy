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

using Apache.Iggy.Enums;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Details about a client.
/// </summary>
public sealed class ClientResponse
{
    /// <summary>
    ///     Client identifier.
    /// </summary>
    public required uint ClientId { get; init; }

    /// <summary>
    ///     Remote address of the client.
    /// </summary>
    public required string Address { get; init; }

    /// <summary>
    ///     Identifier of the user. This field is optional, as the client might be connected but not authenticated yet.
    /// </summary>
    public required uint? UserId { get; init; }

    /// <summary>
    ///     Transport protocol used by the client.
    /// </summary>
    public required Protocol Transport { get; init; }

    /// <summary>
    ///     Number of consumer groups the client is part of.
    /// </summary>
    public required int ConsumerGroupsCount { get; init; }

    /// <summary>
    ///     List of consumer groups the client is part of.
    /// </summary>
    public IEnumerable<ConsumerGroupInfo> ConsumerGroups { get; init; } = [];
}
