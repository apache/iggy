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

namespace Apache.Iggy.Contracts.Auth;

/// <summary>
///     Information about a user.
/// </summary>
public sealed class UserResponse
{
    /// <summary>
    ///     User identifier.
    /// </summary>
    public required uint Id { get; init; }

    /// <summary>
    ///     User creation date.
    /// </summary>
    public required ulong CreatedAt { get; init; }

    /// <summary>
    ///     User status.
    /// </summary>
    public required UserStatus Status { get; init; }

    /// <summary>
    ///     User name.
    /// </summary>
    public required string Username { get; init; }

    /// <summary>
    ///     Optional user permissions.
    /// </summary>
    public Permissions? Permissions { get; init; }
}
