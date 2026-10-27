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

using System.Text.Json.Serialization;
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Contracts;

/// <summary>
///     The changes <see cref="IIggyUsers.UpdateUserAsync" /> applies to a user. Every property left null keeps the
///     user's stored value.
/// </summary>
public sealed class UserUpdate
{
    private static readonly HashSet<UserOptionKey> NoReset = [];

    /// <summary>The user's new username, unique within the server.</summary>
    public string? Username { get; init; }

    /// <summary>The user's new status. Deactivating a user ends its sessions and refuses its next login.</summary>
    public UserStatus? Status { get; init; }

    /// <summary>
    ///     Keys to reset to their defaults. No user key has one, so a non-empty set is refused with the error the
    ///     server returns for it, before anything is sent unless the update carries a raw key.
    /// </summary>
    public IReadOnlySet<UserOptionKey>? Reset { get; init; }

    /// <summary>
    ///     Reset names newer than this build's typed keys. The server decides whether each exists and may be reset,
    ///     so an update naming one skips the client-side checks beyond the structural ones.
    /// </summary>
    public IReadOnlySet<string>? RawReset { get; init; }

    /// <summary>Updatable keys newer than this build, keyed by option name. A property above wins on collision.</summary>
    [JsonConverter(typeof(RedactedOptionsConverter))]
    public IReadOnlyDictionary<string, HeaderValue>? Options { get; init; }

    internal OptionsPatch ToPatch()
    {
        var set = OptionKeys.Raw(Options);
        var hasRawSet = set.Count > 0;
        if (Username is not null)
        {
            set[OptionKeys.Describe(UserOptionKey.Username).Name] = OptionKeys.Text(Username);
        }

        if (Status is { } status)
        {
            set[OptionKeys.Describe(UserOptionKey.Status).Name] = HeaderValue.FromString(status switch
            {
                UserStatus.Active => "active",
                UserStatus.Inactive => "inactive",
                _ => throw new ArgumentOutOfRangeException(nameof(Status), status, "Unknown user status")
            });
        }

        return OptionKeys.Patch(OptionsScope.User, set, (Reset ?? NoReset).Select(OptionKeys.Describe),
            RawReset, hasRawSet);
    }
}
