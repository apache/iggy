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
using Apache.Iggy.Headers;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Typed reads of a response's options. A key lives in one of the two provenance blocks, and an absent key
///     resolves to its catalog default.
/// </summary>
internal static class OptionValues
{
    internal static HeaderValue? Find(Dictionary<HeaderKey, HeaderValue>? explicitOptions,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions, TopicOptionKey key)
    {
        return Find(explicitOptions, derivedOptions, OptionKeys.Describe(key).Name);
    }

    internal static string? Text(Dictionary<HeaderKey, HeaderValue>? explicitOptions,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions, StreamOptionKey key)
    {
        return Find(explicitOptions, derivedOptions, OptionKeys.Describe(key).Name) is { Kind: HeaderKind.String } value
            ? value.ToString()
            : null;
    }

    internal static StreamEncryptionSource? EncryptionKeySource(Dictionary<HeaderKey, HeaderValue>? explicitOptions,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions)
    {
        return Text(explicitOptions, derivedOptions, StreamOptionKey.EncryptionKeySource) switch
        {
            "none" => StreamEncryptionSource.None,
            "stream" => StreamEncryptionSource.Stream,
            "server" => StreamEncryptionSource.Server,
            _ => null
        };
    }

    /// <summary>A u64 setting, <see cref="ulong.MaxValue" /> when absent: never expire and unlimited.</summary>
    internal static ulong UInt64(Dictionary<HeaderKey, HeaderValue>? explicitOptions,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions, TopicOptionKey key)
    {
        return Find(explicitOptions, derivedOptions, key) is { Kind: HeaderKind.Uint64 } value
            ? value.ToUInt64()
            : ulong.MaxValue;
    }

    private static HeaderValue? Find(Dictionary<HeaderKey, HeaderValue>? explicitOptions,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions, string name)
    {
        var key = HeaderKey.FromString(name);
        if (explicitOptions is not null && explicitOptions.TryGetValue(key, out var value))
        {
            return value;
        }

        return derivedOptions is not null && derivedOptions.TryGetValue(key, out value) ? value : null;
    }
}
