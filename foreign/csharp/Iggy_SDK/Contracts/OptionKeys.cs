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

using System.Text;
using Apache.Iggy.Headers;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Contracts;

/// <summary>Bits of the flags byte a DescribeOptions entry carries.</summary>
[Flags]
internal enum OptionFlags : byte
{
    None = 0,
    Updatable = 1,
    Resettable = 2,
    Secret = 4,
    ReadOnly = 8,
    Critical = 16
}

/// <summary>One client-visible catalog key: its name, canonical kind and mutability.</summary>
internal sealed record OptionDescriptor(string Name, HeaderKind Kind, OptionFlags Flags);

/// <summary>
///     The server's option catalog as a client sees it, keys in catalog order. Typed options take their key names
///     from here, and an update refuses a reset with the error the server would return for it.
/// </summary>
internal static class OptionKeys
{
    internal const int MAX_RESET_KEYS = 255;
    private const int ERROR_KEY_PREVIEW_LENGTH = 64;

    private static readonly OptionDescriptor[] StreamKeys =
    [
        new("name", HeaderKind.String, OptionFlags.Updatable),
        new("encryption_key", HeaderKind.Raw, OptionFlags.Secret | OptionFlags.Critical),
        new("encryption_key_source", HeaderKind.String, OptionFlags.ReadOnly),
        new("encryption_key_fingerprint", HeaderKind.String, OptionFlags.ReadOnly),
        new("encryption_kek_fingerprint", HeaderKind.String, OptionFlags.ReadOnly)
    ];

    private static readonly OptionDescriptor[] TopicKeys =
    [
        new("name", HeaderKind.String, OptionFlags.Updatable),
        new("compression_algorithm", HeaderKind.String, OptionFlags.None),
        new("message_expiry", HeaderKind.Uint64, OptionFlags.Updatable | OptionFlags.Resettable),
        new("max_topic_size", HeaderKind.Uint64, OptionFlags.Updatable | OptionFlags.Resettable),
        new("segment_size", HeaderKind.Uint64, OptionFlags.None),
        new("durability", HeaderKind.String, OptionFlags.None),
        new("consumer_offset_durability", HeaderKind.String, OptionFlags.None),
        new("partition_resize_policy", HeaderKind.String, OptionFlags.None),
        new("messages_required_to_save", HeaderKind.Uint32, OptionFlags.None),
        new("size_of_messages_required_to_save", HeaderKind.Uint64, OptionFlags.None),
        new("preallocate_segments", HeaderKind.Bool, OptionFlags.None)
    ];

    private static readonly OptionDescriptor[] UserKeys =
    [
        new("username", HeaderKind.String, OptionFlags.Updatable),
        new("status", HeaderKind.String, OptionFlags.Updatable)
    ];

    internal static IReadOnlyList<OptionDescriptor> For(OptionsScope scope)
    {
        return scope switch
        {
            OptionsScope.Stream => StreamKeys,
            OptionsScope.Topic => TopicKeys,
            OptionsScope.User => UserKeys,
            _ => throw new ArgumentOutOfRangeException(nameof(scope), scope, "Unknown options scope")
        };
    }

    internal static OptionDescriptor Describe(StreamOptionKey key)
    {
        return Find(StreamKeys, key switch
        {
            StreamOptionKey.Name => "name",
            StreamOptionKey.EncryptionKey => "encryption_key",
            StreamOptionKey.EncryptionKeySource => "encryption_key_source",
            StreamOptionKey.EncryptionKeyFingerprint => "encryption_key_fingerprint",
            StreamOptionKey.EncryptionKekFingerprint => "encryption_kek_fingerprint",
            _ => throw new ArgumentOutOfRangeException(nameof(key), key, "Unknown stream option key")
        });
    }

    internal static OptionDescriptor Describe(TopicOptionKey key)
    {
        return Find(TopicKeys, key switch
        {
            TopicOptionKey.Name => "name",
            TopicOptionKey.CompressionAlgorithm => "compression_algorithm",
            TopicOptionKey.MessageExpiry => "message_expiry",
            TopicOptionKey.MaxTopicSize => "max_topic_size",
            TopicOptionKey.SegmentSize => "segment_size",
            TopicOptionKey.Durability => "durability",
            TopicOptionKey.ConsumerOffsetDurability => "consumer_offset_durability",
            TopicOptionKey.PartitionResizePolicy => "partition_resize_policy",
            TopicOptionKey.MessagesRequiredToSave => "messages_required_to_save",
            TopicOptionKey.SizeOfMessagesRequiredToSave => "size_of_messages_required_to_save",
            TopicOptionKey.PreallocateSegments => "preallocate_segments",
            _ => throw new ArgumentOutOfRangeException(nameof(key), key, "Unknown topic option key")
        });
    }

    internal static OptionDescriptor Describe(UserOptionKey key)
    {
        return Find(UserKeys, key switch
        {
            UserOptionKey.Username => "username",
            UserOptionKey.Status => "status",
            _ => throw new ArgumentOutOfRangeException(nameof(key), key, "Unknown user option key")
        });
    }

    /// <summary>
    ///     A name or username as a header value. Text no header value holds travels as a value of a kind the key
    ///     never accepts, so the refusal comes from admission with the key's own error and in its order.
    /// </summary>
    internal static HeaderValue Text(string text)
    {
        var length = Encoding.UTF8.GetByteCount(text);
        return length is 0 or > WireName.MAX_LENGTH ? HeaderValue.FromBool(false) : HeaderValue.FromString(text);
    }

    /// <summary>Raw entries first, so a typed field the caller inserts afterwards wins on collision.</summary>
    internal static Dictionary<string, HeaderValue> Raw(IReadOnlyDictionary<string, HeaderValue>? options)
    {
        return options is null ? new Dictionary<string, HeaderValue>() : new Dictionary<string, HeaderValue>(options);
    }

    /// <summary>
    ///     Validates an update the way server admission does, in the same order, so a refusal reads the same before
    ///     and after the network. An update naming any raw key or raw reset gets only the structural checks: the
    ///     server's catalog may know keys or flags this build does not, so it decides the rest.
    /// </summary>
    /// <param name="scope">The resource the update targets.</param>
    /// <param name="set">The keys to set.</param>
    /// <param name="reset">The typed keys to reset.</param>
    /// <param name="rawReset">Reset names outside this build's typed keys.</param>
    /// <param name="hasRawSet">Whether <paramref name="set" /> holds a raw key.</param>
    internal static OptionsPatch Patch(OptionsScope scope, Dictionary<string, HeaderValue> set,
        IEnumerable<OptionDescriptor> reset, IEnumerable<string>? rawReset, bool hasRawSet)
    {
        List<OptionDescriptor> resets = reset.ToList();
        List<string> rawResets = rawReset?.ToList() ?? [];
        List<string> names = resets.Select(key => key.Name).Concat(rawResets).Distinct(StringComparer.Ordinal)
            .Order(StringComparer.Ordinal).ToList();
        if (set.Count == 0 && names.Count == 0)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "An update must set or reset at least one option.");
        }

        if (names.FirstOrDefault(set.ContainsKey) is { } both)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, $"Option {Preview(both)} cannot be both set and reset.");
        }

        if (names.FirstOrDefault(name => Encoding.UTF8.GetByteCount(name) is 0 or > WireName.MAX_LENGTH) is { } unfit)
        {
            throw VsrError.Exception(VsrError.UNSUPPORTED_OPTION_KEY, $"Unsupported option key {Preview(unfit)}.");
        }

        if (names.Count > MAX_RESET_KEYS)
        {
            // The catalog is far smaller than the limit, so such a list always names a key outside it.
            var unknown = names.First(name => For(scope).All(key => key.Name != name));
            throw VsrError.Exception(VsrError.UNSUPPORTED_OPTION_KEY, $"Unsupported option key {Preview(unknown)}.");
        }

        if (hasRawSet || rawResets.Count > 0)
        {
            return new OptionsPatch(scope, set, names);
        }

        List<OptionDescriptor> keys = set.Keys.Select(name => Find(For(scope), name)).Concat(resets).ToList();
        if (keys.FirstOrDefault(key => key.Flags.HasFlag(OptionFlags.ReadOnly)) is { } readOnly)
        {
            throw VsrError.Exception(VsrError.READ_ONLY_OPTION, $"Option {readOnly.Name} is read-only.");
        }

        if (keys.FirstOrDefault(key => !key.Flags.HasFlag(OptionFlags.Updatable)) is { } createOnly)
        {
            throw VsrError.Exception(VsrError.OPTION_NOT_UPDATABLE, $"Option {createOnly.Name} is set only at creation.");
        }

        if (resets.FirstOrDefault(key => !key.Flags.HasFlag(OptionFlags.Resettable)) is { } fixedKey)
        {
            throw VsrError.Exception(VsrError.OPTION_NOT_RESETTABLE, $"Option {fixedKey.Name} has no default to reset to.");
        }

        ValidateColumns(scope, set);
        return new OptionsPatch(scope, set, names);
    }

    private static void ValidateColumns(OptionsScope scope, Dictionary<string, HeaderValue> set)
    {
        if (set.TryGetValue("name", out var name) && name.Kind != HeaderKind.String)
        {
            throw scope == OptionsScope.Topic
                ? VsrError.Exception(VsrError.INVALID_TOPIC_NAME, $"Topic name must be 1 to {WireName.MAX_LENGTH} UTF-8 bytes.")
                : VsrError.Exception(VsrError.INVALID_STREAM_NAME, $"Stream name must be 1 to {WireName.MAX_LENGTH} UTF-8 bytes.");
        }

        if (set.TryGetValue("username", out var username))
        {
            if (username.Kind != HeaderKind.String)
            {
                throw VsrError.Exception(VsrError.INVALID_USERNAME, "Username does not fit a header value.");
            }

            CredentialBounds.ValidateUsername(Encoding.UTF8.GetString(username.Value));
        }
    }

    private static OptionDescriptor Find(IReadOnlyList<OptionDescriptor> keys, string name)
    {
        return keys.First(key => key.Name == name);
    }

    private static string Preview(string name)
    {
        return name.Length > ERROR_KEY_PREVIEW_LENGTH ? name[..ERROR_KEY_PREVIEW_LENGTH] : name;
    }
}
