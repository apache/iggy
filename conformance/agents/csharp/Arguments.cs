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

// The arguments of the methods, as core/conformance/src/api.rs defines them.

using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Kinds;
using Apache.Iggy.Utils;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.ConformanceAgent;

internal static class Arguments
{
    // Unknown fields are refused, so that a new argument shows up as a failure instead of being ignored.
    private static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
        RespectNullableAnnotations = true,
        RespectRequiredConstructorParameters = true,
        Converters =
        {
            new IdentifierConverter(),
            new StrategyConverter(),
            new PartitioningConverter(),
            new ConsumerConverter(),
            new ExpiryConverter(),
            new TopicSizeConverter(),
            new CommitConverter(),
            new MetadataStartConverter(),
            new JsonStringEnumConverter<OptionsScope>(JsonNamingPolicy.SnakeCaseLower, false),
            new JsonStringEnumConverter<UserStatus>(JsonNamingPolicy.SnakeCaseLower, false),
            new JsonStringEnumConverter<MetadataChangeGroup>(JsonNamingPolicy.SnakeCaseLower, false)
        }
    };

    private static readonly JsonElement NoArguments = JsonSerializer.Deserialize<JsonElement>("{}");

    internal static T Read<T>(JsonElement args)
    {
        var present = args.ValueKind == JsonValueKind.Undefined ? NoArguments : args;
        return present.Deserialize<T>(Json) ?? throw new JsonException("The arguments are null.");
    }

    /// <summary>
    ///     A keyword such as <c>first</c>, which has no value, or a one-key object such as <c>{"offset": 3}</c>.
    /// </summary>
    private static (string Key, JsonElement? Value) Keyed(ref Utf8JsonReader reader)
    {
        using var document = JsonDocument.ParseValue(ref reader);
        var element = document.RootElement;
        if (element.ValueKind == JsonValueKind.String)
        {
            return (element.GetString()!, null);
        }

        if (element.ValueKind == JsonValueKind.Object && element.EnumerateObject().ToList() is [var only])
        {
            return (only.Name, only.Value.Clone());
        }

        throw new JsonException($"Expected a keyword or a one-key object, got {element.GetRawText()}.");
    }

    private static JsonElement Value(JsonElement? value, string key)
    {
        return value ?? throw new JsonException($"{key} needs a value.");
    }

    /// <summary>A number is an id, text is a name.</summary>
    private sealed class IdentifierConverter : ReadOnlyConverter<Identifier>
    {
        public override Identifier Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            return reader.TokenType switch
            {
                JsonTokenType.Number => Identifier.Numeric(reader.GetUInt32()),
                JsonTokenType.String => Identifier.String(reader.GetString()!),
                _ => throw new JsonException("An identifier is a number or text.")
            };
        }
    }

    /// <summary>
    ///     <c>first</c>, <c>last</c>, <c>next</c>, <c>{offset: N}</c> or <c>{timestamp: microseconds}</c>.
    /// </summary>
    private sealed class StrategyConverter : ReadOnlyConverter<PollingStrategy>
    {
        public override PollingStrategy Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
        {
            var (key, value) = Keyed(ref reader);
            return key switch
            {
                "first" when value is null => PollingStrategy.First(),
                "last" when value is null => PollingStrategy.Last(),
                "next" when value is null => PollingStrategy.Next(),
                "offset" => PollingStrategy.Offset(Value(value, key).GetUInt64()),
                "timestamp" => PollingStrategy.Timestamp(Value(value, key).GetUInt64()),
                _ => throw new JsonException($"Unknown strategy {key}.")
            };
        }
    }

    /// <summary><c>balanced</c>, <c>{partition_id: N}</c> or <c>{messages_key: text}</c>.</summary>
    private sealed class PartitioningConverter : ReadOnlyConverter<Partitioning>
    {
        public override Partitioning Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
        {
            var (key, value) = Keyed(ref reader);
            return key switch
            {
                "balanced" when value is null => Partitioning.None(),
                "partition_id" => Partitioning.PartitionId(Value(value, key).GetUInt32()),
                "messages_key" => Partitioning.EntityIdString(Value(value, key).GetString()
                                                              ?? throw new JsonException("A key is text.")),
                _ => throw new JsonException($"Unknown partitioning {key}.")
            };
        }
    }

    /// <summary><c>{consumer: id}</c> or <c>{group: id}</c>.</summary>
    private sealed class ConsumerConverter : ReadOnlyConverter<Consumer>
    {
        public override Consumer Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            var (key, value) = Keyed(ref reader);
            var type = key switch
            {
                "consumer" => ConsumerType.Consumer,
                "group" => ConsumerType.ConsumerGroup,
                _ => throw new JsonException($"Unknown consumer kind {key}.")
            };
            return new Consumer { Type = type, ConsumerId = Value(value, key).Deserialize<Identifier>(options) };
        }
    }

    /// <summary><c>never</c>, <c>server_default</c> or microseconds.</summary>
    private sealed class ExpiryConverter : ReadOnlyConverter<Expiry>
    {
        public override Expiry Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Number)
            {
                return new Expiry(DurationHelpers.FromDuration(reader.GetUInt64()));
            }

            return reader.GetString() switch
            {
                "never" => new Expiry(TimeSpan.MaxValue),
                "server_default" => new Expiry(TimeSpan.Zero),
                var keyword => throw new JsonException($"Unknown expiry {keyword}.")
            };
        }
    }

    /// <summary><c>unlimited</c>, <c>server_default</c> or bytes.</summary>
    private sealed class TopicSizeConverter : ReadOnlyConverter<TopicSize>
    {
        public override TopicSize Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType == JsonTokenType.Number)
            {
                return new TopicSize(reader.GetUInt64());
            }

            return reader.GetString() switch
            {
                "unlimited" => new TopicSize(ulong.MaxValue),
                "server_default" => new TopicSize(0),
                var keyword => throw new JsonException($"Unknown topic size {keyword}.")
            };
        }
    }

    /// <summary><c>manual</c>, <c>on_read</c> or <c>after_processing</c>.</summary>
    private sealed class CommitConverter : ReadOnlyConverter<CommitPolicy>
    {
        public override CommitPolicy Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
        {
            return reader.GetString() switch
            {
                "manual" => CommitPolicy.Manual,
                "on_read" => CommitPolicy.OnRead,
                "after_processing" => CommitPolicy.Auto(AutoCommit.Default with { After = CommitAfter.Processing }),
                var commit => throw new JsonException($"Unknown commit policy {commit}.")
            };
        }
    }

    /// <summary>
    ///     <c>earliest</c>, <c>latest</c>, <c>{after: cursor}</c> or <c>{current: page token or null}</c>.
    /// </summary>
    private sealed class MetadataStartConverter : ReadOnlyConverter<MetadataChangeStart>
    {
        public override MetadataChangeStart Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
        {
            var (key, value) = Keyed(ref reader);
            return key switch
            {
                "earliest" when value is null => MetadataChangeStart.Earliest,
                "latest" when value is null => MetadataChangeStart.Latest,
                "after" => MetadataChangeStart.After(MetadataChangeCursor.Parse(
                    Value(value, key).GetString() ?? throw new JsonException("A cursor is text."))),
                "current" => MetadataChangeStart.Current(Value(value, key).GetString() is { } token
                    ? MetadataCurrentPageToken.Parse(token)
                    : null),
                _ => throw new JsonException($"Unknown metadata start {key}.")
            };
        }
    }

    private abstract class ReadOnlyConverter<T> : JsonConverter<T>
    {
        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        {
            throw new NotSupportedException("Arguments are only read.");
        }
    }
}

/// <summary>
///     A message expiry as the SDK takes it: <see cref="TimeSpan.MaxValue" /> never expires, zero is the server
///     default.
/// </summary>
internal readonly record struct Expiry(TimeSpan Value);

/// <summary>
///     A topic size cap as the SDK takes it: <see cref="ulong.MaxValue" /> is unlimited, zero is the server default.
/// </summary>
internal readonly record struct TopicSize(ulong Value);

internal sealed record NoArgs;

internal sealed record Credentials(string Username, string Password);

internal sealed record CreateClientArgs(string Transport, string Address, Credentials? Login = null);

internal sealed record LoginUserArgs(string Username, string Password);

internal sealed record StreamArgs(Identifier StreamId);

internal sealed record CreateStreamArgs(string Name, string? EncryptionKeyHex = null);

internal sealed record UpdateStreamArgs(Identifier StreamId, string? Name = null, IReadOnlyList<string>? Reset = null);

internal sealed record TopicArgs(Identifier StreamId, Identifier TopicId);

internal sealed record CreateTopicArgs(
    Identifier StreamId,
    string Name,
    uint? PartitionsCount = null,
    Expiry? MessageExpiry = null,
    TopicSize? MaxTopicSize = null);

internal sealed record UpdateTopicArgs(
    Identifier StreamId,
    Identifier TopicId,
    string? Name = null,
    Expiry? MessageExpiry = null,
    TopicSize? MaxTopicSize = null,
    IReadOnlyList<string>? Reset = null);

internal sealed record SendMessagesArgs(
    Identifier StreamId,
    Identifier TopicId,
    Partitioning Partitioning,
    IReadOnlyList<MessageArg> Messages);

/// <summary>A message with its u128 id as decimal text and exactly one of payload and payload_hex.</summary>
internal sealed record MessageArg(
    string? Id = null,
    string? Payload = null,
    string? PayloadHex = null,
    IReadOnlyDictionary<string, HeaderArg>? Headers = null);

internal sealed record HeaderArg(string Kind, string Value);

internal sealed record PollMessagesArgs(
    Identifier StreamId,
    Identifier TopicId,
    uint PartitionId,
    PollingStrategy Strategy,
    uint Count,
    Consumer? Consumer = null,
    CommitPolicy? Commit = null,
    ulong? MaxWaitMs = null,
    uint? MinCount = null);

internal sealed record CreateConsumerGroupArgs(Identifier StreamId, Identifier TopicId, string Name);

internal sealed record ConsumerGroupArgs(Identifier StreamId, Identifier TopicId, Identifier GroupId);

internal sealed record BuildReaderArgs(
    Identifier StreamId,
    Identifier TopicId,
    Consumer? Consumer = null,
    IReadOnlyList<uint>? Partitions = null,
    PollingStrategy? Start = null,
    ulong? MaxWaitMs = null,
    CommitPolicy? Commit = null);

internal sealed record BuildConsumerArgs(
    Identifier StreamId,
    Identifier TopicId,
    string Group,
    ulong? MaxWaitMs = null,
    uint? Count = null,
    CommitPolicy? Commit = null);

internal sealed record BinaryRequestArgs(uint Code, string PayloadHex = "");

internal sealed record BinaryRequestToReplicaArgs(uint Code, byte Replica, string PayloadHex = "");

internal sealed record DescribeOptionsArgs(OptionsScope Scope);

internal sealed record CreateUserArgs(
    string Username,
    string Password,
    UserStatus Status = UserStatus.Active,
    PermissionsArg? Permissions = null);

/// <summary>Global permissions only. Stream permissions are not in format 1.</summary>
internal sealed record PermissionsArg(GlobalPermissionsArg? Global = null);

internal sealed record GlobalPermissionsArg(
    bool ManageServers = false,
    bool ReadServers = false,
    bool ManageUsers = false,
    bool ReadUsers = false,
    bool ManageStreams = false,
    bool ReadStreams = false,
    bool ManageTopics = false,
    bool ReadTopics = false,
    bool PollMessages = false,
    bool SendMessages = false);

internal sealed record UserArgs(Identifier UserId);

internal sealed record UpdateUserArgs(Identifier UserId, string? Username = null, UserStatus? Status = null);

internal sealed record MetadataChangesArgs(
    MetadataChangeStart Start,
    IReadOnlyList<MetadataChangeGroup>? Groups = null,
    uint? Limit = null);

internal sealed record ResolveOperationArgs(ulong OperationId, ulong? DeadlineMs = null);

internal sealed record AbandonOperationArgs(ulong OperationId);
