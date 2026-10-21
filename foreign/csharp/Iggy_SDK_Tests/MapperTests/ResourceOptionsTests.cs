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

using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.JsonConverters;
using Apache.Iggy.Tests.ClientTests;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Tests.MapperTests;

/// <summary>
///     The cross-SDK resource options contract. Every expectation comes from the shared fixture the Rust
///     encoders generate, so a byte this SDK writes or reads differently from the server fails here.
/// </summary>
public sealed class ResourceOptionsTests
{
    private const string StreamJson = """
                                      {"id":1,"created_at":1750000000000000,"name":"orders","size":"0 B",
                                       "messages_count":0,"topics_count":0}
                                      """;

    private const string TopicJson = """
                                     {"id":1,"created_at":1750000000000000,"name":"events","size":"0 B",
                                      "messages_count":0,"partitions_count":3,"partitions":[]}
                                     """;

    private const string UserJson = """
                                    {"id":5,"created_at":1750000000000000,"status":"active","username":"alice"}
                                    """;

    private static readonly JsonElement Fixture = LoadFixture();

    private static readonly JsonSerializerOptions HttpJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        Converters = { new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower) }
    };

    private static readonly Dictionary<string, Func<StreamResponse, object?>> StreamReadOnlyAccessors = new()
    {
        ["encryption_key_source"] = stream => stream.EncryptionKeySource,
        ["encryption_key_fingerprint"] = stream => stream.EncryptionKeyFingerprint,
        ["encryption_kek_fingerprint"] = stream => stream.EncryptionKekFingerprints.SingleOrDefault()
    };

    private static readonly byte[] RawKey = Enumerable.Range(1, EncryptionKeys.KEY_LENGTH).Select(value => (byte)value).ToArray();

    private static readonly string[] HttpUpdateCases = ["topic_reset_array", "stream_rename", "user_username_and_status"];

    public static TheoryData<string, string> SharedVectors
    {
        get
        {
            var vectors = new TheoryData<string, string> { { "constants", "constants" } };
            foreach (var scope in new[] { "stream", "topic", "user" })
            {
                vectors.Add("catalog_scope", scope);
                vectors.Add("typed_fields", scope);
            }

            AddCases(vectors, "catalog", "key");
            AddCases(vectors, "describe_options", "scope");
            vectors.Add("stream_key", "fingerprint");
            foreach (var section in new[]
                     {
                         "update_resource", "update_resource_errors", "create_stream", "create_topic", "create_user",
                         "stream_responses", "topic_responses", "user_responses"
                     })
            {
                AddCases(vectors, section, "case");
            }

            foreach (var vectorCase in HttpUpdateCases)
            {
                vectors.Add("http_update", vectorCase);
            }

            return vectors;
        }
    }

    public static TheoryData<string> RawOptionTypes =>
    [
        nameof(StreamOptions), nameof(StreamUpdate), nameof(TopicOptions), nameof(TopicUpdate), nameof(UserOptions),
        nameof(UserUpdate)
    ];

    public static TheoryData<string> InvalidKeys
    {
        get
        {
            var cases = new TheoryData<string>();
            foreach (var entry in Fixture.GetProperty("stream_key").GetProperty("invalid_keys").EnumerateArray())
            {
                cases.Add(entry.GetProperty("case").GetString()!);
            }

            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(SharedVectors))]
    public async Task given_shared_vectors_when_encoding_should_match_server_contract(string section,
        string vectorCase)
    {
        switch (section)
        {
            case "constants":
                AssertConstants(Fixture.GetProperty("constants"));
                break;
            case "catalog_scope":
                AssertCatalogScope(vectorCase);
                break;
            case "typed_fields":
                AssertTypedFields(vectorCase);
                break;
            case "catalog":
                AssertCatalogRow(Find(section, "key", vectorCase));
                break;
            case "describe_options":
                AssertDescribeOptions(Find(section, "scope", vectorCase));
                break;
            case "stream_key":
                AssertStreamKey(Fixture.GetProperty("stream_key"));
                break;
            case "update_resource":
                await AssertUpdateResource(Find(section, "case", vectorCase));
                break;
            case "update_resource_errors":
                await AssertUpdateResourceError(Find(section, "case", vectorCase));
                break;
            case "create_stream":
                await AssertCreateStream(Find(section, "case", vectorCase));
                break;
            case "create_topic":
                await AssertCreateTopic(Find(section, "case", vectorCase));
                break;
            case "create_user":
                await AssertCreateUser(Find(section, "case", vectorCase));
                break;
            case "http_update":
                await AssertHttpUpdate(Find(section, "case", vectorCase));
                break;
            case "stream_responses":
                AssertStreamResponse(Find(section, "case", vectorCase));
                break;
            case "topic_responses":
                AssertTopicResponse(Find(section, "case", vectorCase));
                break;
            case "user_responses":
                AssertUserResponse(Find(section, "case", vectorCase));
                break;
            default:
                Assert.Fail($"Unknown fixture section {section}.");
                break;
        }
    }

    [Theory]
    [MemberData(nameof(InvalidKeys))]
    public async Task given_invalid_key_when_encoding_should_reject_before_network_io(string vectorCase)
    {
        var vector = Fixture.GetProperty("stream_key").GetProperty("invalid_keys").EnumerateArray()
            .Single(entry => entry.GetProperty("case").GetString() == vectorCase);
        var input = vector.GetProperty("input");
        var expectedCode = vector.GetProperty("error").GetProperty("code").GetInt32();

        if (input.TryGetProperty("base64", out var base64))
        {
            var text = base64.GetString()!;
            var parse = Assert.Throws<ArgumentException>(() => EncryptionKeys.FromBase64(text));
            Assert.DoesNotContain(text.Trim(), parse.Message);
            Assert.Throws<ArgumentException>(() => EncryptionKeys.Fingerprint(text));
            return;
        }

        var key = Convert.FromHexString(input.GetProperty("raw_hex").GetString()!);
        Assert.Throws<ArgumentException>(() => EncryptionKeys.Fingerprint(key));
        var options = new StreamOptions { EncryptionKey = key };
        AssertRedacted($"{options}", key);

        var handler = new HttpTopicOptionsTests.StubHandler(StreamJson);
        using var http = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var httpError = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            http.CreateStreamAsync("orders", options, TestContext.Current.CancellationToken));
        Assert.Equal(expectedCode, httpError.StatusCode);
        Assert.False(httpError.FromServer);
        Assert.Equal(string.Empty, handler.RequestPath);
        AssertRedacted(httpError.Message, key);

        // Never connected: an encoder that ran after the network call would surface a connection error instead.
        using var tcp = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:1",
            Protocol = Protocol.Tcp
        }, NullLoggerFactory.Instance);
        var tcpError = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            tcp.CreateStreamAsync("orders", options, TestContext.Current.CancellationToken));
        Assert.Equal(expectedCode, tcpError.StatusCode);
        AssertRedacted(tcpError.Message, key);

        var valid = new byte[EncryptionKeys.KEY_LENGTH];
        Assert.Equal(16, EncryptionKeys.Fingerprint(valid).Length);
        Assert.Equal(valid, new StreamOptions { EncryptionKey = valid }.ToDictionary()["encryption_key"].Value);
    }

    [Fact]
    public void given_raw_set_key_and_typed_reset_when_patching_should_leave_the_verdict_to_the_server()
    {
        var patch = new StreamUpdate
        {
            Options = new Dictionary<string, HeaderValue>
            {
                ["encryption_key_source"] = HeaderValue.FromString("stream")
            },
            Reset = new HashSet<StreamOptionKey> { StreamOptionKey.Name }
        }.ToPatch();

        Assert.Equal(["encryption_key_source"], patch.Set.Keys);
        Assert.Equal(["name"], patch.Reset);
    }

    [Fact]
    public void given_raw_reset_when_patching_should_send_it_for_the_server_to_decide()
    {
        var patch = new TopicUpdate
        {
            Reset = new HashSet<TopicOptionKey> { TopicOptionKey.MessageExpiry },
            RawReset = new HashSet<string> { "retention", "message_expiry" }
        }.ToPatch();

        Assert.Empty(patch.Set);
        Assert.Equal(["message_expiry", "retention"], patch.Reset);
    }

    [Fact]
    public void given_raw_reset_no_wire_name_holds_when_patching_should_refuse_unsupported_key()
    {
        var empty = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            new TopicUpdate { RawReset = new HashSet<string> { string.Empty } }.ToPatch());
        Assert.Equal(VsrError.UNSUPPORTED_OPTION_KEY, empty.StatusCode);

        var tooMany = Assert.Throws<IggyInvalidStatusCodeException>(() => new TopicUpdate
        {
            RawReset = Enumerable.Range(0, OptionKeys.MAX_RESET_KEYS + 1)
                .Select(index => $"key_{index:D3}").ToHashSet()
        }.ToPatch());
        Assert.Equal(VsrError.UNSUPPORTED_OPTION_KEY, tooMany.StatusCode);
        Assert.Contains("key_000", tooMany.Message);
    }

    [Fact]
    public void given_raw_set_key_also_reset_when_patching_should_refuse_invalid_command()
    {
        var both = Assert.Throws<IggyInvalidStatusCodeException>(() => new TopicUpdate
        {
            Options = new Dictionary<string, HeaderValue> { ["retention"] = HeaderValue.FromString("1 day") },
            RawReset = new HashSet<string> { "retention" }
        }.ToPatch());
        Assert.Equal(VsrError.INVALID_COMMAND, both.StatusCode);
    }

    [Fact]
    public void given_unencodable_name_beside_raw_key_when_patching_should_send_a_stand_in()
    {
        var patch = new TopicUpdate
        {
            Name = string.Empty,
            Options = new Dictionary<string, HeaderValue> { ["retention"] = HeaderValue.FromString("1 day") }
        }.ToPatch();

        Assert.Equal(HeaderKind.Bool, patch.Set["name"].Kind);
    }

    [Fact]
    public void given_raw_set_key_named_reset_when_building_http_body_should_refuse_unsupported_key()
    {
        var patch = new TopicUpdate
        {
            Options = new Dictionary<string, HeaderValue> { ["reset"] = HeaderValue.FromString("max_topic_size") }
        }.ToPatch();

        var refused = Assert.Throws<IggyInvalidStatusCodeException>(() => patch.ToHttpBody());
        Assert.Equal(VsrError.UNSUPPORTED_OPTION_KEY, refused.StatusCode);
    }

    [Fact]
    public void given_stream_options_with_key_when_serializing_to_json_should_not_write_the_key()
    {
        var key = Enumerable.Range(1, EncryptionKeys.KEY_LENGTH).Select(value => (byte)value).ToArray();
        var options = new StreamOptions { EncryptionKey = key };

        AssertRedacted(JsonSerializer.Serialize(options), key);
        AssertRedacted(JsonSerializer.Serialize(options, HttpJsonOptions), key);
    }

    [Theory]
    [MemberData(nameof(RawOptionTypes))]
    public void given_raw_secret_entries_when_serializing_to_json_should_write_names_without_values(
        string type)
    {
        var raw = new Dictionary<string, HeaderValue>
        {
            ["encryption_key"] = HeaderValue.FromBytes(RawKey),
            ["future_secret"] = HeaderValue.FromString(Convert.ToBase64String(RawKey))
        };
        object options = type switch
        {
            nameof(StreamOptions) => new StreamOptions { Options = raw },
            nameof(StreamUpdate) => new StreamUpdate { Options = raw },
            nameof(TopicOptions) => new TopicOptions { Options = raw },
            nameof(TopicUpdate) => new TopicUpdate { Options = raw },
            nameof(UserOptions) => new UserOptions { Options = raw },
            nameof(UserUpdate) => new UserUpdate { Options = raw },
            _ => throw new ArgumentOutOfRangeException(nameof(type), type, "Unknown options type")
        };

        foreach (var json in new[]
                 {
                     JsonSerializer.Serialize(options, options.GetType()),
                     JsonSerializer.Serialize(options, options.GetType(), HttpJsonOptions)
                 })
        {
            AssertRedacted(json, RawKey);
            AssertRedacted(json, Encoding.UTF8.GetBytes(Convert.ToBase64String(RawKey)));
            Assert.Contains("\"encryption_key\"", json);
            Assert.Contains("\"future_secret\"", json);
        }
    }

    private static void AssertConstants(JsonElement constants)
    {
        Assert.Equal(VsrError.CANNOT_MODIFY_ROOT_USER,
            constants.GetProperty("error_codes").GetProperty("cannot_modify_root_user").GetInt32());
        var version = constants.GetProperty("protocol_version");
        Assert.Equal(LoginRegister.PROTOCOL_VERSION, version.GetProperty("packed").GetUInt32());
        Assert.Equal(LoginRegister.PROTOCOL_VERSION_MAJOR, version.GetProperty("major").GetInt32());
        Assert.Equal(LoginRegister.PROTOCOL_VERSION_MINOR, version.GetProperty("minor").GetInt32());
        Assert.Equal(LoginRegister.PROTOCOL_VERSION_PATCH, version.GetProperty("patch").GetInt32());

        Dictionary<string, int> codes = typeof(CommandCodes)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(field => field.IsLiteral)
            .ToDictionary(field => field.Name, field => (int)field.GetRawConstantValue()!);
        foreach (var command in constants.GetProperty("command_codes").EnumerateObject())
        {
            var code = command.Value.GetProperty("code").GetInt32();
            Assert.Equal(code, codes[command.Name.ToUpperInvariant() + "_CODE"]);
            var operation = command.Value.GetProperty("operation");
            var expected = operation.ValueKind == JsonValueKind.Null
                ? VsrOperation.NonReplicated
                : (VsrOperation)operation.GetByte();
            Assert.Equal(expected, VsrOperations.ForCode(code));
        }

        foreach (var retired in constants.GetProperty("retired_command_codes").EnumerateObject())
        {
            var code = retired.Value.GetInt32();
            Assert.DoesNotContain(code, codes.Values);
            Assert.Equal(VsrOperation.NonReplicated, VsrOperations.ForCode(code));
        }

        foreach (var operation in constants.GetProperty("operations").EnumerateObject())
        {
            var value = operation.Value.GetProperty("value").GetByte();
            Assert.True(VsrOperations.IsKnown(value), $"{operation.Name} must be a known operation");
            Assert.Equal(operation.Value.GetProperty("internal").GetBoolean(), ((VsrOperation)value).IsInternal());
            Assert.Equal((VsrOperation)operation.Value.GetProperty("request_operation").GetByte(),
                ((VsrOperation)value).RequestOperation());
        }

        foreach (var retired in constants.GetProperty("retired_operations").EnumerateObject())
        {
            Assert.False(VsrOperations.IsKnown(retired.Value.GetByte()), $"{retired.Name} must stay retired");
        }

        foreach (var scope in constants.GetProperty("scope_codes").EnumerateObject())
        {
            Assert.Equal(scope.Value.GetByte(), (byte)Enum.Parse<OptionsScope>(scope.Name, true));
        }

        foreach (var kind in constants.GetProperty("header_kind_codes").EnumerateObject())
        {
            Assert.Equal(UserHeadersConverter.ParseHeaderKind(kind.Name), Mappers.BinaryMapper.MapHeaderKind(kind.Value.GetByte()));
        }

        var flags = constants.GetProperty("option_flags");
        Assert.Equal((byte)OptionFlags.Updatable, flags.GetProperty("updatable").GetByte());
        Assert.Equal((byte)OptionFlags.Resettable, flags.GetProperty("resettable").GetByte());
        Assert.Equal((byte)OptionFlags.Secret, flags.GetProperty("secret").GetByte());
        Assert.Equal((byte)OptionFlags.ReadOnly, flags.GetProperty("read_only").GetByte());
        Assert.Equal((byte)OptionFlags.Critical, flags.GetProperty("critical").GetByte());

        foreach (var status in constants.GetProperty("user_status_codes").EnumerateObject())
        {
            Assert.Equal(status.Value.GetInt32(), (int)Enum.Parse<UserStatus>(status.Name, true));
        }

        Assert.Equal(EncryptionKeys.KEY_LENGTH, constants.GetProperty("stream_key_length").GetInt32());
    }

    private static void AssertCatalogScope(string scopeName)
    {
        var scope = Enum.Parse<OptionsScope>(scopeName, true);
        var expected = Fixture.GetProperty("catalog").EnumerateArray()
            .Where(row => row.GetProperty("described").GetBoolean() && InScope(row, scopeName))
            .Select(row => row.GetProperty("key").GetString()!)
            .ToList();

        Assert.Equal(expected, OptionKeys.For(scope).Select(descriptor => descriptor.Name).ToList());
    }

    private static void AssertTypedFields(string scopeName)
    {
        List<JsonElement> rows = Fixture.GetProperty("catalog").EnumerateArray()
            .Where(row => InScope(row, scopeName) && row.GetProperty("described").GetBoolean())
            .ToList();
        var createKeys = rows
            .Where(row => row.GetProperty("storage").GetString() == "map"
                          && row.GetProperty("mutability").GetString() is "create_only" or "updatable")
            .Select(row => row.GetProperty("key").GetString()!)
            .ToHashSet();
        var updateKeys = rows
            .Where(row => row.GetProperty("mutability").GetString() == "updatable")
            .Select(row => row.GetProperty("key").GetString()!)
            .ToHashSet();

        var resettableKeys = rows
            .Where(row => (row.GetProperty("flags").GetByte() & (byte)OptionFlags.Resettable) != 0)
            .Select(row => row.GetProperty("key").GetString()!)
            .ToHashSet();
        var readOnlyKeys = rows
            .Where(row => row.GetProperty("mutability").GetString() == "read_only")
            .Select(row => row.GetProperty("key").GetString()!)
            .ToHashSet();
        var describedKeys = rows.Select(row => row.GetProperty("key").GetString()!).ToHashSet();

        switch (scopeName)
        {
            case "stream":
                AssertKeyMembers<StreamOptionKey>(describedKeys, resettableKeys, OptionKeys.Describe,
                    reset => new StreamUpdate { Reset = reset }.ToPatch());
                Assert.Equal(readOnlyKeys, StreamReadOnlyAccessors.Keys.ToHashSet());
                foreach (var (key, accessor) in StreamReadOnlyAccessors)
                {
                    var response = new StreamResponse
                    {
                        Id = 1,
                        Name = "orders",
                        Size = 0,
                        CreatedAt = DateTimeOffset.UnixEpoch,
                        MessagesCount = 0,
                        TopicsCount = 0,
                        DerivedOptions = new Dictionary<HeaderKey, HeaderValue>
                        {
                            [HeaderKey.FromString(key)] = HeaderValue.FromString("stream")
                        }
                    };
                    Assert.NotNull(accessor(response));
                }

                Assert.Equal(createKeys,
                    new StreamOptions { EncryptionKey = new byte[EncryptionKeys.KEY_LENGTH] }.ToDictionary().Keys
                        .ToHashSet());
                Assert.Equal(updateKeys, new StreamUpdate { Name = "renamed" }.ToPatch().Set.Keys.ToHashSet());
                break;
            case "topic":
                AssertKeyMembers<TopicOptionKey>(describedKeys, resettableKeys, OptionKeys.Describe,
                    reset => new TopicUpdate { Reset = reset }.ToPatch());
                Assert.Empty(readOnlyKeys);
                Assert.Equal(createKeys, new TopicOptions
                {
                    CompressionAlgorithm = CompressionAlgorithm.Gzip,
                    MessageExpiry = TimeSpan.FromDays(7),
                    MaxTopicSize = 1_073_741_824,
                    SegmentSize = 134_217_728,
                    Durability = Durability.Persisted,
                    ConsumerOffsetDurability = Durability.Persisted,
                    PartitionResizePolicy = PartitionResizePolicy.Fixed,
                    MessagesRequiredToSave = 64,
                    SizeOfMessagesRequiredToSave = 4096,
                    PreallocateSegments = true
                }.ToDictionary().Keys.ToHashSet());
                Assert.Equal(updateKeys, new TopicUpdate
                {
                    Name = "renamed",
                    MessageExpiry = TimeSpan.FromDays(1),
                    MaxTopicSize = 1_073_741_824
                }.ToPatch().Set.Keys.ToHashSet());
                break;
            case "user":
                AssertKeyMembers<UserOptionKey>(describedKeys, resettableKeys, OptionKeys.Describe,
                    reset => new UserUpdate { Reset = reset }.ToPatch());
                Assert.Empty(readOnlyKeys);
                Assert.Equal(createKeys, new UserOptions().ToDictionary().Keys.ToHashSet());
                Assert.Equal(updateKeys,
                    new UserUpdate { Username = "renamed", Status = UserStatus.Inactive }.ToPatch().Set.Keys
                        .ToHashSet());
                break;
        }
    }

    /// <summary>
    ///     Pins every key member to its catalog name by the member's own name, so reordering either list cannot move
    ///     a member onto another key, and checks there is a member per described key and a reset per resettable one.
    /// </summary>
    private static void AssertKeyMembers<TKey>(HashSet<string> describedKeys, HashSet<string> resettableKeys,
        Func<TKey, OptionDescriptor> describe, Func<IReadOnlySet<TKey>, OptionsPatch> reset) where TKey : struct, Enum
    {
        foreach (var member in Enum.GetValues<TKey>())
        {
            Assert.Equal(JsonNamingPolicy.SnakeCaseLower.ConvertName(member.ToString()), describe(member).Name);
        }

        Assert.Equal(describedKeys, Enum.GetValues<TKey>().Select(member => describe(member).Name).ToHashSet());
        var resettable = Enum.GetValues<TKey>().Where(member => resettableKeys.Contains(describe(member).Name))
            .ToHashSet();
        if (resettable.Count > 0)
        {
            Assert.Equal(resettableKeys, reset(resettable).Reset.ToHashSet());
        }
    }

    private static void AssertCatalogRow(JsonElement row)
    {
        var key = row.GetProperty("key").GetString();
        var described = row.GetProperty("described").GetBoolean();
        foreach (var scope in Enum.GetValues<OptionsScope>())
        {
            var descriptor = OptionKeys.For(scope).SingleOrDefault(entry => entry.Name == key);
            if (!described || !InScope(row, scope.ToString().ToLowerInvariant()))
            {
                Assert.Null(descriptor);
                continue;
            }

            Assert.NotNull(descriptor);
            Assert.Equal(Mappers.BinaryMapper.MapHeaderKind(row.GetProperty("kind_code").GetByte()), descriptor.Kind);
            Assert.Equal(row.GetProperty("flags").GetByte(), (byte)descriptor.Flags);
        }
    }

    private static void AssertDescribeOptions(JsonElement vector)
    {
        var scope = Enum.Parse<OptionsScope>(vector.GetProperty("scope").GetString()!, true);
        Assert.Equal(Hex(vector, "request_hex"), new[] { (byte)scope });

        IReadOnlyList<OptionSpec> specs = Mappers.BinaryMapper.MapOptionSpecs(Hex(vector, "response_hex"));
        List<JsonElement> entries = vector.GetProperty("entries").EnumerateArray().ToList();
        Assert.Equal(entries.Count, specs.Count);
        for (var index = 0; index < entries.Count; index++)
        {
            var entry = entries[index];
            var spec = specs[index];
            Assert.Equal(entry.GetProperty("key").GetString(), spec.Key);
            Assert.Equal(UserHeadersConverter.ParseHeaderKind(entry.GetProperty("kind").GetString()), spec.Kind);
            Assert.Equal(entry.GetProperty("flags").GetByte(), Flags(spec));
            var defaultValue = entry.GetProperty("default");
            if (defaultValue.ValueKind == JsonValueKind.Null)
            {
                Assert.Empty(spec.DefaultValue);
            }
            else
            {
                Assert.Equal(defaultValue.GetString(),
                    ResourceOptionsConverter.EncodeValue(new HeaderValue { Kind = spec.Kind, Value = spec.DefaultValue }));
            }

            Assert.False(string.IsNullOrWhiteSpace(spec.Description));
        }
    }

    private static void AssertStreamKey(JsonElement streamKey)
    {
        var key = Convert.FromHexString(streamKey.GetProperty("key_hex").GetString()!);
        var base64 = streamKey.GetProperty("key_base64").GetString()!;
        var fingerprint = streamKey.GetProperty("fingerprint").GetString();

        Assert.Equal(fingerprint, EncryptionKeys.Fingerprint(key));
        Assert.Equal(fingerprint, EncryptionKeys.Fingerprint(base64));
        Assert.Equal(key, EncryptionKeys.FromBase64(base64));

        var value = new StreamOptions { EncryptionKey = key }.ToDictionary()["encryption_key"];
        Assert.Equal(HeaderKind.Raw, value.Kind);
        Assert.Equal(key, value.Value);
    }

    private static async Task AssertUpdateResource(JsonElement vector)
    {
        var scope = vector.GetProperty("scope").GetString()!;
        var target = vector.GetProperty("target");
        var input = vector.GetProperty("input");

        var (wire, path, send) = UpdateRequest(scope, target, input);
        Assert.Equal(Hex(vector, "body_hex"), wire);

        var handler = new HttpTopicOptionsTests.StubHandler(string.Empty);
        using var http = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        await send(http);
        Assert.Equal(path, handler.RequestPath);
        AssertUpdateBody(JsonDocument.Parse(handler.RequestBody).RootElement, vector.GetProperty("wire_set"),
            vector.GetProperty("wire_reset"));
    }

    private static async Task AssertUpdateResourceError(JsonElement vector)
    {
        var scope = vector.GetProperty("scope").GetString()!;
        var expectedCode = vector.GetProperty("error").GetProperty("code").GetInt32();
        if (HasRawReset(scope, vector.GetProperty("input")))
        {
            // A reset this build has no typed key for is the server's to judge, so the request goes out.
            var (_, path, sendRaw) = UpdateRequest(scope, vector.GetProperty("target"), vector.GetProperty("input"));
            var stub = new HttpTopicOptionsTests.StubHandler(string.Empty);
            using var rawHttp = new HttpMessageStream(new HttpClient(stub) { BaseAddress = new Uri("http://localhost") });
            await sendRaw(rawHttp);
            Assert.Equal(path, stub.RequestPath);
            return;
        }

        var encode = Assert.Throws<IggyInvalidStatusCodeException>(() =>
            UpdateRequest(scope, vector.GetProperty("target"), vector.GetProperty("input")));
        Assert.Equal(expectedCode, encode.StatusCode);
        Assert.False(encode.FromServer);

        var handler = new HttpTopicOptionsTests.StubHandler(string.Empty);
        using var http = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        var send = UpdateCall(scope, vector.GetProperty("target"), vector.GetProperty("input"));
        var refused = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => send(http));
        Assert.Equal(expectedCode, refused.StatusCode);
        Assert.Equal(string.Empty, handler.RequestPath);

        using var tcp = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = "127.0.0.1:1",
            Protocol = Protocol.Tcp
        }, NullLoggerFactory.Instance);
        var tcpRefused = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => send(tcp));
        Assert.Equal(expectedCode, tcpRefused.StatusCode);
    }

    private static async Task AssertCreateStream(JsonElement vector)
    {
        var input = vector.GetProperty("input");
        var name = input.GetProperty("name").GetString()!;
        var options = new StreamOptions
        {
            EncryptionKey = input.TryGetProperty("encryption_key", out var key)
                ? Convert.FromHexString(key.GetProperty("raw_hex").GetString()!)
                : null,
            Options = RawOptions(input)
        };

        Assert.Equal(Hex(vector, "body_hex"), TcpContracts.CreateStream(name, options.ToDictionary()));

        var handler = new HttpTopicOptionsTests.StubHandler(StreamJson);
        using var http = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        await http.CreateStreamAsync(name, options, TestContext.Current.CancellationToken);
        Assert.Equal("/streams", handler.RequestPath);
        var body = JsonDocument.Parse(handler.RequestBody).RootElement;
        Assert.Equal(name, body.GetProperty("name").GetString());
        AssertCreateOptionsBody(body, vector.GetProperty("wire_options"));
        AssertRedacted($"{options}", options.EncryptionKey ?? []);
    }

    private static async Task AssertCreateTopic(JsonElement vector)
    {
        var input = vector.GetProperty("input");
        var streamId = TargetIdentifier(input.GetProperty("stream_id"));
        var name = input.GetProperty("name").GetString()!;
        var partitionsCount = input.GetProperty("partitions_count").GetUInt32();
        var options = new TopicOptions
        {
            CompressionAlgorithm = Enum.Parse<CompressionAlgorithm>(input.GetProperty("compression_algorithm").GetString()!, true),
            MessageExpiry = DurationHelpers.FromDuration(ParseU64(input, "message_expiry")),
            MaxTopicSize = ParseU64(input, "max_topic_size"),
            SegmentSize = ParseU64(input, "segment_size"),
            Durability = Enum.Parse<Durability>(input.GetProperty("durability").GetString()!, true),
            ConsumerOffsetDurability =
                Enum.Parse<Durability>(input.GetProperty("consumer_offset_durability").GetString()!, true)
        };

        Assert.Equal(Hex(vector, "body_hex"),
            TcpContracts.CreateTopic(streamId, name, partitionsCount, options.ToDictionary()));

        var handler = new HttpTopicOptionsTests.StubHandler(TopicJson);
        using var http = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        await http.CreateTopicAsync(streamId, name, partitionsCount, options, TestContext.Current.CancellationToken);
        Assert.Equal($"/streams/{streamId}/topics", handler.RequestPath);
        var body = JsonDocument.Parse(handler.RequestBody).RootElement;
        Assert.Equal(name, body.GetProperty("name").GetString());
        Assert.Equal(partitionsCount, body.GetProperty("partitions_count").GetUInt32());
        AssertCreateOptionsBody(body, vector.GetProperty("wire_options"));
    }

    private static async Task AssertCreateUser(JsonElement vector)
    {
        var input = vector.GetProperty("input");
        var username = input.GetProperty("username").GetString()!;
        var password = input.GetProperty("password").GetString()!;
        var status = Enum.Parse<UserStatus>(input.GetProperty("status").GetString()!, true);
        var options = new UserOptions { Options = RawOptions(input) };

        Assert.Equal(Hex(vector, "body_hex"),
            TcpContracts.CreateUser(username, password, status, null, options.ToDictionary()));

        var handler = new HttpTopicOptionsTests.StubHandler(UserJson);
        using var http = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        await http.CreateUserAsync(username, password, status, options: options,
            token: TestContext.Current.CancellationToken);
        Assert.Equal("/users", handler.RequestPath);
        var body = JsonDocument.Parse(handler.RequestBody).RootElement;
        Assert.Equal(username, body.GetProperty("username").GetString());
        Assert.Equal(input.GetProperty("status").GetString(), body.GetProperty("status").GetString());
        AssertCreateOptionsBody(body, vector.GetProperty("wire_options"));
    }

    private static async Task AssertHttpUpdate(JsonElement vector)
    {
        var scope = vector.GetProperty("scope").GetString()!;
        var expectedBody = JsonDocument.Parse(vector.GetProperty("body").GetString()!).RootElement;
        var input = TypedInput(expectedBody);

        var (wire, path, send) = UpdateRequest(scope, vector.GetProperty("target"), input);
        Assert.Equal(Hex(vector, "update_resource_hex"), wire);

        var handler = new HttpTopicOptionsTests.StubHandler(string.Empty);
        using var http = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
        await send(http);
        Assert.Equal(path, handler.RequestPath);
        var actual = JsonDocument.Parse(handler.RequestBody).RootElement;
        Assert.Equal(Members(expectedBody), Members(actual));
    }

    private static void AssertStreamResponse(JsonElement vector)
    {
        var expected = vector.GetProperty("expected");
        var binary = Mappers.BinaryMapper.MapStream(Hex(vector, "response_hex"));
        var rest = JsonSerializer.Deserialize<StreamResponse>(RestJson(expected, "topics_count"), HttpJsonOptions)!;

        foreach (var stream in new[] { binary, rest })
        {
            Assert.Equal(expected.GetProperty("id").GetUInt32(), stream.Id);
            Assert.Equal(expected.GetProperty("name").GetString(), stream.Name);
            Assert.Equal(expected.GetProperty("topics_count").GetUInt32(), stream.TopicsCount);
            Assert.Equal(expected.GetProperty("size_bytes").GetUInt64(), stream.Size);
            Assert.Equal(expected.GetProperty("messages_count").GetUInt64(), stream.MessagesCount);
            Assert.Equal(Projection(expected.GetProperty("options")), Projection(stream.Options, stream.DerivedOptions));
            Assert.Equal(Enum.Parse<StreamEncryptionSource>(expected.GetProperty("encryption_key_source").GetString()!, true),
                stream.EncryptionKeySource);
            var fingerprint = expected.GetProperty("encryption_key_fingerprint");
            Assert.Equal(fingerprint.ValueKind == JsonValueKind.Null ? null : fingerprint.GetString(),
                stream.EncryptionKeyFingerprint);
            Assert.Equal(expected.GetProperty("encryption_kek_fingerprints").EnumerateArray()
                .Select(entry => entry.GetString()!).ToList(), stream.EncryptionKekFingerprints);
            AssertNeverReturned(stream.Options, stream.DerivedOptions);
        }

        Assert.Equal(MicrosSinceEpoch(binary.CreatedAt), expected.GetProperty("created_at").GetUInt64());
    }

    private static void AssertTopicResponse(JsonElement vector)
    {
        var expected = vector.GetProperty("expected");
        var binary = Mappers.BinaryMapper.MapTopic(Hex(vector, "response_hex"));
        var rest = JsonSerializer.Deserialize<TopicResponse>(RestJson(expected, "partitions_count"), HttpJsonOptions)!;

        foreach (var topic in new[] { binary, rest })
        {
            Assert.Equal(expected.GetProperty("id").GetUInt32(), topic.Id);
            Assert.Equal(expected.GetProperty("name").GetString(), topic.Name);
            Assert.Equal(expected.GetProperty("partitions_count").GetUInt32(), topic.PartitionsCount);
            Assert.Equal(expected.GetProperty("size_bytes").GetUInt64(), topic.Size);
            Assert.Equal(expected.GetProperty("messages_count").GetUInt64(), topic.MessagesCount);
            Assert.Equal(Projection(expected.GetProperty("options")), Projection(topic.Options, topic.DerivedOptions));
            Assert.Equal(Enum.Parse<CompressionAlgorithm>(expected.GetProperty("compression_algorithm").GetString()!, true),
                topic.CompressionAlgorithm);
            Assert.Equal(ParseU64(expected, "message_expiry"), DurationHelpers.ToDuration(topic.MessageExpiry));
            Assert.Equal(ParseU64(expected, "max_topic_size"), topic.MaxTopicSize);
            AssertNeverReturned(topic.Options, topic.DerivedOptions);
        }

        Assert.Equal(MicrosSinceEpoch(binary.CreatedAt), expected.GetProperty("created_at").GetUInt64());
    }

    private static void AssertUserResponse(JsonElement vector)
    {
        var expected = vector.GetProperty("expected");
        var binary = Mappers.BinaryMapper.MapUser(Hex(vector, "response_hex"));
        var rest = JsonSerializer.Deserialize<UserResponse>(
            $$"""
              {"id":{{expected.GetProperty("id").GetUInt32()}},
               "created_at":{{expected.GetProperty("created_at").GetUInt64()}},
               "status":"{{expected.GetProperty("status").GetString()}}",
               "username":"{{expected.GetProperty("username").GetString()}}",
               "options":{{expected.GetProperty("options").GetRawText()}}}
              """, HttpJsonOptions)!;

        foreach (var user in new[] { binary, rest })
        {
            Assert.Equal(expected.GetProperty("id").GetUInt32(), user.Id);
            Assert.Equal(expected.GetProperty("created_at").GetUInt64(), user.CreatedAt);
            Assert.Equal(Enum.Parse<UserStatus>(expected.GetProperty("status").GetString()!, true), user.Status);
            Assert.Equal(expected.GetProperty("username").GetString(), user.Username);
            Assert.Equal(Projection(expected.GetProperty("options")), Projection(user.Options, user.DerivedOptions));
            AssertNeverReturned(user.Options, user.DerivedOptions);
        }
    }

    /// <summary>
    ///     Builds the typed update a vector describes and returns its TCP payload, its REST path and the call
    ///     that sends it through a client.
    /// </summary>
    private static (byte[] Wire, string Path, Func<Apache.Iggy.IggyClient.IIggyClient, Task> Send) UpdateRequest(
        string scope, JsonElement target, JsonElement input)
    {
        var send = UpdateCall(scope, target, input);
        switch (scope)
        {
            case "topic":
                {
                    var streamId = TargetIdentifier(target.GetProperty("stream_id"));
                    var topicId = TargetIdentifier(target.GetProperty("topic_id"));
                    return (TcpContracts.UpdateTopic(streamId, topicId, TopicUpdateFrom(input).ToPatch()),
                        $"/streams/{streamId}/topics/{topicId}", send);
                }
            case "stream":
                {
                    var streamId = TargetIdentifier(target.GetProperty("stream_id"));
                    return (TcpContracts.UpdateStream(streamId, StreamUpdateFrom(input).ToPatch()),
                        $"/streams/{streamId}", send);
                }
            default:
                {
                    var userId = TargetIdentifier(target.GetProperty("user_id"));
                    return (TcpContracts.UpdateUser(userId, UserUpdateFrom(input).ToPatch()), $"/users/{userId}", send);
                }
        }
    }

    private static Func<Apache.Iggy.IggyClient.IIggyClient, Task> UpdateCall(string scope, JsonElement target,
        JsonElement input)
    {
        var token = TestContext.Current.CancellationToken;
        return scope switch
        {
            "topic" => client => client.UpdateTopicAsync(TargetIdentifier(target.GetProperty("stream_id")),
                TargetIdentifier(target.GetProperty("topic_id")), TopicUpdateFrom(input), token),
            "stream" => client => client.UpdateStreamAsync(TargetIdentifier(target.GetProperty("stream_id")),
                StreamUpdateFrom(input), token),
            _ => client => client.UpdateUserAsync(TargetIdentifier(target.GetProperty("user_id")),
                UserUpdateFrom(input), token)
        };
    }

    private static TopicUpdate TopicUpdateFrom(JsonElement input)
    {
        return new TopicUpdate
        {
            Name = input.TryGetProperty("name", out var name) ? name.GetString() : null,
            MessageExpiry = input.TryGetProperty("message_expiry", out _)
                ? DurationHelpers.FromDuration(ParseU64(input, "message_expiry"))
                : null,
            MaxTopicSize = input.TryGetProperty("max_topic_size", out _) ? ParseU64(input, "max_topic_size") : null,
            Reset = ResetKeys<TopicOptionKey>(input),
            RawReset = RawResetNames<TopicOptionKey>(input)
        };
    }

    private static StreamUpdate StreamUpdateFrom(JsonElement input)
    {
        return new StreamUpdate
        {
            Name = input.TryGetProperty("name", out var name) ? name.GetString() : null,
            Reset = ResetKeys<StreamOptionKey>(input),
            RawReset = RawResetNames<StreamOptionKey>(input)
        };
    }

    private static UserUpdate UserUpdateFrom(JsonElement input)
    {
        return new UserUpdate
        {
            Username = input.TryGetProperty("username", out var username) ? username.GetString() : null,
            Status = input.TryGetProperty("status", out var status)
                ? Enum.Parse<UserStatus>(status.GetString()!, true)
                : null,
            Reset = ResetKeys<UserOptionKey>(input),
            RawReset = RawResetNames<UserOptionKey>(input)
        };
    }

    private static bool HasRawReset(string scope, JsonElement input)
    {
        return scope switch
        {
            "topic" => RawResetNames<TopicOptionKey>(input) is not null,
            "stream" => RawResetNames<StreamOptionKey>(input) is not null,
            _ => RawResetNames<UserOptionKey>(input) is not null
        };
    }

    private static HashSet<TKey>? ResetKeys<TKey>(JsonElement input) where TKey : struct, Enum
    {
        if (!input.TryGetProperty("reset", out var reset))
        {
            return null;
        }

        return reset.EnumerateArray()
            .Select(name => TypedKey<TKey>(name.GetString()!))
            .OfType<TKey>()
            .ToHashSet();
    }

    private static HashSet<string>? RawResetNames<TKey>(JsonElement input) where TKey : struct, Enum
    {
        if (!input.TryGetProperty("reset", out var reset))
        {
            return null;
        }

        var raw = reset.EnumerateArray().Select(name => name.GetString()!)
            .Where(name => TypedKey<TKey>(name) is null)
            .ToHashSet();
        return raw.Count == 0 ? null : raw;
    }

    private static TKey? TypedKey<TKey>(string name) where TKey : struct, Enum
    {
        return Enum.GetValues<TKey>().Select(member => (TKey?)member)
            .FirstOrDefault(member => JsonNamingPolicy.SnakeCaseLower.ConvertName(member.ToString()!) == name);
    }

    /// <summary>
    ///     A REST update body read back as the typed input that produces it: decimal numbers are typed values,
    ///     every other member keeps its JSON text.
    /// </summary>
    private static JsonElement TypedInput(JsonElement body)
    {
        var members = body.EnumerateObject().ToDictionary(member => member.Name, member =>
            member.Value.ValueKind == JsonValueKind.Number
                ? JsonSerializer.SerializeToElement(member.Value.GetUInt64().ToString(CultureInfo.InvariantCulture))
                : member.Value);
        return JsonSerializer.SerializeToElement(members);
    }

    private static void AssertUpdateBody(JsonElement body, JsonElement wireSet, JsonElement wireReset)
    {
        var expectedReset = wireReset.EnumerateArray().Select(name => name.GetString()!).ToHashSet();
        var setMembers = body.EnumerateObject().Where(member => member.Name != "reset").ToList();

        Assert.Equal(wireSet.EnumerateObject().Select(member => member.Name).ToHashSet(),
            setMembers.Select(member => member.Name).ToHashSet());
        foreach (var member in setMembers)
        {
            AssertInputValue(member.Value, wireSet.GetProperty(member.Name));
        }

        if (expectedReset.Count == 0)
        {
            Assert.False(body.TryGetProperty("reset", out _), "an empty reset list stays out of the body");
        }
        else
        {
            Assert.Equal(expectedReset,
                body.GetProperty("reset").EnumerateArray().Select(name => name.GetString()!).ToHashSet());
        }
    }

    private static void AssertCreateOptionsBody(JsonElement body, JsonElement wireOptions)
    {
        var expected = wireOptions.EnumerateObject().ToList();
        if (expected.Count == 0)
        {
            Assert.False(body.TryGetProperty("options", out _), "empty options stay out of the body");
            return;
        }

        var options = body.GetProperty("options");
        Assert.Equal(expected.Select(member => member.Name).ToHashSet(),
            options.EnumerateObject().Select(member => member.Name).ToHashSet());
        foreach (var member in expected)
        {
            AssertInputValue(options.GetProperty(member.Name), member.Value);
        }
    }

    private static void AssertInputValue(JsonElement actual, JsonElement expectedEntry)
    {
        var text = expectedEntry.GetProperty("value").GetString()!;
        switch (expectedEntry.GetProperty("kind").GetString())
        {
            case "uint32" or "uint64":
                Assert.Equal(JsonValueKind.Number, actual.ValueKind);
                Assert.Equal(ulong.Parse(text, CultureInfo.InvariantCulture), actual.GetUInt64());
                break;
            case "bool":
                Assert.Equal(bool.Parse(text), actual.GetBoolean());
                break;
            default:
                Assert.Equal(JsonValueKind.String, actual.ValueKind);
                Assert.Equal(text, actual.GetString());
                break;
        }
    }

    private static List<string> Members(JsonElement body)
    {
        return body.EnumerateObject()
            .Select(member => member.Name == "reset"
                ? $"reset={string.Join(',', member.Value.EnumerateArray().Select(name => name.GetString()).Order(StringComparer.Ordinal))}"
                : $"{member.Name}={member.Value.GetRawText()}")
            .Order(StringComparer.Ordinal)
            .ToList();
    }

    private static List<string> Projection(JsonElement options)
    {
        return options.EnumerateObject()
            .Select(member =>
                $"{member.Name}|{member.Value.GetProperty("value").GetString()}|{member.Value.GetProperty("kind").GetString()}|{member.Value.GetProperty("explicit").GetBoolean()}")
            .Order(StringComparer.Ordinal)
            .ToList();
    }

    private static List<string> Projection(Dictionary<HeaderKey, HeaderValue>? explicitOptions,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions)
    {
        IEnumerable<string> Render(Dictionary<HeaderKey, HeaderValue>? options, bool isExplicit)
        {
            return (options ?? []).Select(option =>
                $"{option.Key.AsString()}|{ResourceOptionsConverter.EncodeValue(option.Value)}|{KindName(option.Value.Kind)}|{isExplicit}");
        }

        return Render(explicitOptions, true).Concat(Render(derivedOptions, false))
            .Order(StringComparer.Ordinal)
            .ToList();
    }

    private static void AssertNeverReturned(Dictionary<HeaderKey, HeaderValue>? explicitOptions,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions)
    {
        var returned = (explicitOptions ?? []).Keys.Concat((derivedOptions ?? []).Keys)
            .Select(key => key.AsString())
            .ToHashSet();
        foreach (var key in Fixture.GetProperty("constants").GetProperty("never_returned_keys").EnumerateArray())
        {
            Assert.DoesNotContain(key.GetString()!, returned);
        }
    }

    /// <summary>The REST JSON of a response projection: its fixed fields plus the shared options object.</summary>
    private static string RestJson(JsonElement expected, string countField)
    {
        return $$"""
                 {"id":{{expected.GetProperty("id").GetUInt32()}},
                  "created_at":{{expected.GetProperty("created_at").GetUInt64()}},
                  "name":"{{expected.GetProperty("name").GetString()}}",
                  "size":"{{expected.GetProperty("size_bytes").GetUInt64()}} B",
                  "messages_count":{{expected.GetProperty("messages_count").GetUInt64()}},
                  "{{countField}}":{{expected.GetProperty(countField).GetUInt32()}},
                  "options":{{expected.GetProperty("options").GetRawText()}}}
                 """;
    }

    private static IReadOnlyDictionary<string, HeaderValue>? RawOptions(JsonElement input)
    {
        if (!input.TryGetProperty("raw", out var raw))
        {
            return null;
        }

        return raw.EnumerateObject()
            .ToDictionary(member => member.Name, member => HeaderValue.FromString(member.Value.GetString()!));
    }

    private static void AssertRedacted(string text, byte[] key)
    {
        if (key.Length == 0)
        {
            return;
        }

        Assert.DoesNotContain(Convert.ToHexString(key), text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(Convert.ToBase64String(key), text);
    }

    private static byte Flags(OptionSpec spec)
    {
        var flags = OptionFlags.None;
        flags |= spec.Updatable ? OptionFlags.Updatable : OptionFlags.None;
        flags |= spec.Resettable ? OptionFlags.Resettable : OptionFlags.None;
        flags |= spec.Secret ? OptionFlags.Secret : OptionFlags.None;
        flags |= spec.ReadOnly ? OptionFlags.ReadOnly : OptionFlags.None;
        flags |= spec.Critical ? OptionFlags.Critical : OptionFlags.None;
        return (byte)flags;
    }

    private static string KindName(HeaderKind kind)
    {
        return kind switch
        {
            HeaderKind.Float => "float32",
            HeaderKind.Double => "float64",
            _ => kind.ToString().ToLowerInvariant()
        };
    }

    private static ulong MicrosSinceEpoch(DateTimeOffset timestamp)
    {
        return (ulong)((timestamp.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / TimeSpan.TicksPerMicrosecond);
    }

    private static Identifier TargetIdentifier(JsonElement value)
    {
        return value.ValueKind == JsonValueKind.Number
            ? Identifier.Numeric(value.GetUInt32())
            : Identifier.String(value.GetString()!);
    }

    private static ulong ParseU64(JsonElement element, string property)
    {
        return ulong.Parse(element.GetProperty(property).GetString()!, CultureInfo.InvariantCulture);
    }

    private static bool InScope(JsonElement row, string scope)
    {
        return row.GetProperty("scopes").EnumerateArray().Any(entry => entry.GetString() == scope);
    }

    private static byte[] Hex(JsonElement vector, string property)
    {
        return Convert.FromHexString(vector.GetProperty(property).GetString()!);
    }

    private static JsonElement Find(string section, string property, string value)
    {
        return Fixture.GetProperty(section).EnumerateArray()
            .Single(entry => entry.GetProperty(property).GetString() == value);
    }

    private static void AddCases(TheoryData<string, string> vectors, string section, string property)
    {
        foreach (var entry in Fixture.GetProperty(section).EnumerateArray())
        {
            vectors.Add(section, entry.GetProperty(property).GetString()!);
        }
    }

    private static JsonElement LoadFixture()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "resource_options.json");
        using var document = JsonDocument.Parse(File.ReadAllText(path));
        Assert.Equal(1, document.RootElement.GetProperty("schema_version").GetInt32());
        return document.RootElement.Clone();
    }
}
