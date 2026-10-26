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

using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Apache.Iggy.Contracts;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

public class OptionsTests
{
    private const int OptionNotUpdatable = 4103;
    private const int ReadOnlyOption = 4105;
    private const int UnsupportedOptionKey = 4041;
    private const int InvalidTopicName = 2014;

    private static readonly string[] TopicCatalogKeys =
    [
        "name",
        "compression_algorithm",
        "message_expiry",
        "max_topic_size",
        "segment_size",
        "durability",
        "consumer_offset_durability",
        "partition_resize_policy",
        "messages_required_to_save",
        "size_of_messages_required_to_save",
        "preallocate_segments"
    ];

    private static readonly string[] NeverReturnedKeys = ["encryption_key", "encryption_key_wrapped", "critical_keys"];

    private static readonly Regex Fingerprint = new("^[0-9a-f]{16}$", RegexOptions.Compiled);

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DescribeOptions_Topic_Should_Return_FullCatalog(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        IReadOnlyList<OptionSpec> specs = await client.DescribeOptionsAsync(OptionsScope.Topic);

        specs.ShouldNotBeNull();
        specs.Select(spec => spec.Key).ShouldBe(TopicCatalogKeys);
        foreach (var spec in specs)
        {
            spec.Description.ShouldNotBeNullOrWhiteSpace();
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DescribeOptions_Topic_Should_Return_KindsAndDefaults(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        Dictionary<string, OptionSpec> specs
            = (await client.DescribeOptionsAsync(OptionsScope.Topic)).ToDictionary(spec => spec.Key);

        specs["compression_algorithm"].Kind.ShouldBe(HeaderKind.String);
        Encoding.UTF8.GetString(specs["compression_algorithm"].DefaultValue).ShouldBe("none");

        specs["message_expiry"].Kind.ShouldBe(HeaderKind.Uint64);
        BitConverter.ToUInt64(specs["message_expiry"].DefaultValue).ShouldBe(ulong.MaxValue);

        specs["max_topic_size"].Kind.ShouldBe(HeaderKind.Uint64);
        BitConverter.ToUInt64(specs["max_topic_size"].DefaultValue).ShouldBe(ulong.MaxValue);

        specs["segment_size"].Kind.ShouldBe(HeaderKind.Uint64);
        BitConverter.ToUInt64(specs["segment_size"].DefaultValue).ShouldBe(1024UL * 1024 * 1024);

        specs["durability"].Kind.ShouldBe(HeaderKind.String);
        specs["consumer_offset_durability"].Kind.ShouldBe(HeaderKind.String);
        Encoding.UTF8.GetString(specs["durability"].DefaultValue).ShouldBe("replicated");
        Encoding.UTF8.GetString(specs["consumer_offset_durability"].DefaultValue).ShouldBe("replicated");

        specs["messages_required_to_save"].Kind.ShouldBe(HeaderKind.Uint32);
        BitConverter.ToUInt32(specs["messages_required_to_save"].DefaultValue).ShouldBe(1024u);

        specs["size_of_messages_required_to_save"].Kind.ShouldBe(HeaderKind.Uint64);
        BitConverter.ToUInt64(specs["size_of_messages_required_to_save"].DefaultValue).ShouldBe(1024UL * 1024);

        specs["preallocate_segments"].Kind.ShouldBe(HeaderKind.Bool);
        specs["preallocate_segments"].DefaultValue.ShouldBe([0]);

        specs["message_expiry"].Updatable.ShouldBeTrue();
        specs["message_expiry"].Resettable.ShouldBeTrue();
        specs["compression_algorithm"].Updatable.ShouldBeFalse();
        specs["name"].Resettable.ShouldBeFalse();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DescribeOptions_Stream_Should_Report_SensitivityAndMutability(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        IReadOnlyList<OptionSpec> specs = await client.DescribeOptionsAsync(OptionsScope.Stream);

        specs.Select(spec => spec.Key).ShouldBe([
            "name", "encryption_key", "encryption_key_source", "encryption_key_fingerprint",
            "encryption_kek_fingerprint"
        ]);
        var byKey = specs.ToDictionary(spec => spec.Key);
        byKey["name"].Updatable.ShouldBeTrue();
        byKey["name"].Secret.ShouldBeFalse();
        byKey["encryption_key"].Kind.ShouldBe(HeaderKind.Raw);
        byKey["encryption_key"].Secret.ShouldBeTrue();
        byKey["encryption_key"].Critical.ShouldBeTrue();
        byKey["encryption_key"].Updatable.ShouldBeFalse();
        byKey["encryption_key"].Resettable.ShouldBeFalse();
        foreach (var key in new[] { "encryption_key_source", "encryption_key_fingerprint", "encryption_kek_fingerprint" })
        {
            byKey[key].ReadOnly.ShouldBeTrue();
            byKey[key].Updatable.ShouldBeFalse();
            byKey[key].Secret.ShouldBeFalse();
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DescribeOptions_User_Should_Report_UpdatableColumns(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        IReadOnlyList<OptionSpec> specs = await client.DescribeOptionsAsync(OptionsScope.User);

        specs.Select(spec => spec.Key).ShouldBe(["username", "status"]);
        specs.ShouldAllBe(spec => spec.Updatable && !spec.Resettable && !spec.Secret && !spec.ReadOnly);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DescribeOptions_Topic_Should_Match_TopicOptionsKeys(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        HashSet<string> catalogKeys = (await client.DescribeOptionsAsync(OptionsScope.Topic))
            .Select(spec => spec.Key)
            .ToHashSet();
        var typedKeys = new TopicOptions
        {
            CompressionAlgorithm = CompressionAlgorithm.Gzip,
            MessageExpiry = TimeSpan.FromDays(1),
            MaxTopicSize = 2_000_000_000,
            SegmentSize = 1,
            Durability = Durability.Persisted,
            PartitionResizePolicy = PartitionResizePolicy.Fixed,
            MessagesRequiredToSave = 1,
            SizeOfMessagesRequiredToSave = 1,
            PreallocateSegments = true
        }.ToDictionary().Keys;

        typedKeys.ShouldAllBe(key => catalogKeys.Contains(key));
        catalogKeys.Except(typedKeys).ShouldBe(["name"]);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateTopic_WithCatalogOptions_Should_Echo_ExplicitAndDerivedOptions(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"options-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);

        IReadOnlyList<OptionSpec> catalog = await client.DescribeOptionsAsync(OptionsScope.Topic);
        var options = new TopicOptions
        {
            Durability = Durability.Persisted,
            MessagesRequiredToSave = 7
        };

        var topic = await client.CreateTopicAsync(Identifier.String(streamName), "opts-topic", 1, options);

        topic.ShouldNotBeNull();
        topic.Options.ShouldNotBeNull();
        topic.DerivedOptions.ShouldNotBeNull();

        HashSet<string> explicitKeys = topic.Options!.Keys.Select(key => key.AsString()).ToHashSet();
        explicitKeys.ShouldContain("durability");
        explicitKeys.ShouldContain("messages_required_to_save");
        topic.Options.Single(kv => kv.Key.AsString() == "durability").Value.ToString().ShouldBe("persisted");
        topic.Options.Single(kv => kv.Key.AsString() == "messages_required_to_save").Value.ToString()
            .ShouldBe("7");

        HashSet<string> derivedKeys = topic.DerivedOptions!.Keys.Select(key => key.AsString()).ToHashSet();
        derivedKeys.ShouldNotContain("durability");
        derivedKeys.ShouldNotContain("messages_required_to_save");
        derivedKeys.ShouldContain("segment_size");

        HashSet<string> allKeys = explicitKeys.Union(derivedKeys).ToHashSet();
        allKeys.ShouldAllBe(key => catalog.Any(spec => spec.Key == key));

        var fetched = await client.GetTopicByIdAsync(Identifier.String(streamName), Identifier.String("opts-topic"));
        fetched.ShouldNotBeNull();
        fetched.Options!.Single(kv => kv.Key.AsString() == "durability").Value.ToString().ShouldBe("persisted");
        fetched.DerivedOptions!.Keys.Select(key => key.AsString()).ShouldContain("segment_size");
        fetched.DerivedOptions!.Single(kv => kv.Key.AsString() == "segment_size").Value.Kind
            .ShouldBe(HeaderKind.Uint64);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_stream_key_when_creating_should_read_matching_projection(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var key = RandomNumberGenerator.GetBytes(EncryptionKeys.KEY_LENGTH);

        var created = await client.CreateStreamAsync($"keyed-{Guid.NewGuid():N}", new StreamOptions { EncryptionKey = key });

        created.ShouldNotBeNull();
        var fetched = await client.GetStreamByIdAsync(Identifier.Numeric(created.Id));
        fetched.ShouldNotBeNull();
        var listed = (await client.GetStreamsAsync()).Single(stream => stream.Id == created.Id);
        foreach (var stream in new[] { created, fetched, listed })
        {
            stream.EncryptionKeySource.ShouldBe(StreamEncryptionSource.Stream);
            stream.EncryptionKeyFingerprint.ShouldBe(EncryptionKeys.Fingerprint(key));
            stream.EncryptionKekFingerprints.ShouldNotBeEmpty();
            stream.EncryptionKekFingerprints.ShouldAllBe(fingerprint => Fingerprint.IsMatch(fingerprint));
            stream.EncryptionKekFingerprints.ShouldBe(created.EncryptionKekFingerprints);
            ShouldExposeNoKeyMaterial(stream, key);
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_keyless_stream_when_reading_should_report_server_generated_key(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var first = await client.CreateStreamAsync($"generated-{Guid.NewGuid():N}");
        var second = await client.CreateStreamAsync($"generated-{Guid.NewGuid():N}", new StreamOptions());

        foreach (var created in new[] { first, second })
        {
            created.ShouldNotBeNull();
            var stream = await client.GetStreamByIdAsync(Identifier.Numeric(created.Id));
            stream.ShouldNotBeNull();
            stream.EncryptionKeySource.ShouldBe(StreamEncryptionSource.Server);
            stream.EncryptionKeyFingerprint.ShouldNotBeNull();
            Fingerprint.IsMatch(stream.EncryptionKeyFingerprint).ShouldBeTrue();
            stream.EncryptionKekFingerprints.ShouldNotBeEmpty();
            ShouldExposeNoKeyMaterial(stream, []);
        }

        first!.EncryptionKeyFingerprint.ShouldNotBe(second!.EncryptionKeyFingerprint);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_stream_rename_when_updating_should_preserve_encryption_projection(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var key = RandomNumberGenerator.GetBytes(EncryptionKeys.KEY_LENGTH);
        var created = await client.CreateStreamAsync($"rename-{Guid.NewGuid():N}", new StreamOptions { EncryptionKey = key });
        created.ShouldNotBeNull();
        var streamId = Identifier.Numeric(created.Id);
        await client.CreateTopicAsync(streamId, "events", 1);
        var payload = Encoding.UTF8.GetBytes("before-rename");
        await client.SendMessagesAsync(streamId, Identifier.String("events"), Partitioning.PartitionId(0),
            [new Message(Guid.NewGuid(), payload)]);

        var renamed = $"renamed-{Guid.NewGuid():N}";
        await client.UpdateStreamAsync(streamId, new StreamUpdate { Name = renamed });

        var stream = await client.GetStreamByIdAsync(streamId);
        stream.ShouldNotBeNull();
        stream.Id.ShouldBe(created.Id);
        stream.Name.ShouldBe(renamed);
        stream.EncryptionKeySource.ShouldBe(StreamEncryptionSource.Stream);
        stream.EncryptionKeyFingerprint.ShouldBe(EncryptionKeys.Fingerprint(key));
        stream.EncryptionKekFingerprints.ShouldBe(created.EncryptionKekFingerprints);
        (await PollFirstPayload(client, streamId)).ShouldBe(payload);

        var reset = await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.UpdateStreamAsync(streamId,
            new StreamUpdate { Reset = new HashSet<StreamOptionKey> { StreamOptionKey.EncryptionKey } }));
        reset.StatusCode.ShouldBe(OptionNotUpdatable);
        var unchanged = await client.GetStreamByIdAsync(streamId);
        unchanged!.Name.ShouldBe(renamed);
        unchanged.EncryptionKeyFingerprint.ShouldBe(EncryptionKeys.Fingerprint(key));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_topic_reset_when_updating_should_preserve_unmentioned_values(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var streamName = $"reset-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);
        var streamId = Identifier.String(streamName);
        await client.CreateTopicAsync(streamId, "reset-topic", 1, new TopicOptions
        {
            CompressionAlgorithm = CompressionAlgorithm.Gzip,
            MessageExpiry = TimeSpan.FromHours(1),
            MaxTopicSize = 2_000_000_000
        });

        await client.UpdateTopicAsync(streamId, Identifier.String("reset-topic"), new TopicUpdate
        {
            MessageExpiry = TimeSpan.FromHours(2),
            Reset = new HashSet<TopicOptionKey> { TopicOptionKey.MaxTopicSize }
        });

        var topic = await client.GetTopicByIdAsync(streamId, Identifier.String("reset-topic"));
        topic.ShouldNotBeNull();
        topic.Name.ShouldBe("reset-topic");
        topic.MessageExpiry.ShouldBe(TimeSpan.FromHours(2));
        topic.MaxTopicSize.ShouldBe(ulong.MaxValue);
        topic.CompressionAlgorithm.ShouldBe(CompressionAlgorithm.Gzip);
        topic.Options!.Keys.Select(option => option.AsString()).ShouldContain("message_expiry");
        topic.Options!.Keys.Select(option => option.AsString()).ShouldContain("compression_algorithm");
        topic.Options!.Keys.Select(option => option.AsString()).ShouldNotContain("max_topic_size");
        topic.DerivedOptions!.Keys.Select(option => option.AsString()).ShouldContain("max_topic_size");
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_user_options_when_creating_and_updating_should_preserve_user_behavior(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var username = $"opts-{Guid.NewGuid():N}"[..20];
        var renamed = $"ren-{Guid.NewGuid():N}"[..20];

        var created = await client.CreateUserAsync(username, "options-password", UserStatus.Active,
            options: new UserOptions());

        created.ShouldNotBeNull();
        (created.Options ?? []).ShouldBeEmpty();
        (created.DerivedOptions ?? []).ShouldBeEmpty();

        await client.UpdateUserAsync(Identifier.Numeric(created.Id),
            new UserUpdate { Username = renamed, Status = UserStatus.Inactive });

        var user = await client.GetUserAsync(Identifier.String(renamed));
        user.ShouldNotBeNull();
        user.Id.ShouldBe(created.Id);
        user.Status.ShouldBe(UserStatus.Inactive);
        (user.Options ?? []).ShouldBeEmpty();

        var otherName = $"oth-{Guid.NewGuid():N}"[..20];
        var other = await client.CreateUserAsync(otherName, "options-password", UserStatus.Active);
        other.ShouldNotBeNull();
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.UpdateUserAsync(Identifier.Numeric(other.Id),
            new UserUpdate { Username = renamed, Status = UserStatus.Inactive }));

        var unchanged = await client.GetUserAsync(Identifier.Numeric(other.Id));
        unchanged.ShouldNotBeNull();
        unchanged.Username.ShouldBe(otherName);
        unchanged.Status.ShouldBe(UserStatus.Active);
    }

    [Test]
    public async Task given_http_status_update_when_deactivating_should_prevent_new_login()
    {
        var admin = await Fixture.CreateAuthenticatedClient(Protocol.Http);
        var username = $"deact-{Guid.NewGuid():N}"[..20];
        const string Password = "deactivate-password";
        await admin.CreateUserAsync(username, Password, UserStatus.Active);
        var before = await Fixture.CreateClient(Protocol.Http);
        (await before.LoginUserAsync(username, Password)).ShouldNotBeNull();

        await admin.UpdateUserAsync(Identifier.String(username), new UserUpdate { Status = UserStatus.Inactive });

        (await admin.GetUserAsync(Identifier.String(username)))!.Status.ShouldBe(UserStatus.Inactive);
        var after = await Fixture.CreateClient(Protocol.Http);
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(after.LoginUserAsync(username, Password));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_raw_keys_when_updating_should_report_the_server_category(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var created = await client.CreateUserAsync($"raw-{Guid.NewGuid():N}"[..20], "options-password",
            UserStatus.Active);
        var stream = await client.CreateStreamAsync($"raw-{Guid.NewGuid():N}"[..20]);
        created.ShouldNotBeNull();
        stream.ShouldNotBeNull();

        var internalReset = await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.UpdateUserAsync(
            Identifier.Numeric(created.Id), new UserUpdate { RawReset = new HashSet<string> { "critical_keys" } }));
        internalReset.StatusCode.ShouldBe(ReadOnlyOption);
        internalReset.FromServer.ShouldBeTrue();

        var unknownReset = await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.UpdateStreamAsync(
            Identifier.Numeric(stream.Id), new StreamUpdate { RawReset = new HashSet<string> { "retention" } }));
        unknownReset.StatusCode.ShouldBe(UnsupportedOptionKey);
        unknownReset.FromServer.ShouldBeTrue();

        var emptyName = await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.UpdateStreamAsync(
            Identifier.Numeric(stream.Id), new StreamUpdate
            {
                Name = string.Empty,
                Options = new Dictionary<string, HeaderValue> { ["encryption_key_source"] = HeaderValue.FromString("stream") }
            }));
        emptyName.StatusCode.ShouldBe(ReadOnlyOption);
        emptyName.FromServer.ShouldBeTrue();

        var topic = await client.CreateTopicAsync(Identifier.Numeric(stream.Id), "events", 1);
        topic.ShouldNotBeNull();
        var standIn = await Should.ThrowAsync<IggyInvalidStatusCodeException>(client.UpdateTopicAsync(
            Identifier.Numeric(stream.Id), Identifier.Numeric(topic.Id), new TopicUpdate
            {
                Name = string.Empty,
                RawReset = new HashSet<string> { "message_expiry" }
            }));
        standIn.StatusCode.ShouldBe(InvalidTopicName);
        standIn.FromServer.ShouldBeTrue();
    }

    private static async Task<byte[]> PollFirstPayload(IIggyClient client, Identifier streamId)
    {
        var polled = await client.PollMessagesAsync(new MessageFetchRequest
        {
            Count = 1,
            AutoCommit = false,
            Consumer = Consumer.New(1),
            PartitionId = 0,
            PollingStrategy = PollingStrategy.Offset(0),
            StreamId = streamId,
            TopicId = Identifier.String("events")
        });

        return polled.Messages.Single().Payload;
    }

    private static void ShouldExposeNoKeyMaterial(StreamResponse stream, byte[] key)
    {
        var options = (stream.Options ?? []).Concat(stream.DerivedOptions ?? []).ToList();
        options.Select(option => option.Key.AsString()).ShouldAllBe(name => !NeverReturnedKeys.Contains(name));
        if (key.Length == 0)
        {
            return;
        }

        var base64 = Convert.ToBase64String(key);
        foreach (var (_, value) in options)
        {
            value.Value.SequenceEqual(key).ShouldBeFalse();
            Encoding.UTF8.GetString(value.Value).ShouldNotContain(base64);
        }
    }
}
