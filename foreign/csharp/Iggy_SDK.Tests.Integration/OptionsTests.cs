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
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class OptionsTests
{
    private static readonly string[] TopicCatalogKeys =
    [
        "compression_algorithm",
        "message_expiry",
        "max_topic_size",
        "segment_size",
        "enforce_fsync",
        "messages_required_to_save",
        "size_of_messages_required_to_save",
        "preallocate_segments"
    ];

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

        specs["enforce_fsync"].Kind.ShouldBe(HeaderKind.Bool);
        specs["enforce_fsync"].DefaultValue.ShouldBe([0]);

        specs["messages_required_to_save"].Kind.ShouldBe(HeaderKind.Uint32);
        BitConverter.ToUInt32(specs["messages_required_to_save"].DefaultValue).ShouldBe(1024u);

        specs["size_of_messages_required_to_save"].Kind.ShouldBe(HeaderKind.Uint64);
        BitConverter.ToUInt64(specs["size_of_messages_required_to_save"].DefaultValue).ShouldBe(1024UL * 1024);

        specs["preallocate_segments"].Kind.ShouldBe(HeaderKind.Bool);
        specs["preallocate_segments"].DefaultValue.ShouldBe([0]);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DescribeOptions_Stream_Should_Return_EmptyCatalog(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        IReadOnlyList<OptionSpec> specs = await client.DescribeOptionsAsync(OptionsScope.Stream);

        specs.ShouldNotBeNull();
        specs.ShouldBeEmpty();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DescribeOptions_User_Should_Return_EmptyCatalog(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        IReadOnlyList<OptionSpec> specs = await client.DescribeOptionsAsync(OptionsScope.User);

        specs.ShouldNotBeNull();
        specs.ShouldBeEmpty();
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
            SegmentSize = 1,
            EnforceFsync = true,
            MessagesRequiredToSave = 1,
            SizeOfMessagesRequiredToSave = 1,
            PreallocateSegments = true
        }.ToDictionary().Keys;

        typedKeys.ShouldAllBe(key => catalogKeys.Contains(key));
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task CreateTopic_WithCatalogOptions_Should_Echo_ExplicitAndDerivedOptions(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var streamName = $"options-{Guid.NewGuid():N}";
        await client.CreateStreamAsync(streamName);

        IReadOnlyList<OptionSpec> catalog = await client.DescribeOptionsAsync(OptionsScope.Topic);
        Dictionary<string, HeaderValue> options = new TopicOptions
        {
            EnforceFsync = true,
            MessagesRequiredToSave = 7
        }.ToDictionary();

        var topic = await client.CreateTopicAsync(Identifier.String(streamName), "opts-topic", 1,
            options: options);

        topic.ShouldNotBeNull();
        topic.Options.ShouldNotBeNull();
        topic.DerivedOptions.ShouldNotBeNull();

        HashSet<string> explicitKeys = topic.Options!.Keys.Select(key => key.AsString()).ToHashSet();
        explicitKeys.ShouldContain("enforce_fsync");
        explicitKeys.ShouldContain("messages_required_to_save");
        AsBool(topic.Options.Single(kv => kv.Key.AsString() == "enforce_fsync").Value).ShouldBeTrue();
        topic.Options.Single(kv => kv.Key.AsString() == "messages_required_to_save").Value.ToString()
            .ShouldBe("7");

        HashSet<string> derivedKeys = topic.DerivedOptions!.Keys.Select(key => key.AsString()).ToHashSet();
        derivedKeys.ShouldNotContain("enforce_fsync");
        derivedKeys.ShouldNotContain("messages_required_to_save");
        derivedKeys.ShouldContain("segment_size");

        HashSet<string> allKeys = explicitKeys.Union(derivedKeys).ToHashSet();
        allKeys.ShouldAllBe(key => catalog.Any(spec => spec.Key == key));

        var fetched = await client.GetTopicByIdAsync(Identifier.String(streamName), Identifier.String("opts-topic"));
        fetched.ShouldNotBeNull();
        AsBool(fetched.Options!.Single(kv => kv.Key.AsString() == "enforce_fsync").Value).ShouldBeTrue();
        fetched.DerivedOptions!.Keys.Select(key => key.AsString()).ShouldContain("segment_size");
    }

    // REST carries options as readable strings ("true"), the binary transports as the canonical
    // kind (Bool byte, rendered "1"), so a bool assertion has to accept both spellings.
    private static bool AsBool(HeaderValue value)
    {
        return value.Kind == HeaderKind.Bool ? value.Value[0] != 0 : bool.Parse(value.ToString());
    }
}
