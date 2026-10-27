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
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class MetadataChangesTests
{
    private const int InvalidCommand = 3;
    private const int Unauthorized = 41;
    private const uint RootUserId = 0;
    private const int FollowAttempts = 100;

    private static readonly TimeSpan FollowInterval = TimeSpan.FromMilliseconds(100);

    private static readonly string[] HiddenOptions = ["encryption_key", "encryption_key_wrapped", "critical_keys"];

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_stream_lifecycle_when_following_from_latest_should_return_its_records(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var latest = await Read(client, MetadataChangeStart.Latest, [MetadataChangeGroup.Streams]);
        latest.Status.ShouldBe(MetadataChangeStatus.Ok);
        latest.Phase.ShouldBe(MetadataChangePhase.Changes);
        latest.Records.ShouldBeEmpty();
        latest.Next.ShouldNotBeNull();
        latest.LogId.ShouldBe(latest.Next.Value.LogId);
        var name = $"changes-{Guid.NewGuid():N}";
        var renamed = $"{name}-renamed";

        var created = await client.CreateStreamAsync(name);
        created.ShouldNotBeNull();
        await client.UpdateStreamAsync(Identifier.Numeric(created.Id), new StreamUpdate { Name = renamed });
        await client.DeleteStreamAsync(Identifier.Numeric(created.Id));

        var records = await Follow(client, latest.Next.Value, [MetadataChangeGroup.Streams],
            followed => OfStream(followed, created.Id, name).Count >= 3);
        records.ShouldAllBe(record => record.Group == MetadataChangeGroup.Streams);
        records.Select(record => (record.Op, record.Ordinal)).ShouldBeInOrder();
        var mine = OfStream(records, created.Id, name);
        mine.Select(record => record.Kind).ShouldBe([
            MetadataChangeKind.StreamCreated, MetadataChangeKind.StreamUpdated, MetadataChangeKind.StreamDeleted
        ]);
        mine.ShouldAllBe(record => record.ActorUserId == RootUserId);
        mine.ShouldAllBe(record => record.Checksum != UInt128.Zero && record.Timestamp > 0);
        var createdBody = mine[0].Body.ShouldBeOfType<MetadataStreamChange>();
        createdBody.Name.ShouldBe(name);
        createdBody.StreamCreatedOp.ShouldBe(mine[0].Op);
        createdBody.OptionsUnreadable.ShouldBeFalse();
        mine[1].Body.ShouldBeOfType<MetadataStreamChange>().Name.ShouldBe(renamed);
        var deleted = mine[2].Body.ShouldBeOfType<MetadataStreamDeleted>();
        deleted.Name.ShouldBe(renamed);
        deleted.CreatedAt.ShouldBe(createdBody.CreatedAt);
        deleted.RequestedOp.ShouldBeNull();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_topics_group_when_following_should_return_only_topic_records(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var cursor = await LatestCursor(client, [MetadataChangeGroup.Topics]);
        var created = await client.CreateStreamAsync($"changes-filter-{Guid.NewGuid():N}");
        created.ShouldNotBeNull();
        var streamId = Identifier.Numeric(created.Id);
        await client.CreateTopicAsync(streamId, "events", 1);

        try
        {
            var records = await Follow(client, cursor, [MetadataChangeGroup.Topics],
                followed => followed.Any(record => IsTopicOf(record, created.Id)));

            records.ShouldAllBe(record => record.Group == MetadataChangeGroup.Topics);
            var topic = records.Single(record => IsTopicOf(record, created.Id)).Body
                .ShouldBeOfType<MetadataTopicChange>();
            topic.Name.ShouldBe("events");
            topic.PartitionsCount.ShouldBe(1u);
            topic.Options.ShouldNotBeNull();
            topic.DerivedOptions.ShouldNotBeNull();
            topic.DerivedOptions.Keys.Select(key => key.AsString()).ShouldContain("segment_size");
        }
        finally
        {
            await client.DeleteStreamAsync(streamId);
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_limit_of_one_when_following_should_page_one_record_at_a_time(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var cursor = await LatestCursor(client, [MetadataChangeGroup.Streams]);
        var names = Enumerable.Range(0, 3).Select(index => $"changes-page-{index}-{Guid.NewGuid():N}").ToArray();
        var created = new List<uint>();
        foreach (var name in names)
        {
            created.Add((await client.CreateStreamAsync(name))!.Id);
        }

        try
        {
            var seen = new List<string>();
            for (var attempt = 0; attempt < FollowAttempts && seen.Count < names.Length; attempt++)
            {
                var page = await Read(client, MetadataChangeStart.After(cursor), [MetadataChangeGroup.Streams], 1);
                page.Status.ShouldBe(MetadataChangeStatus.Ok);
                page.Records.Count.ShouldBeLessThanOrEqualTo(1);
                var next = page.Next.ShouldNotBeNull();
                if (page.Records.Count == 1)
                {
                    (next.Op, next.Ordinal).ShouldBe((page.Records[0].Op, page.Records[0].Ordinal));
                    page.Records[0].Op.ShouldBeGreaterThan(cursor.Op);
                    if (page.Records[0].Body is MetadataStreamChange stream && names.Contains(stream.Name))
                    {
                        seen.Add(stream.Name);
                    }
                }
                else if (!page.HasMore)
                {
                    await Task.Delay(FollowInterval);
                }

                cursor = next;
            }

            seen.ShouldBe(names);
        }
        finally
        {
            foreach (var id in created)
            {
                await client.DeleteStreamAsync(Identifier.Numeric(id));
            }
        }
    }

    [Test]
    [NotInParallel]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_current_state_scan_when_paging_should_list_the_streams_the_list_returns(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var created = new List<uint>();
        for (var index = 0; index < 3; index++)
        {
            created.Add((await client.CreateStreamAsync($"changes-current-{index}-{Guid.NewGuid():N}"))!.Id);
        }

        var withTopic = Identifier.Numeric(created[0]);
        await client.CreateTopicAsync(withTopic, "events", 2);
        MetadataChangeGroup[] groups = [MetadataChangeGroup.Streams, MetadataChangeGroup.Topics, MetadataChangeGroup.Partitions];

        try
        {
            // A deferred delete another test started can complete at any time, so the scan is compared with a
            // stream list that did not change around it.
            List<MetadataCurrentItem> items = [];
            MetadataChangeCursor? anchor = null;
            Dictionary<uint, string> listed = [];
            for (var attempt = 0; attempt < 3; attempt++)
            {
                var before = await ListStreams(client);
                (items, anchor) = await ScanCurrent(client, groups, 2);
                listed = await ListStreams(client);
                if (before.Count == listed.Count && !before.Except(listed).Any())
                {
                    break;
                }
            }

            var projected = items.Select(item => item.Body).OfType<MetadataStreamChange>()
                .ToDictionary(stream => stream.StreamId, stream => stream.Name);
            projected.ShouldBe(listed, ignoreOrder: true);
            items.ShouldAllBe(item => item.Group != null && groups.Contains(item.Group.Value));
            var topic = items.Select(item => item.Body).OfType<MetadataTopicItem>()
                .Single(item => item.StreamId == created[0]);
            topic.Name.ShouldBe("events");
            items.Select(item => item.Body).OfType<MetadataPartitionsItem>()
                .Single(item => item.StreamId == created[0] && item.TopicId == topic.TopicId)
                .PartitionsCount.ShouldBe(2u);

            var followed = await Read(client, MetadataChangeStart.After(anchor.ShouldNotBeNull()), groups);
            followed.Status.ShouldBe(MetadataChangeStatus.Ok);
        }
        finally
        {
            foreach (var id in created)
            {
                await client.DeleteStreamAsync(Identifier.Numeric(id));
            }
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_users_scan_when_reading_current_state_should_list_the_root_user(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);

        var page = await Read(client, MetadataChangeStart.Current(), [MetadataChangeGroup.Users]);

        page.Status.ShouldBe(MetadataChangeStatus.Ok);
        var root = page.Items.Select(item => item.Body).OfType<MetadataUserChange>().First();
        root.UserId.ShouldBe(RootUserId);
        root.Username.ShouldBe("iggy");
        root.Status.ShouldBe(UserStatus.Active);
        root.OptionsUnreadable.ShouldBeFalse();
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_cursor_of_another_history_or_beyond_the_head_when_following_should_answer_reset_or_behind(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var latest = await LatestCursor(client, [MetadataChangeGroup.Streams]);

        var reset = await Read(client, MetadataChangeStart.After(latest with { LogId = latest.LogId + UInt128.One }),
            [MetadataChangeGroup.Streams]);
        var behind = await Read(client, MetadataChangeStart.After(latest with { Op = ulong.MaxValue }),
            [MetadataChangeGroup.Streams]);

        reset.Status.ShouldBe(MetadataChangeStatus.Reset);
        reset.Records.ShouldBeEmpty();
        reset.Next.ShouldBeNull();
        behind.Status.ShouldBe(MetadataChangeStatus.Behind);
        behind.Records.ShouldBeEmpty();
        behind.Next.ShouldBe(latest with { Op = ulong.MaxValue });
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_query_the_rules_refuse_when_reading_changes_should_refuse_it_before_sending(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var first = await client.CreateStreamAsync($"changes-refused-{Guid.NewGuid():N}");
        var second = await client.CreateStreamAsync($"changes-refused-{Guid.NewGuid():N}");

        try
        {
            var page = await Read(client, MetadataChangeStart.Current(), [MetadataChangeGroup.Streams], 1);
            var token = page.CurrentPageToken.ShouldNotBeNull();
            MetadataChangesQuery[] refused =
            [
                new() { Limit = 0 },
                new() { Limit = MetadataChangesQuery.MAX_LIMIT + 1 },
                new() { Since = 2, Until = 1 },
                new() { Start = MetadataChangeStart.Latest, Since = 1 },
                new() { Start = MetadataChangeStart.Current(), Until = 1 },
                new() { Start = MetadataChangeStart.Current(), Groups = [MetadataChangeGroup.Topics] },
                new()
                {
                    Start = MetadataChangeStart.Current(token),
                    Groups = [MetadataChangeGroup.Streams, MetadataChangeGroup.Topics]
                },
                new() { Start = MetadataChangeStart.After(new MetadataChangeCursor(1UL, 0, 2UL, 0)) }
            ];

            foreach (var query in refused)
            {
                var error = await Should.ThrowAsync<IggyInvalidStatusCodeException>(
                    client.GetMetadataChangesAsync(query));
                error.StatusCode.ShouldBe(InvalidCommand);
                error.FromServer.ShouldBeFalse();
            }
        }
        finally
        {
            await client.DeleteStreamAsync(Identifier.Numeric(first!.Id));
            await client.DeleteStreamAsync(Identifier.Numeric(second!.Id));
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_user_without_manage_servers_when_reading_changes_should_be_refused_as_unauthorized(
        Protocol protocol)
    {
        var root = await Fixture.CreateAuthenticatedClient(protocol);
        var username = $"changes-reader-{Guid.NewGuid():N}";
        const string password = "changes-secret";
        var user = await root.CreateUserAsync(username, password, UserStatus.Active, new Permissions
        {
            Global = new GlobalPermissions
            {
                ManageServers = false,
                ReadServers = true,
                ManageUsers = false,
                ReadUsers = false,
                ManageStreams = false,
                ReadStreams = false,
                ManageTopics = false,
                ReadTopics = false,
                PollMessages = false,
                SendMessages = false
            }
        });
        user.ShouldNotBeNull();

        try
        {
            var reader = await Fixture.CreateAuthenticatedClient(protocol, username, password);

            var error = await Should.ThrowAsync<IggyInvalidStatusCodeException>(
                reader.GetMetadataChangesAsync(new MetadataChangesQuery()));

            error.StatusCode.ShouldBe(Unauthorized);
            error.FromServer.ShouldBeTrue();
        }
        finally
        {
            await root.DeleteUserAsync(Identifier.Numeric(user.Id));
        }
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task given_keyed_and_generated_stream_keys_when_following_should_report_fingerprints_and_no_key(
        Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var cursor = await LatestCursor(client, [MetadataChangeGroup.Streams]);
        var key = RandomNumberGenerator.GetBytes(EncryptionKeys.KEY_LENGTH);
        var keyedName = $"changes-keyed-{Guid.NewGuid():N}";
        var generatedName = $"changes-generated-{Guid.NewGuid():N}";
        var keyed = await client.CreateStreamAsync(keyedName, new StreamOptions { EncryptionKey = key });
        var generated = await client.CreateStreamAsync(generatedName);

        try
        {
            var records = await Follow(client, cursor, [MetadataChangeGroup.Streams],
                followed => OfStream(followed, keyed!.Id, keyedName).Count == 1
                            && OfStream(followed, generated!.Id, generatedName).Count == 1);

            var keyedStream = OfStream(records, keyed!.Id, keyedName).Single().Body
                .ShouldBeOfType<MetadataStreamChange>();
            keyedStream.EncryptionKeySource.ShouldBe(StreamEncryptionSource.Stream);
            keyedStream.EncryptionKeyFingerprint.ShouldBe(EncryptionKeys.Fingerprint(key));
            var generatedStream = OfStream(records, generated!.Id, generatedName).Single().Body
                .ShouldBeOfType<MetadataStreamChange>();
            generatedStream.EncryptionKeySource.ShouldBe(StreamEncryptionSource.Server);
            generatedStream.EncryptionKeyFingerprint.ShouldNotBeNull();
            foreach (var stream in new[] { keyedStream, generatedStream })
            {
                foreach (var options in new[] { stream.Options!, stream.DerivedOptions! })
                {
                    options.Keys.Select(option => option.AsString()).ShouldNotContain(name => HiddenOptions.Contains(name));
                    options.Values.ShouldAllBe(value => !value.Value.SequenceEqual(key));
                }
            }
        }
        finally
        {
            await client.DeleteStreamAsync(Identifier.Numeric(keyed!.Id));
            await client.DeleteStreamAsync(Identifier.Numeric(generated!.Id));
        }
    }

    private static async Task<Dictionary<uint, string>> ListStreams(IIggyClient client)
    {
        return (await client.GetStreamsAsync()).ToDictionary(stream => stream.Id, stream => stream.Name);
    }

    /// <summary>Pages a current state scan to its end and returns the items and the anchor to follow from.</summary>
    private static async Task<(List<MetadataCurrentItem> Items, MetadataChangeCursor Anchor)> ScanCurrent(
        IIggyClient client, IReadOnlyList<MetadataChangeGroup> groups, uint limit)
    {
        var items = new List<MetadataCurrentItem>();
        var start = MetadataChangeStart.Current();
        while (true)
        {
            var page = await Read(client, start, groups, limit);
            page.Status.ShouldBe(MetadataChangeStatus.Ok);
            page.Phase.ShouldBe(MetadataChangePhase.Current);
            page.Records.ShouldBeEmpty();
            page.Items.Count.ShouldBeLessThanOrEqualTo((int)limit);
            page.HasMore.ShouldBe(page.CurrentPageToken is not null);
            items.AddRange(page.Items);
            if (page.CurrentPageToken is not { } token)
            {
                return (items, page.Next.ShouldNotBeNull());
            }

            token.Groups.ShouldBe(groups);
            start = MetadataChangeStart.Current(token);
        }
    }

    private static Task<MetadataChanges> Read(IIggyClient client, MetadataChangeStart start,
        IReadOnlyList<MetadataChangeGroup> groups, uint limit = MetadataChangesQuery.MAX_LIMIT)
    {
        return client.GetMetadataChangesAsync(new MetadataChangesQuery { Start = start, Groups = groups, Limit = limit });
    }

    private static async Task<MetadataChangeCursor> LatestCursor(IIggyClient client,
        IReadOnlyList<MetadataChangeGroup> groups)
    {
        var page = await Read(client, MetadataChangeStart.Latest, groups);
        page.Status.ShouldBe(MetadataChangeStatus.Ok);
        return page.Next.ShouldNotBeNull();
    }

    /// <summary>
    ///     Follows the change log from <paramref name="cursor" /> until <paramref name="done" /> holds. A two phase
    ///     delete commits its record after the call returns, so an empty tail is polled again.
    /// </summary>
    private static async Task<List<MetadataChangeRecord>> Follow(IIggyClient client, MetadataChangeCursor cursor,
        IReadOnlyList<MetadataChangeGroup> groups, Func<List<MetadataChangeRecord>, bool> done)
    {
        var records = new List<MetadataChangeRecord>();
        for (var attempt = 0; attempt < FollowAttempts; attempt++)
        {
            var page = await Read(client, MetadataChangeStart.After(cursor), groups);
            page.Status.ShouldBe(MetadataChangeStatus.Ok);
            cursor = page.Next.ShouldNotBeNull();
            records.AddRange(page.Records);
            if (page.HasMore)
            {
                continue;
            }

            if (done(records))
            {
                return records;
            }

            await Task.Delay(FollowInterval);
        }

        throw new TimeoutException("The followed metadata changes never reached the expected records.");
    }

    /// <summary>
    ///     The records of the stream created under <paramref name="name" />, matched by its full identity because
    ///     stream ids are reused after a delete.
    /// </summary>
    private static List<MetadataChangeRecord> OfStream(List<MetadataChangeRecord> records, uint streamId, string name)
    {
        var createdOp = records.FirstOrDefault(record => record.Body is MetadataStreamChange stream
                                                         && record.Kind == MetadataChangeKind.StreamCreated
                                                         && stream.StreamId == streamId && stream.Name == name)?.Op;
        return records.Where(record => createdOp is { } op && record.Body switch
        {
            MetadataStreamChange stream => stream.StreamId == streamId && stream.StreamCreatedOp == op,
            MetadataStreamDeleted deleted => deleted.StreamId == streamId && deleted.StreamCreatedOp == op,
            _ => false
        }).ToList();
    }

    private static bool IsTopicOf(MetadataChangeRecord record, uint streamId)
    {
        return record is { Kind: MetadataChangeKind.TopicCreated, Body: MetadataTopicChange topic }
               && topic.StreamId == streamId && topic.TopicCreatedOp == record.Op;
    }
}
