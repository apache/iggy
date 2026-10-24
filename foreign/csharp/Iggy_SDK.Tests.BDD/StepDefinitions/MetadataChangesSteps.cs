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

using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Factory;
using Apache.Iggy.Headers;
using Reqnroll;
using Shouldly;
using TestContext = Apache.Iggy.Tests.BDD.Context.TestContext;
using TestEnvironment = Apache.Iggy.Tests.BDD.Context.TestEnvironment;

namespace Apache.Iggy.Tests.BDD.StepDefinitions;

[Binding]
public sealed class MetadataChangesSteps
{
    private const uint RootUserId = 0;
    private const int FollowAttempts = 100;
    private const int Unauthorized = 41;

    private static readonly TimeSpan FollowInterval = TimeSpan.FromMilliseconds(100);
    private static readonly string[] HiddenOptions = ["encryption_key", "encryption_key_wrapped", "critical_keys"];

    private readonly TestContext _context;
    private IReadOnlyList<MetadataChangeGroup> _groups = [];
    private MetadataChangeCursor? _cursor;
    private List<MetadataChangeRecord> _followed = [];
    private List<MetadataCurrentItem> _items = [];
    private uint? _userId;

    public MetadataChangesSteps(TestContext context)
    {
        _context = context;
    }

    // The client may be signed in as a user without manage users by now, so a root client removes the user.
    [AfterScenario(Order = 0)]
    public async Task DeleteCreatedUser()
    {
        if (_userId is not { } userId)
        {
            return;
        }

        using var root = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = _context.TcpUrl,
            Protocol = Protocol.Tcp
        });
        await root.ConnectAsync();
        await root.LoginUserAsync(TestEnvironment.RootUsername, TestEnvironment.RootPassword);
        await root.DeleteUserAsync(Identifier.Numeric(userId));
    }

    [Given("I hold the latest metadata change cursor for the groups {string}")]
    public async Task HoldLatestCursor(string groups)
    {
        _groups = ParseGroups(groups);
        var page = await Read(MetadataChangeStart.Latest);
        page.Status.ShouldBe(MetadataChangeStatus.Ok);
        _cursor = page.Next.ShouldNotBeNull();
    }

    [When("I create and delete {int} streams named {string}")]
    public async Task ChurnStreams(int count, string prefix)
    {
        for (var index = 0; index < count; index++)
        {
            var stream = await _context.IggyClient.CreateStreamAsync($"{prefix}-{index}");
            stream.ShouldNotBeNull();
            await _context.IggyClient.DeleteStreamAsync(Identifier.Numeric(stream.Id));
        }
    }

    [Then("following the metadata changes should return the kinds {string} for the stream")]
    public async Task FollowedKinds(string kinds)
    {
        var expected = kinds.Split(',').Select(name => Enum.Parse<MetadataChangeKind>(name.Replace("_", ""), true))
            .ToList();
        var streamId = StreamId();
        var records = new List<MetadataChangeRecord>();
        var cursor = _cursor.ShouldNotBeNull();
        // A two phase delete commits its record after the call returns.
        for (var attempt = 0; attempt < FollowAttempts; attempt++)
        {
            var page = await Read(MetadataChangeStart.After(cursor));
            page.Status.ShouldBe(MetadataChangeStatus.Ok);
            cursor = page.Next.ShouldNotBeNull();
            records.AddRange(page.Records);
            if (page.HasMore)
            {
                continue;
            }

            if (records.Count(record => ChangeStreamId(record) == streamId) >= expected.Count)
            {
                break;
            }

            await Task.Delay(FollowInterval);
        }

        foreach (var record in records)
        {
            record.Group.ShouldNotBeNull();
            _groups.ShouldContain(record.Group.Value);
        }

        _followed = records.Where(record => ChangeStreamId(record) == streamId).ToList();
        _followed.Select(record => record.Kind).ShouldBe(expected.Select(kind => (MetadataChangeKind?)kind));
    }

    [Then("each followed change should name the root user as its actor")]
    public void RootActor()
    {
        _followed.ShouldAllBe(record => record.ActorUserId == RootUserId);
    }

    [Then("the followed {string} change should carry a requested op before its own op")]
    public void RequestedOpBeforeOwnOp(string kind)
    {
        var record = Followed(kind);
        var deleted = record.Body.ShouldBeOfType<MetadataStreamDeleted>();
        deleted.RequestedOp.ShouldNotBeNull();
        deleted.RequestedOp.Value.ShouldBeLessThan(record.Op);
    }

    [Then("the followed {string} change should report encryption key source {string}")]
    public void KeySource(string kind, string source)
    {
        FollowedStream(kind).EncryptionKeySource.ShouldBe(Enum.Parse<StreamEncryptionSource>(source, true));
    }

    [Then("the followed {string} change should carry the stream key fingerprint")]
    public void KeyFingerprint(string kind)
    {
        var key = _context.LastStreamKey.ShouldNotBeNull();
        FollowedStream(kind).EncryptionKeyFingerprint.ShouldBe(EncryptionKeys.Fingerprint(key));
    }

    [Then("the followed changes should expose no encryption key material")]
    public void NoKeyMaterial()
    {
        foreach (var stream in _followed.Select(record => record.Body).OfType<MetadataStreamChange>())
        {
            stream.OptionsUnreadable.ShouldBeFalse();
            foreach (var options in new[] { stream.Options, stream.DerivedOptions })
            {
                foreach (var name in HiddenOptions)
                {
                    options.ShouldNotBeNull().ContainsKey(HeaderKey.FromString(name)).ShouldBeFalse();
                }
            }
        }
    }

    [Then("following the metadata changes should answer {string}")]
    public async Task FollowStatus(string status)
    {
        var expected = Enum.Parse<MetadataChangeStatus>(status, true);
        var page = await Read(MetadataChangeStart.After(_cursor.ShouldNotBeNull()));
        page.Status.ShouldBe(expected);
        if (expected != MetadataChangeStatus.Ok)
        {
            page.Records.ShouldBeEmpty();
            page.Next.ShouldBeNull();
        }
    }

    [When("I read the current state for the groups {string}")]
    public async Task ReadCurrentState(string groups)
    {
        _groups = ParseGroups(groups);
        var start = MetadataChangeStart.Current();
        var items = new List<MetadataCurrentItem>();
        while (true)
        {
            var page = await Read(start);
            page.Status.ShouldBe(MetadataChangeStatus.Ok);
            items.AddRange(page.Items);
            if (page.CurrentPageToken is not { } token)
            {
                _cursor = page.Next.ShouldNotBeNull();
                break;
            }

            start = MetadataChangeStart.Current(token);
        }

        _items = items;
    }

    [Then("the current state should list the same streams as the stream list")]
    public async Task CurrentStateMatchesList()
    {
        var listed = (await _context.IggyClient.GetStreamsAsync())
            .ToDictionary(stream => stream.Id, stream => stream.Name);
        var projected = _items.Select(item => item.Body).OfType<MetadataStreamChange>()
            .ToDictionary(stream => stream.StreamId, stream => stream.Name);
        projected.ShouldBe(listed, ignoreOrder: true);
    }

    [Given("a user named {string} with password {string} and no server permissions exists")]
    public async Task UserWithoutServerPermissions(string username, string password)
    {
        var user = await _context.IggyClient.CreateUserAsync(username, password, UserStatus.Active, new Permissions
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
        _userId = user.Id;
    }

    [When("I authenticate as {string} with password {string}")]
    public async Task AuthenticateAs(string username, string password)
    {
        await _context.IggyClient.LogoutUserAsync();
        (await _context.IggyClient.LoginUserAsync(username, password)).ShouldNotBeNull();
    }

    [Then("reading the metadata changes should be refused as unauthorized")]
    public async Task ReadingRefused()
    {
        _groups = [];
        var error = await Should.ThrowAsync<IggyInvalidStatusCodeException>(Read(MetadataChangeStart.Earliest));
        error.StatusCode.ShouldBe(Unauthorized);
    }

    private Task<MetadataChanges> Read(MetadataChangeStart start)
    {
        return _context.IggyClient.GetMetadataChangesAsync(new MetadataChangesQuery
        {
            Start = start,
            Groups = _groups,
            Limit = MetadataChangesQuery.MAX_LIMIT
        });
    }

    private MetadataChangeRecord Followed(string kind)
    {
        var parsed = Enum.Parse<MetadataChangeKind>(kind.Replace("_", ""), true);
        return _followed.First(record => record.Kind == parsed);
    }

    private MetadataStreamChange FollowedStream(string kind)
    {
        var stream = Followed(kind).Body.ShouldBeOfType<MetadataStreamChange>();
        stream.OptionsUnreadable.ShouldBeFalse();
        return stream;
    }

    private uint StreamId()
    {
        return _context.CreatedStream.ShouldNotBeNull().Id;
    }

    private static IReadOnlyList<MetadataChangeGroup> ParseGroups(string groups)
    {
        return groups.Split(',').Select(name => Enum.Parse<MetadataChangeGroup>(name.Trim().Replace("_", ""), true))
            .ToArray();
    }

    private static uint? ChangeStreamId(MetadataChangeRecord record)
    {
        return record.Body switch
        {
            MetadataStreamChange stream => stream.StreamId,
            MetadataStreamDeleted stream => stream.StreamId,
            MetadataTopicChange topic => topic.StreamId,
            MetadataTopicDeleted topic => topic.StreamId,
            MetadataPartitionsChanged partitions => partitions.StreamId,
            _ => null
        };
    }
}
