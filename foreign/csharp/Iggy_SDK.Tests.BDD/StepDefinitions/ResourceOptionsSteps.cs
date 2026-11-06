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
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Reqnroll;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;
using TestContext = Apache.Iggy.Tests.BDD.Context.TestContext;

namespace Apache.Iggy.Tests.BDD.StepDefinitions;

[Binding]
public sealed class ResourceOptionsSteps
{
    private readonly TestContext _context;
    private uint? _streamId;
    private uint? _topicId;
    private byte[]? _streamKey;
    private bool _updateRejected;

    public ResourceOptionsSteps(TestContext context)
    {
        _context = context;
    }

    // Features share one server, and this runs before the default-order hook disposes the client.
    [AfterScenario(Order = 0)]
    public async Task DeleteCreatedResources()
    {
        if (_streamId is { } streamId)
        {
            await _context.IggyClient.DeleteStreamAsync(Identifier.Numeric(streamId));
        }
    }

    [When("I create a stream named {string} with an encryption key of 32 bytes valued {int}")]
    public async Task CreateKeyedStream(string name, int value)
    {
        _streamKey = Enumerable.Repeat((byte)value, 32).ToArray();
        _context.LastStreamKey = _streamKey;
        _context.CreatedStream = await _context.IggyClient.CreateStreamAsync(name,
            new StreamOptions { EncryptionKey = _streamKey });
        _context.CreatedStream.ShouldNotBeNull();
        _streamId = _context.CreatedStream.Id;
    }

    [Given("a stream named {string} with an encryption key of 32 bytes valued {int} exists")]
    public Task KeyedStreamExists(string name, int value) => CreateKeyedStream(name, value);

    [When("I create a stream named {string} without an encryption key")]
    public async Task CreateKeylessStream(string name)
    {
        _context.CreatedStream = await _context.IggyClient.CreateStreamAsync(name);
        _context.CreatedStream.ShouldNotBeNull();
        _streamId = _context.CreatedStream.Id;
    }

    [Then("the stream should report encryption key source {string}")]
    public async Task StreamSource(string source)
    {
        var stream = await Stream();
        stream.EncryptionKeySource.ShouldBe(Enum.Parse<StreamEncryptionSource>(source, true));
    }

    [Then("the stream encryption key fingerprint should match the key")]
    public async Task StreamFingerprintMatches()
    {
        _streamKey.ShouldNotBeNull();
        (await Stream()).EncryptionKeyFingerprint.ShouldBe(EncryptionKeys.Fingerprint(_streamKey));
    }

    [Then("the stream should report an encryption key fingerprint")]
    public async Task StreamFingerprintExists()
    {
        var fingerprint = (await Stream()).EncryptionKeyFingerprint;
        fingerprint.ShouldNotBeNull();
        fingerprint!.ShouldMatch("^[0-9a-f]{16}$");
    }

    [Then("the stream should report at least one KEK fingerprint")]
    public async Task KekFingerprintExists()
    {
        var fingerprints = (await Stream()).EncryptionKekFingerprints;
        fingerprints.ShouldNotBeEmpty();
        foreach (var fingerprint in fingerprints)
        {
            fingerprint.ShouldMatch("^[0-9a-f]{16}$");
        }
    }

    [Then("the stream should expose no encryption key material")]
    public async Task StreamDoesNotExposeKey()
    {
        var stream = await Stream();
        foreach (var options in new[] { stream.Options, stream.DerivedOptions })
        {
            if (options is null)
            {
                continue;
            }
            foreach (var name in new[] { "encryption_key", "encryption_key_wrapped", "critical_keys" })
            {
                options.ContainsKey(HeaderKey.FromString(name)).ShouldBeFalse();
            }
        }
    }

    [When("I create a topic named {string} with {int} partition in the stream")]
    public async Task CreateTopic(string name, int partitions)
    {
        _context.CreatedTopic = await _context.IggyClient.CreateTopicAsync(StreamId(), name, (uint)partitions);
        _context.CreatedTopic.ShouldNotBeNull();
        _topicId = _context.CreatedTopic.Id;
    }

    [When("I send a message with payload {string} to the topic")]
    public Task SendPayload(string payload) => SendPayloadWithHeaders(payload, null);

    [When("I send a message with payload {string} and header {string} set to {string} to the topic")]
    public Task SendPayloadWithHeader(string payload, string name, string value) =>
        SendPayloadWithHeaders(payload, new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString(name)] = HeaderValue.FromString(value)
        });

    [Then("polling the topic should return the payload {string}")]
    public async Task PollPayload(string expected)
    {
        var messages = await Poll();
        messages.Count.ShouldBe(1);
        Encoding.UTF8.GetString(messages[0].Payload).ShouldBe(expected);
    }

    [Then("polling the topic should return the payload {string} and header {string} set to {string}")]
    public async Task PollPayloadWithHeader(string expected, string name, string value)
    {
        var messages = await Poll();
        messages.Count.ShouldBe(1);
        Encoding.UTF8.GetString(messages[0].Payload).ShouldBe(expected);
        var headers = messages[0].UserHeaders;
        headers.ShouldNotBeNull();
        headers![HeaderKey.FromString(name)].ToString().ShouldBe(value);
    }

    [When("I delete the stream")]
    public async Task DeleteStream()
    {
        await _context.IggyClient.DeleteStreamAsync(StreamId());
        _streamId = null;
    }

    [When("I rename the stream to {string}")]
    public Task RenameStream(string name) =>
        _context.IggyClient.UpdateStreamAsync(StreamId(), new StreamUpdate { Name = name });

    [Then("the stream name should be {string}")]
    public async Task StreamName(string name)
    {
        (await Stream()).Name.ShouldBe(name);
    }

    [When("I request a reset of the stream option {string}")]
    public async Task ResetStreamOption(string name)
    {
        var key = Enum.Parse<StreamOptionKey>(name.Replace("_", ""), true);
        try
        {
            await _context.IggyClient.UpdateStreamAsync(StreamId(), new StreamUpdate
            {
                Reset = new HashSet<StreamOptionKey> { key }
            });
        }
        catch (Exception)
        {
            _updateRejected = true;
        }
    }

    [Then("the stream update should be rejected")]
    public void StreamUpdateRejected()
    {
        _updateRejected.ShouldBeTrue();
    }

    [When("I describe the stream options")]
    public Task DescribeStreamOptions() => _context.IggyClient.DescribeOptionsAsync(OptionsScope.Stream);

    [Then("the stream option {string} should be updatable and not secret")]
    public async Task UpdatableStreamOption(string name)
    {
        var option = await StreamOption(name);
        option.Updatable.ShouldBeTrue();
        option.Secret.ShouldBeFalse();
    }

    [Then("the stream option {string} should be create only and secret")]
    public async Task SecretStreamOption(string name)
    {
        var option = await StreamOption(name);
        option.Updatable.ShouldBeFalse();
        option.Secret.ShouldBeTrue();
    }

    [Then("the stream option {string} should be read only")]
    public async Task ReadOnlyStreamOption(string name)
    {
        (await StreamOption(name)).ReadOnly.ShouldBeTrue();
    }

    private async Task SendPayloadWithHeaders(string payload, Dictionary<HeaderKey, HeaderValue>? headers)
    {
        var message = new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(payload), headers);
        await _context.IggyClient.SendMessagesAsync(StreamId(), TopicId(), Partitioning.PartitionId(0), [message]);
    }

    private async Task<IReadOnlyList<MessageResponse>> Poll()
    {
        var response = await _context.IggyClient.PollMessagesAsync(new MessageFetchRequest
        {
            StreamId = StreamId(),
            TopicId = TopicId(),
            PartitionId = 0,
            PollingStrategy = PollingStrategy.First(),
            Consumer = Consumer.New(0),
            Count = 10,
            AutoCommit = false
        });
        return response.Messages.ToList();
    }

    private async Task<StreamResponse> Stream()
    {
        var stream = await _context.IggyClient.GetStreamByIdAsync(StreamId());
        stream.ShouldNotBeNull();
        return stream;
    }

    private async Task<OptionSpec> StreamOption(string name)
    {
        var options = await _context.IggyClient.DescribeOptionsAsync(OptionsScope.Stream);
        return options.Single(option => option.Key == name);
    }

    private Identifier StreamId()
    {
        _streamId.ShouldNotBeNull();
        return Identifier.Numeric(_streamId.Value);
    }

    private Identifier TopicId()
    {
        _topicId.ShouldNotBeNull();
        return Identifier.Numeric(_topicId.Value);
    }
}
