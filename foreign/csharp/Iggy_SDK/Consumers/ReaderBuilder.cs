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

using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Consumers;

/// <summary>
///     Builds a <see cref="PartitionReader" />. Create one with
///     <see cref="Extensions.IggyClientExtension.Reader" />.
/// </summary>
public sealed class ReaderBuilder
{
    internal const uint DEFAULT_PREFETCH_BYTES = 16 * 1024 * 1024;
    internal const uint DEFAULT_PREFETCH_MESSAGES = 16_000;

    /// <summary>The wait of a reader read when the caller sets no options.</summary>
    internal static readonly TimeSpan DefaultMaxWait = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan DefaultRetryInterval = TimeSpan.FromSeconds(1);

    private readonly IIggyClient _client;
    private readonly Consumer _consumer;
    private readonly Identifier _streamId;
    private readonly Identifier _topicId;
    private ILoggerFactory _loggerFactory = NullLoggerFactory.Instance;
    private PollOptions _options = new() { MaxWait = DefaultMaxWait };
    private uint[] _partitions = [];
    private uint _prefetchBytes = DEFAULT_PREFETCH_BYTES;
    private uint _prefetchMessages = DEFAULT_PREFETCH_MESSAGES;
    private TimeSpan _retryInterval = DefaultRetryInterval;
    private PollingStrategy _start = PollingStrategy.Next();
    private bool _countsMessages;

    internal ReaderBuilder(IIggyClient client, Identifier streamId, Identifier topicId, Consumer consumer)
    {
        _client = client;
        _streamId = streamId;
        _topicId = topicId;
        _consumer = consumer;
    }

    /// <summary>The partitions of a standalone consumer. A consumer group reads the assignment of its member.</summary>
    public ReaderBuilder WithPartitions(params uint[] partitions)
    {
        _partitions = [.. partitions.Distinct()];
        return this;
    }

    /// <summary>
    ///     Where a partition without a position starts. <see cref="PollingStrategy.Next" /> by default, the stored
    ///     offset of the consumer.
    /// </summary>
    public ReaderBuilder WithStart(PollingStrategy start)
    {
        _start = start;
        return this;
    }

    /// <summary>
    ///     The limits of each read and the commit policy. By default a read waits up to 5 seconds and nothing is
    ///     stored.
    /// </summary>
    public ReaderBuilder WithOptions(PollOptions options)
    {
        _options = options;
        return this;
    }

    /// <summary>
    ///     The most bytes and messages that the reader holds read ahead of the application, 16 MiB and 16,000 by
    ///     default. With <see cref="CommitPolicy.OnRead" /> the reader reads only when the application waits.
    /// </summary>
    public ReaderBuilder WithPrefetch(uint bytes, uint messages)
    {
        _prefetchBytes = bytes;
        _prefetchMessages = messages;
        return this;
    }

    /// <summary>The logger factory of the reader.</summary>
    public ReaderBuilder WithLoggerFactory(ILoggerFactory loggerFactory)
    {
        _loggerFactory = loggerFactory;
        return this;
    }

    /// <summary>
    ///     A consumer hands over one message at a time, so it reports each delivered message and counts it itself
    ///     instead of the reader counting whole batches.
    /// </summary>
    internal ReaderBuilder CountingMessages()
    {
        _countsMessages = true;
        return this;
    }

    /// <summary>The wait before a refused partition reads again.</summary>
    internal ReaderBuilder WithRetryInterval(TimeSpan retryInterval)
    {
        _retryInterval = retryInterval;
        return this;
    }

    /// <summary>Checks the configuration, joins the group of a group reader and starts reading.</summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 3 for partitions of a consumer group, a standalone consumer without partitions, or a continuation
    ///     as the start. Status 2 for <see cref="CommitPolicy.OnRead" /> with an encryptor: the server would store
    ///     the offset of a batch before the client decrypts it. The status of invalid options, and the error of the
    ///     join.
    /// </exception>
    /// <exception cref="FeatureUnavailableException">A consumer group on a transport without its assignment, such as HTTP.</exception>
    public async Task<PartitionReader> BuildAsync(CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(_options);
        _options.Validate();
        var group = _consumer.Type == ConsumerType.ConsumerGroup;
        if (group && _partitions.Length > 0)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND,
                "A consumer group reads the partitions of its assignment, not partitions given to the reader.");
        }

        if (!group && _partitions.Length == 0)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND, "A standalone reader needs partitions.");
        }

        if (_start.PartitionId is not null)
        {
            throw VsrError.Exception(VsrError.INVALID_COMMAND,
                "A continuation belongs to one partition, so it cannot start a reader.");
        }

        if (_options.Commit.Mode == CommitMode.OnRead && _client.MessageEncryptor is not null)
        {
            throw VsrError.Exception(VsrError.INVALID_CONFIGURATION,
                "Storing the offset on read with an encryptor loses a batch that does not decrypt.");
        }

        var assignment = _client as IConsumerGroupAssignmentProvider;
        if (group && assignment is null)
        {
            throw new FeatureUnavailableException();
        }

        if (group)
        {
            await _client.JoinConsumerGroupAsync(_streamId, _topicId, _consumer.ConsumerId, token);
        }

        var settings = new ReaderSettings(_streamId, _topicId, _consumer, _partitions,
            _start.Kind == MessagePolling.First ? PollingStrategy.Offset(0) : _start, _options, _prefetchBytes,
            _prefetchMessages, _retryInterval)
        {
            CountsMessages = _countsMessages
        };
        return new PartitionReader(_client, group ? assignment : null, settings,
            _loggerFactory.CreateLogger<PartitionReader>());
    }
}

/// <summary>The fixed configuration of a reader.</summary>
internal sealed record ReaderSettings(
    Identifier StreamId,
    Identifier TopicId,
    Consumer Consumer,
    IReadOnlyList<uint> Partitions,
    PollingStrategy Start,
    PollOptions Options,
    uint PrefetchBytes,
    uint PrefetchMessages,
    TimeSpan RetryInterval)
{
    /// <summary>Whether the owner of the reader reports and counts each delivered message.</summary>
    internal bool CountsMessages { get; init; }
}
