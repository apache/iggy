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

// The methods of a client: decode the arguments, call the SDK, and return the canonical form of the result.

using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.ConformanceAgent;

internal static class Calls
{
    // The SDK has no default consumer. Consumer 0 is the default of the Rust and Go SDKs.
    private static readonly Consumer DefaultConsumer = Consumer.New(0);

    // ResolveOperationAsync takes no default budget. This is the request timeout of the SDK, and the default of the
    // Rust and Go SDKs.
    private static readonly TimeSpan DefaultResolveDeadline = TimeSpan.FromSeconds(30);

    internal static Task<Reply> CallAsync(Agent agent, IIggyClient client, string method, JsonElement args,
        string? saveAs, CancellationToken token)
    {
        return method switch
        {
            "connect" => Run<NoArgs>(args, _ => client.ConnectAsync(token)),
            "disconnect" => Unsupported("the SDK has no public disconnect"),
            "ping" => Run<NoArgs>(args, _ => client.PingAsync(token)),
            "login_user" => Run<LoginUserArgs, AuthResponse?>(args,
                login => client.LoginUserAsync(login.Username, login.Password, token),
                identity => identity is null ? null : new IdentityResult(identity.UserId)),
            "logout_user" => Run<NoArgs>(args, _ => client.LogoutUserAsync(token)),
            "get_stream" => Run<StreamArgs, StreamResponse?>(args,
                stream => client.GetStreamByIdAsync(stream.StreamId, token),
                stream => stream is null ? null : StreamDetailsResult.From(stream)),
            "get_streams" => Run<NoArgs, IReadOnlyList<StreamResponse>>(args, _ => client.GetStreamsAsync(token),
                streams => streams.Select(StreamResult.From).ToList()),
            "create_stream" => Run<CreateStreamArgs, StreamResponse?>(args, create => client.CreateStreamAsync(
                    create.Name, create.EncryptionKeyHex is { } key
                        ? new StreamOptions { EncryptionKey = Convert.FromHexString(key) }
                        : null, token),
                stream => stream is null ? null : StreamDetailsResult.From(stream)),
            "update_stream" => Run<UpdateStreamArgs>(args, update => client.UpdateStreamAsync(update.StreamId,
                new StreamUpdate { Name = update.Name, RawReset = RawReset(update.Reset) }, token)),
            "delete_stream" => Run<StreamArgs>(args, stream => client.DeleteStreamAsync(stream.StreamId, token)),
            "get_topic" => Run<TopicArgs, TopicResponse?>(args,
                topic => client.GetTopicByIdAsync(topic.StreamId, topic.TopicId, token),
                topic => topic is null ? null : TopicDetailsResult.From(topic)),
            "get_topics" => Run<StreamArgs, IReadOnlyList<TopicResponse>>(args,
                stream => client.GetTopicsAsync(stream.StreamId, token),
                topics => topics.Select(TopicResult.From).ToList()),
            "create_topic" => CreateTopicAsync(client, args, token),
            "update_topic" => Run<UpdateTopicArgs>(args, update => client.UpdateTopicAsync(update.StreamId,
                update.TopicId, new TopicUpdate
                {
                    Name = update.Name,
                    MessageExpiry = update.MessageExpiry?.Value,
                    MaxTopicSize = update.MaxTopicSize?.Value,
                    RawReset = RawReset(update.Reset)
                }, token)),
            "delete_topic" => Run<TopicArgs>(args,
                topic => client.DeleteTopicAsync(topic.StreamId, topic.TopicId, token)),
            "send_messages" => Run<SendMessagesArgs>(args, send => client.SendMessagesAsync(send.StreamId,
                send.TopicId, send.Partitioning, send.Messages.Select(BuildMessage).ToList(), token)),
            "poll_messages" => Run<PollMessagesArgs, PolledMessages>(args, poll => client.PollMessagesAsync(
                    poll.StreamId, poll.TopicId, poll.PartitionId, poll.Consumer ?? DefaultConsumer, poll.Strategy,
                    ReadOptions(poll.Commit, poll.MaxWaitMs) with
                    {
                        Count = poll.Count,
                        MinCount = poll.MinCount ?? new PollOptions().MinCount
                    }, token),
                PolledMessagesResult.From),
            "create_consumer_group" => Run<CreateConsumerGroupArgs, ConsumerGroupResponse?>(args,
                create => client.CreateConsumerGroupAsync(create.StreamId, create.TopicId, create.Name, token),
                group => group is null ? null : ConsumerGroupResult.From(group)),
            "get_consumer_group" => Run<ConsumerGroupArgs, ConsumerGroupResponse?>(args,
                group => client.GetConsumerGroupByIdAsync(group.StreamId, group.TopicId, group.GroupId, token),
                group => group is null ? null : ConsumerGroupResult.From(group)),
            "delete_consumer_group" => Run<ConsumerGroupArgs>(args,
                group => client.DeleteConsumerGroupAsync(group.StreamId, group.TopicId, group.GroupId, token)),
            "build_reader" or "build_consumer" when saveAs is null => Failure($"{method} needs save_as"),
            "build_reader" => Run<BuildReaderArgs>(args, async build =>
                agent.Keep(saveAs!, new ReaderEntity(await BuildReaderAsync(client, build, token)))),
            "build_consumer" => Run<BuildConsumerArgs>(args, async build =>
                agent.Keep(saveAs!, new ConsumerEntity(await BuildConsumerAsync(client, build, token)))),
            "resolve_operation" => Run<ResolveOperationArgs>(args, resolve => client.ResolveOperationAsync(
                resolve.OperationId,
                resolve.DeadlineMs is { } deadline ? TimeSpan.FromMilliseconds(deadline) : DefaultResolveDeadline,
                token)),
            "abandon_operation" => Run<AbandonOperationArgs>(args,
                abandon => client.AbandonOperationAsync(abandon.OperationId)),
            "send_binary_request" => Run<BinaryRequestArgs, byte[]>(args,
                request => client.SendBinaryRequestAsync(request.Code, Convert.FromHexString(request.PayloadHex),
                    token),
                reply => new RawResult(Convert.ToHexStringLower(reply))),
            "send_binary_request_to_replica" => Run<BinaryRequestToReplicaArgs, byte[]>(args,
                request => client.SendBinaryRequestToReplicaAsync(request.Code,
                    Convert.FromHexString(request.PayloadHex), request.Replica, token),
                reply => new RawResult(Convert.ToHexStringLower(reply))),
            "describe_options" => Run<DescribeOptionsArgs, IReadOnlyList<OptionSpec>>(args,
                describe => client.DescribeOptionsAsync(describe.Scope, token),
                specs => specs.Select(OptionSpecResult.From).ToList()),
            "create_user" => Run<CreateUserArgs, UserResponse?>(args, create => client.CreateUserAsync(
                    create.Username, create.Password, create.Status, BuildPermissions(create.Permissions),
                    token: token),
                user => user is null ? null : UserDetailsResult.From(user)),
            "get_user" => Run<UserArgs, UserResponse?>(args, user => client.GetUserAsync(user.UserId, token),
                user => user is null ? null : UserDetailsResult.From(user)),
            "update_user" => Run<UpdateUserArgs>(args, update => client.UpdateUserAsync(update.UserId,
                new UserUpdate { Username = update.Username, Status = update.Status }, token)),
            "delete_user" => Run<UserArgs>(args, user => client.DeleteUserAsync(user.UserId, token)),
            "get_metadata_changes" => Run<MetadataChangesArgs, MetadataChanges>(args,
                changes => client.GetMetadataChangesAsync(MetadataQuery(changes), token), MetadataView.Changes),
            "get_connection_info" => Run<NoArgs, string>(args, _ => Task.FromResult(client.GetCurrentAddress()),
                address => new ConnectionInfoResult(address)),
            "next" => Failure("next is a reader or consumer method"),
            _ => Unsupported($"the method {method}")
        };
    }

    private static Task<Reply> CreateTopicAsync(IIggyClient client, JsonElement args, CancellationToken token)
    {
        return Run<CreateTopicArgs, TopicResponse?>(args, create => create.PartitionsCount is { } partitionsCount
                ? client.CreateTopicAsync(create.StreamId, create.Name, partitionsCount, new TopicOptions
                {
                    MessageExpiry = create.MessageExpiry?.Value,
                    MaxTopicSize = create.MaxTopicSize?.Value
                }, token)
                : throw new UnsupportedArgumentException("a topic without partitions_count: the SDK needs a count"),
            topic => topic is null ? null : TopicDetailsResult.From(topic));
    }

    private static async Task<PartitionReader> BuildReaderAsync(IIggyClient client, BuildReaderArgs build,
        CancellationToken token)
    {
        var builder = client.Reader(build.StreamId, build.TopicId, build.Consumer ?? DefaultConsumer)
            .WithOptions(ReadOptions(build.Commit, build.MaxWaitMs));
        if (build.Partitions is { } partitions)
        {
            builder.WithPartitions([.. partitions]);
        }

        if (build.Start is { } start)
        {
            builder.WithStart(start);
        }

        return await builder.BuildAsync(token);
    }

    private static async Task<IggyConsumer> BuildConsumerAsync(IIggyClient client, BuildConsumerArgs build,
        CancellationToken token)
    {
        var options = ReadOptions(build.Commit, build.MaxWaitMs);
        var consumer = new IggyConsumer(client, new IggyConsumerConfig
        {
            StreamId = build.StreamId,
            TopicId = build.TopicId,
            Consumer = Consumer.Group(build.Group),
            PollOptions = build.Count is { } count ? options with { Count = count } : options
        }, NullLoggerFactory.Instance);
        try
        {
            await consumer.InitAsync(token);
            return consumer;
        }
        catch
        {
            await consumer.DisposeAsync();
            throw;
        }
    }

    /// <summary>The defaults of the SDK's poll options, with the commit policy and the wait of the call.</summary>
    private static PollOptions ReadOptions(CommitPolicy? commit, ulong? maxWaitMs)
    {
        return new PollOptions
        {
            Commit = commit ?? CommitPolicy.Manual,
            MaxWait = TimeSpan.FromMilliseconds(maxWaitMs ?? 0)
        };
    }

    // Raw names reach the server as given, as in the Rust and Go agents, so the server and not a table of the
    // client judges each key.
    private static HashSet<string>? RawReset(IReadOnlyList<string>? reset)
    {
        return reset is { Count: > 0 } ? [.. reset] : null;
    }

    private static Message BuildMessage(MessageArg message)
    {
        var payload = (message.Payload, message.PayloadHex) switch
        {
            ({ } text, null) => Encoding.UTF8.GetBytes(text),
            (null, { } hex) => Convert.FromHexString(hex),
            _ => throw new FormatException("A message needs exactly one of payload and payload_hex.")
        };
        // Id 0 lets the SDK pick the id.
        var id = message.Id is { } idText
            ? UInt128.Parse(idText, NumberStyles.None, CultureInfo.InvariantCulture)
            : UInt128.Zero;
        Dictionary<HeaderKey, HeaderValue>? headers = message.Headers is { Count: > 0 } entries
            ? entries.ToDictionary(entry => HeaderKey.FromString(entry.Key),
                entry => ValueText.Decode(entry.Value.Kind, entry.Value.Value))
            : null;
        return new Message(id, payload, headers);
    }

    private static Permissions? BuildPermissions(PermissionsArg? permissions)
    {
        if (permissions is null)
        {
            return null;
        }

        var global = permissions.Global ?? new GlobalPermissionsArg();
        return new Permissions
        {
            Global = new GlobalPermissions
            {
                ManageServers = global.ManageServers,
                ReadServers = global.ReadServers,
                ManageUsers = global.ManageUsers,
                ReadUsers = global.ReadUsers,
                ManageStreams = global.ManageStreams,
                ReadStreams = global.ReadStreams,
                ManageTopics = global.ManageTopics,
                ReadTopics = global.ReadTopics,
                PollMessages = global.PollMessages,
                SendMessages = global.SendMessages
            }
        };
    }

    private static MetadataChangesQuery MetadataQuery(MetadataChangesArgs changes)
    {
        var query = new MetadataChangesQuery { Start = changes.Start, Groups = changes.Groups ?? [] };
        return changes.Limit is { } limit ? query with { Limit = limit } : query;
    }

    private static Task<Reply> Unsupported(string reason)
    {
        return Task.FromResult<Reply>(new Reply.Unsupported(reason));
    }

    private static Task<Reply> Failure(string reason)
    {
        return Task.FromResult<Reply>(new Reply.Failure(reason));
    }

    private static Task<Reply> Run<TArgs>(JsonElement args, Func<TArgs, Task> call)
    {
        return Run<TArgs, object?>(args, async decoded =>
        {
            await call(decoded);
            return null;
        }, _ => null);
    }

    private static async Task<Reply> Run<TArgs, TValue>(JsonElement args, Func<TArgs, Task<TValue>> call,
        Func<TValue, object?> canonical)
    {
        TArgs decoded;
        try
        {
            decoded = Arguments.Read<TArgs>(args);
        }
        catch (Exception error)
        {
            return new Reply.Failure($"arguments: {error.Message}");
        }

        TValue value;
        try
        {
            value = await call(decoded);
        }
        catch (UnsupportedArgumentException error)
        {
            return new Reply.Unsupported(error.Message);
        }
        catch (Exception error)
        {
            return Reply.FromException(error);
        }

        try
        {
            return new Reply.Result(canonical(value) is { } result ? Canonical.Node(result) : null);
        }
        catch (Exception error)
        {
            return new Reply.Failure($"result: {error.Message}");
        }
    }

    /// <summary>An argument value that the SDK cannot express.</summary>
    private sealed class UnsupportedArgumentException(string message) : Exception(message);
}
