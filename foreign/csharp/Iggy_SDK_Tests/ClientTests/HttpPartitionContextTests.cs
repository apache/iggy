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

using System.Net;
using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Tests.ClientTests;

/// <summary>
///     HTTP sends carry the context the topic details report for their partition, so a send never lands on an
///     incarnation of the partition that replaced the one the client knows.
/// </summary>
public sealed class HttpPartitionContextTests
{
    private const uint PartitionId = 1;
    private static readonly PartitionContext Original = new(17, 9, 52);
    private static readonly PartitionContext Recreated = new(18, 9, 60);

    [Fact]
    public async Task given_topic_details_with_a_context_when_sending_should_send_it_and_read_the_topic_once()
    {
        var handler = new TopicHandler(Original);
        var client = Client(handler);

        await SendAsync(client);
        await SendAsync(client);

        Assert.Equal(1, handler.TopicReads);
        Assert.Equal(2, handler.SendBodies.Count);
        Assert.All(handler.SendBodies, body => Assert.Contains(ContextJson(Original), body));
    }

    [Fact]
    public async Task given_a_history_refusal_when_sending_should_send_once_more_under_a_fresh_context()
    {
        var handler = new TopicHandler(Original, Recreated);
        handler.SendStatuses.Enqueue(HttpStatusCode.BadRequest);
        var client = Client(handler);

        await SendAsync(client);

        Assert.Equal(2, handler.TopicReads);
        Assert.Equal(2, handler.SendBodies.Count);
        Assert.Contains(ContextJson(Recreated), handler.SendBodies[1]);
    }

    [Fact]
    public async Task given_this_client_deleted_a_topic_when_sending_again_should_read_the_topic_again()
    {
        var handler = new TopicHandler(Original);
        var client = Client(handler);

        await SendAsync(client);
        await client.DeleteTopicAsync(Identifier.Numeric(1), Identifier.Numeric(3),
            TestContext.Current.CancellationToken);
        await SendAsync(client);

        Assert.Equal(2, handler.TopicReads);
    }

    [Fact]
    public async Task given_this_client_created_a_stream_and_a_topic_when_sending_again_should_keep_the_context()
    {
        var handler = new TopicHandler(Original);
        var client = Client(handler);

        await SendAsync(client);
        await client.CreateStreamAsync("created", TestContext.Current.CancellationToken);
        await client.CreateTopicAsync(Identifier.Numeric(1), "created", 1,
            token: TestContext.Current.CancellationToken);
        await SendAsync(client);

        Assert.Equal(1, handler.TopicReads);
        Assert.All(handler.SendBodies, body => Assert.Contains(ContextJson(Original), body));
    }

    [Fact]
    public async Task given_a_caller_context_when_polling_should_send_it_with_the_poll()
    {
        var handler = new TopicHandler(Original);
        var client = Client(handler);

        await client.PollMessagesAsync(Identifier.Numeric(1), Identifier.Numeric(2), PartitionId, Consumer.New(1),
            PollingStrategy.Next().WithContext(Recreated), 10, false, TestContext.Current.CancellationToken);

        Assert.Contains($"&context={Uri.EscapeDataString(ContextJson(Recreated))}", handler.PollQuery);
    }

    [Fact]
    public async Task given_a_strategy_per_partition_when_polling_a_partition_should_poll_with_its_strategy()
    {
        var handler = new TopicHandler(Original);
        IIggyClient client = Client(handler);
        var asked = new List<uint>();

        await client.PollMessagesAsync(Identifier.Numeric(1), Identifier.Numeric(2), PartitionId, Consumer.New(1),
            partitionId =>
            {
                asked.Add(partitionId);
                return PollingStrategy.Offset(7).WithContext(Recreated);
            }, 10, false, TestContext.Current.CancellationToken);

        Assert.Equal([PartitionId], asked);
        Assert.Contains("&value=7&", handler.PollQuery);
        Assert.Contains($"&context={Uri.EscapeDataString(ContextJson(Recreated))}", handler.PollQuery);
    }

    /// <summary>The HTTP client cannot pick a partition of a consumer group, so it cannot ask for its strategy.</summary>
    [Fact]
    public async Task given_a_strategy_per_partition_when_polling_a_group_without_a_partition_should_refuse_it()
    {
        var handler = new TopicHandler(Original);
        IIggyClient client = Client(handler);

        await Assert.ThrowsAsync<FeatureUnavailableException>(() => client.PollMessagesAsync(Identifier.Numeric(1),
            Identifier.Numeric(2), null, Consumer.Group(1), _ => PollingStrategy.Next(), 10, false,
            TestContext.Current.CancellationToken));

        Assert.Empty(handler.PollQuery);
    }

    private static HttpMessageStream Client(TopicHandler handler)
    {
        return new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
    }

    private static Task SendAsync(HttpMessageStream client)
    {
        return client.SendMessagesAsync(Identifier.Numeric(1), Identifier.Numeric(2),
            Partitioning.PartitionId(PartitionId), [new Message(Guid.NewGuid(), "payload"u8.ToArray())],
            TestContext.Current.CancellationToken);
    }

    private static string ContextJson(PartitionContext context)
    {
        return $"{{\"incarnation\":{context.Incarnation},\"owner_generation\":{context.OwnerGeneration}," +
               $"\"metadata_op\":{context.MetadataOp}}}";
    }

    /// <summary>
    ///     Serves the topic details of stream 1, topic 2. Each topic read reports the next of the given contexts and
    ///     keeps reporting the last one.
    /// </summary>
    private sealed class TopicHandler(params PartitionContext[] contexts) : HttpMessageHandler
    {
        private readonly Queue<PartitionContext> _contexts = new(contexts);

        internal Queue<HttpStatusCode> SendStatuses { get; } = new();

        internal List<string> SendBodies { get; } = [];

        internal int TopicReads { get; private set; }

        internal string PollQuery { get; private set; } = string.Empty;

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken ct)
        {
            var path = request.RequestUri!.AbsolutePath;
            if (request.Method == HttpMethod.Get && path == "/streams/1/topics/2")
            {
                TopicReads++;
                var context = _contexts.Count > 1 ? _contexts.Dequeue() : _contexts.Peek();
                return Json(request, HttpStatusCode.OK, TopicJson(context));
            }

            if (request.Method == HttpMethod.Post && path == "/streams/1/topics/2/messages")
            {
                SendBodies.Add(await request.Content!.ReadAsStringAsync(ct));
                return SendStatuses.TryDequeue(out var status)
                    ? Json(request, status, """{"id":87,"code":"history_unavailable","reason":"replaced"}""")
                    : Json(request, HttpStatusCode.OK, """{"confirmations":[]}""");
            }

            if (request.Method == HttpMethod.Get && path == "/streams/1/topics/2/messages")
            {
                PollQuery = request.RequestUri.Query;
                return Json(request, HttpStatusCode.OK, """{"partition_id":1,"current_offset":0,"messages":[]}""");
            }

            if (request.Method == HttpMethod.Post && path == "/streams")
            {
                return Json(request, HttpStatusCode.Created,
                    """{"id":1,"created_at":1750000000000000,"name":"created","size":"0 B","messages_count":0,"topics_count":0}""");
            }

            if (request.Method == HttpMethod.Post && path == "/streams/1/topics")
            {
                return Json(request, HttpStatusCode.Created, TopicJson(_contexts.Peek()));
            }

            return Json(request, HttpStatusCode.NoContent, string.Empty);
        }

        private static string TopicJson(PartitionContext context)
        {
            return $$"""
                     {
                       "id": 2,
                       "created_at": 1750000000000000,
                       "name": "topic",
                       "size": "0 B",
                       "message_expiry": 0,
                       "compression_algorithm": "none",
                       "max_topic_size": 0,
                       "messages_count": 0,
                       "partitions_count": 2,
                       "partitions": [
                         {
                           "id": 0,
                           "created_at": 1750000000000000,
                           "segments_count": 1,
                           "current_offset": 0,
                           "size": "0 B",
                           "messages_count": 0,
                           "context": { "incarnation": 5, "owner_generation": 0, "metadata_op": 6 }
                         },
                         {
                           "id": 1,
                           "created_at": 1750000000000000,
                           "segments_count": 1,
                           "current_offset": 0,
                           "size": "0 B",
                           "messages_count": 0,
                           "context": {{ContextJson(context)}}
                         }
                       ]
                     }
                     """;
        }

        private static HttpResponseMessage Json(HttpRequestMessage request, HttpStatusCode status, string json)
        {
            return new HttpResponseMessage(status)
            {
                RequestMessage = request,
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            };
        }
    }
}
