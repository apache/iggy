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
using System.Text.Json;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Tests.MapperTests;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.ClientTests;

public sealed class HttpMetadataChangesTests
{
    [Fact]
    public async Task given_query_when_reading_changes_over_http_should_send_the_documented_query_and_decode_the_page()
    {
        var request = Vector("requests", "after_cursor_with_time_range");
        var page = Vector("responses", "changes_page").GetProperty("expected");
        var handler = new RecordingHandler(page.GetRawText());
        using var client = Client(handler);

        var answer = await client.GetMetadataChangesAsync(new MetadataChangesQuery
        {
            Start = MetadataChangeStart.After(MetadataChangeCursor.Parse(
                request.GetProperty("input").GetProperty("cursor").GetString()!)),
            Groups = [MetadataChangeGroup.Topics, MetadataChangeGroup.Streams],
            Since = 1760356800000000,
            Until = 1760360400000000,
            Limit = 500
        }, TestContext.Current.CancellationToken);

        Assert.Equal("/metadata/changes", handler.Path);
        Assert.Equal($"?{request.GetProperty("http_query").GetString()}", handler.Query);
        Assert.Equal(MetadataChangeStatus.Ok, answer.Status);
        Assert.Equal(page.GetProperty("next").GetString(), answer.Next.ToString());
        Assert.Equal(
            new MetadataChangeKind?[]
            {
                MetadataChangeKind.StreamCreated, MetadataChangeKind.StreamUpdated, MetadataChangeKind.StreamDeleted
            },
            answer.Records.Select(record => record.Kind));
        Assert.True(Assert.IsType<MetadataStreamChange>(answer.Records[1].Body).OptionsUnreadable);
    }

    [Fact]
    public async Task given_refused_query_when_reading_changes_over_http_should_not_send_it()
    {
        var handler = new RecordingHandler("{}");
        using var client = Client(handler);

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.GetMetadataChangesAsync(new MetadataChangesQuery
            {
                Start = MetadataChangeStart.Current(),
                Groups = [MetadataChangeGroup.Topics]
            }, TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.INVALID_COMMAND, refusal.StatusCode);
        Assert.Equal(0, handler.Requests);
    }

    [Fact]
    public async Task given_forbidden_answer_when_reading_changes_over_http_should_raise_unauthorized()
    {
        var handler = new RecordingHandler("""{"id":41,"code":"unauthorized","reason":"Unauthorized"}""")
        {
            StatusCode = HttpStatusCode.Forbidden
        };
        using var client = Client(handler);

        var refusal = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.GetMetadataChangesAsync(new MetadataChangesQuery(), TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.UNAUTHORIZED, refusal.StatusCode);
        Assert.True(refusal.FromServer);
    }

    [Fact]
    public async Task given_server_without_the_route_when_reading_changes_over_http_should_raise_instead_of_an_empty_page()
    {
        var handler = new RecordingHandler("") { StatusCode = HttpStatusCode.NotFound };
        using var client = Client(handler);

        await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            client.GetMetadataChangesAsync(new MetadataChangesQuery(), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task given_record_of_a_kind_this_client_does_not_know_when_reading_over_http_should_keep_it_whole()
    {
        const string unknown = """{"op":9,"ordinal":0,"checksum":"0000000000000000000000000000000a","timestamp":1,"group":"streams","kind":"stream_archived","archive":"cold"}""";
        var handler = new RecordingHandler($$"""
            {"status":"ok","phase":"changes","log_id":"0000000000000000000000000000000a","oldest_op":1,
             "retained_since":0,"head_op":9,"next":null,"has_more":false,"records":[{{unknown}}],"items":[],
             "current_page_token":null}
            """);
        using var client = Client(handler);

        var answer = await client.GetMetadataChangesAsync(new MetadataChangesQuery(),
            TestContext.Current.CancellationToken);

        var record = Assert.Single(answer.Records);
        Assert.Null(record.Kind);
        Assert.Null(record.Group);
        Assert.Equal(9UL, record.Op);
        var body = Assert.IsType<MetadataUnknownJsonBody>(record.Body);
        Assert.Equal("streams", body.Group);
        Assert.Equal("stream_archived", body.Kind);
        Assert.Equal(unknown, body.Json);
    }

    [Fact]
    public async Task given_answer_with_a_value_of_the_wrong_type_when_reading_over_http_should_raise_a_json_error()
    {
        var handler = new RecordingHandler("""
            {"status":"ok","phase":"changes","log_id":null,"oldest_op":"1","retained_since":0,"head_op":0,
             "next":null,"has_more":false,"records":[],"items":[],"current_page_token":null}
            """);
        using var client = Client(handler);

        await Assert.ThrowsAsync<JsonException>(() =>
            client.GetMetadataChangesAsync(new MetadataChangesQuery(), TestContext.Current.CancellationToken));
    }

    [Theory]
    [InlineData("changes", """[{"op":9,"ordinal":0,"checksum":"0000000000000000000000000000000a","timestamp":1,"group":null,"kind":"stream_created"}]""", "[]")]
    [InlineData("changes", """[{"op":9,"ordinal":0,"checksum":"0000000000000000000000000000000a","timestamp":1,"group":"streams","kind":null}]""", "[]")]
    [InlineData("current", "[]", """[{"group":null}]""")]
    public async Task given_record_or_item_with_a_null_group_or_kind_when_reading_over_http_should_raise_a_json_error(
        string phase, string records, string items)
    {
        var handler = new RecordingHandler($$"""
            {"status":"ok","phase":"{{phase}}","log_id":null,"oldest_op":0,"retained_since":0,"head_op":0,
             "next":null,"has_more":false,"records":{{records}},"items":{{items}},"current_page_token":null}
            """);
        using var client = Client(handler);

        await Assert.ThrowsAsync<JsonException>(() =>
            client.GetMetadataChangesAsync(new MetadataChangesQuery(), TestContext.Current.CancellationToken));
    }

    private static HttpMessageStream Client(RecordingHandler handler)
    {
        return new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
    }

    private static JsonElement Vector(string section, string vectorCase)
    {
        return MetadataChangesTests.Fixture.GetProperty(section).EnumerateArray()
            .Single(entry => entry.GetProperty("case").GetString() == vectorCase);
    }

    private sealed class RecordingHandler(string json) : HttpMessageHandler
    {
        internal HttpStatusCode StatusCode { get; init; } = HttpStatusCode.OK;

        internal int Requests { get; private set; }

        internal string Path { get; private set; } = string.Empty;

        internal string Query { get; private set; } = string.Empty;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            Requests++;
            Path = request.RequestUri?.AbsolutePath ?? string.Empty;
            Query = request.RequestUri?.Query ?? string.Empty;
            return Task.FromResult(new HttpResponseMessage(StatusCode)
            {
                RequestMessage = request,
                Content = new StringContent(json, Encoding.UTF8, "application/json")
            });
        }
    }
}
