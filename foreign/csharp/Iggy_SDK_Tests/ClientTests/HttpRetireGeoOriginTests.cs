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
using Apache.Iggy.IggyClient.Implementations;

namespace Apache.Iggy.Tests.ClientTests;

public sealed class HttpRetireGeoOriginTests
{
    [Theory]
    [InlineData("dc-a", "/geo/origins/dc-a/retire")]
    [InlineData("dc a/b", "/geo/origins/dc%20a%2Fb/retire")]
    public async Task given_an_origin_when_retiring_should_post_an_empty_object_to_its_retire_route(string origin,
        string expectedPath)
    {
        var handler = new RecordingHandler();

        await Client(handler).RetireGeoOriginAsync(origin, TestContext.Current.CancellationToken);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal(expectedPath, request.PathAndQuery);
        Assert.Equal("{}", request.Body);
    }

    [Theory]
    [InlineData(".")]
    [InlineData("..")]
    [InlineData(null)]
    public async Task given_an_origin_no_path_segment_carries_when_retiring_should_throw_without_sending_a_request(
        string? origin)
    {
        var handler = new RecordingHandler();

        await Assert.ThrowsAnyAsync<ArgumentException>(() =>
            Client(handler).RetireGeoOriginAsync(origin!, TestContext.Current.CancellationToken));

        Assert.Empty(handler.Requests);
    }

    private static HttpMessageStream Client(RecordingHandler handler)
    {
        return new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        internal List<(HttpMethod Method, string PathAndQuery, string Body)> Requests { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken ct)
        {
            var body = request.Content is null ? string.Empty : await request.Content.ReadAsStringAsync(ct);
            Requests.Add((request.Method, request.RequestUri!.PathAndQuery, body));
            return new HttpResponseMessage(HttpStatusCode.NoContent) { RequestMessage = request };
        }
    }
}
