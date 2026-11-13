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

public sealed class HttpPersonalAccessTokenPathTests
{
    [Theory]
    [InlineData("my-token", "/personal-access-tokens/my-token")]
    [InlineData("../users/42", "/personal-access-tokens/..%2Fusers%2F42")]
    [InlineData("a/b", "/personal-access-tokens/a%2Fb")]
    [InlineData("a?b=c", "/personal-access-tokens/a%3Fb%3Dc")]
    [InlineData("a#b", "/personal-access-tokens/a%23b")]
    [InlineData("my token", "/personal-access-tokens/my%20token")]
    [InlineData("a+b", "/personal-access-tokens/a%2Bb")]
    [InlineData("a%2Fb", "/personal-access-tokens/a%252Fb")]
    public async Task given_a_token_name_when_deleting_should_send_it_as_one_escaped_path_segment(string name,
        string expectedPath)
    {
        var handler = new RecordingHandler();

        await Client(handler).DeletePersonalAccessTokenAsync(name, TestContext.Current.CancellationToken);

        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Delete, request.Method);
        Assert.Equal(expectedPath, request.PathAndQuery);
    }

    [Theory]
    [InlineData(".")]
    [InlineData("..")]
    public async Task given_a_dot_segment_name_when_deleting_should_throw_without_sending_a_request(string name)
    {
        var handler = new RecordingHandler();

        await Assert.ThrowsAsync<ArgumentException>(() =>
            Client(handler).DeletePersonalAccessTokenAsync(name, TestContext.Current.CancellationToken));

        Assert.Empty(handler.Requests);
    }

    private static HttpMessageStream Client(RecordingHandler handler)
    {
        return new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
    }

    private sealed class RecordingHandler : HttpMessageHandler
    {
        internal List<(HttpMethod Method, string PathAndQuery)> Requests { get; } = [];

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add((request.Method, request.RequestUri!.PathAndQuery));
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent) { RequestMessage = request });
        }
    }
}
