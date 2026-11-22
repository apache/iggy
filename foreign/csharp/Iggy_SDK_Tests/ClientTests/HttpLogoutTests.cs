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
using System.Net.Http.Headers;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;

namespace Apache.Iggy.Tests.ClientTests;

public sealed class HttpLogoutTests
{
    private const string Unauthenticated =
        "{\"id\":40,\"code\":\"unauthenticated\",\"reason\":\"Unauthenticated\",\"field\":null}";

    private const string ServerError = "{\"id\":1,\"code\":\"error\",\"reason\":\"Error\",\"field\":null}";

    [Fact]
    public async Task given_a_refused_token_when_logging_out_should_succeed_and_drop_it()
    {
        var http = SignedIn(HttpStatusCode.Unauthorized, Unauthenticated);

        await new HttpMessageStream(http).LogoutUserAsync(TestContext.Current.CancellationToken);

        Assert.Null(http.DefaultRequestHeaders.Authorization);
    }

    [Theory]
    [InlineData("<html><body>401 Authorization Required</body></html>")]
    [InlineData("{\"id\":42,\"code\":\"unauthorized\",\"reason\":\"Unauthorized\",\"field\":null}")]
    public async Task given_a_refusal_that_is_not_unauthenticated_when_logging_out_should_surface_it_and_keep_the_token(
        string body)
    {
        var http = SignedIn(HttpStatusCode.Unauthorized, body);

        await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            new HttpMessageStream(http).LogoutUserAsync(TestContext.Current.CancellationToken));

        Assert.Equal("token", http.DefaultRequestHeaders.Authorization?.Parameter);
    }

    [Fact]
    public async Task given_no_token_when_logging_out_should_surface_the_refusal()
    {
        var http = SignedIn(HttpStatusCode.Unauthorized, Unauthenticated);
        http.DefaultRequestHeaders.Authorization = null;

        await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            new HttpMessageStream(http).LogoutUserAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task given_a_server_failure_when_logging_out_should_keep_the_token()
    {
        var http = SignedIn(HttpStatusCode.InternalServerError, ServerError);

        await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() =>
            new HttpMessageStream(http).LogoutUserAsync(TestContext.Current.CancellationToken));

        Assert.Equal("token", http.DefaultRequestHeaders.Authorization?.Parameter);
    }

    private static HttpClient SignedIn(HttpStatusCode logoutStatus, string logoutBody)
    {
        return new HttpClient(new StatusHandler(logoutStatus, logoutBody))
        {
            BaseAddress = new Uri("http://localhost"),
            DefaultRequestHeaders = { Authorization = new AuthenticationHeaderValue("Bearer", "token") }
        };
    }

    private sealed class StatusHandler(HttpStatusCode status, string body) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            return Task.FromResult(new HttpResponseMessage(status)
            {
                RequestMessage = request,
                Content = new StringContent(body)
            });
        }
    }
}
