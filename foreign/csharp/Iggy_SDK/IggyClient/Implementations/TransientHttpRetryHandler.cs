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

namespace Apache.Iggy.IggyClient.Implementations;

/// <summary>
///     Replays requests the server answered with a retryable status: 503 (no caught-up primary, full
///     pipeline, view-change cancel, shard write budget) and 429 (session write cap). The server signals
///     both as transient with a Retry-After; the binary transports absorb the equivalent frames in their
///     in-client replay loop, so HTTP replays here for transport parity. A write 504 stays terminal: the
///     request may still commit, so its outcome is unknown and must surface to the caller.
/// </summary>
internal sealed class TransientHttpRetryHandler : DelegatingHandler
{
    private static readonly TimeSpan RetryDeadline = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan RetryInterval = TimeSpan.FromMilliseconds(50);

    public TransientHttpRetryHandler(HttpMessageHandler innerHandler) : base(innerHandler)
    {
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
        CancellationToken cancellationToken)
    {
        var deadline = Environment.TickCount64 + (long)RetryDeadline.TotalMilliseconds;
        byte[]? bufferedContent = null;
        if (request.Content is not null)
        {
            bufferedContent = await request.Content.ReadAsByteArrayAsync(cancellationToken);
        }

        while (true)
        {
            HttpResponseMessage response = await base.SendAsync(CloneRequest(request, bufferedContent),
                cancellationToken);
            if (!IsRetryable(request.Method, response.StatusCode) || Environment.TickCount64 >= deadline)
            {
                return response;
            }

            var delay = response.Headers.RetryAfter?.Delta ?? RetryInterval;
            response.Dispose();
            await Task.Delay(delay, cancellationToken);
        }
    }

    private static bool IsRetryable(HttpMethod method, HttpStatusCode statusCode)
    {
        if (statusCode is HttpStatusCode.ServiceUnavailable or HttpStatusCode.TooManyRequests)
        {
            return true;
        }

        // A read that timed out in the partition plane (e.g. a poll racing the reconciler that has not yet
        // materialised a fresh topic's partition group) has no side effects, so it replays. A write 504 is an
        // unknown outcome and stays terminal.
        return statusCode == HttpStatusCode.GatewayTimeout && method == HttpMethod.Get;
    }

    /// <summary>
    ///     An <see cref="HttpRequestMessage" /> is single-use, so every attempt sends a copy built from the
    ///     buffered body.
    /// </summary>
    private static HttpRequestMessage CloneRequest(HttpRequestMessage request, byte[]? bufferedContent)
    {
        var clone = new HttpRequestMessage(request.Method, request.RequestUri)
        {
            Version = request.Version,
            VersionPolicy = request.VersionPolicy
        };

        foreach (var header in request.Headers)
        {
            clone.Headers.TryAddWithoutValidation(header.Key, header.Value);
        }

        if (bufferedContent is not null)
        {
            clone.Content = new ByteArrayContent(bufferedContent);
            foreach (var header in request.Content!.Headers)
            {
                clone.Content.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }
        }

        foreach (var option in request.Options)
        {
            clone.Options.Set(new HttpRequestOptionsKey<object?>(option.Key), option.Value);
        }

        return clone;
    }
}
