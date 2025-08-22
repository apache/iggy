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
using System.Text.Json.Serialization;
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;

namespace Apache.Iggy.MessagesDispatcher;

internal sealed class HttpMessageInvoker : IMessageInvoker
{
    private readonly HttpClient _client;
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    public HttpMessageInvoker(HttpClient client)
    {
        _client = client;
        _jsonSerializerOptions = new JsonSerializerOptions
        {
            PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
            Converters = { new JsonStringEnumConverter(JsonNamingPolicy.SnakeCaseLower) }
        };
    }

    public async Task SendMessagesAsync(MessageSendRequest request, CancellationToken token = default)
    {
        var json = JsonSerializer.Serialize(request, _jsonSerializerOptions);
        var data = new StringContent(json, Encoding.UTF8, "application/json");

        var response = await _client.PostAsync($"/streams/{request.StreamId}/topics/{request.TopicId}/messages", data,
            token);
        if (!response.IsSuccessStatusCode)
        {
            await HandleResponseAsync(response);
        }
    }

    private static async Task HandleResponseAsync(HttpResponseMessage response)
    {
        if ((int)response.StatusCode > 300 && (int)response.StatusCode < 500)
        {
            var err = await response.Content.ReadAsStringAsync();
            throw new InvalidResponseException(err);
        }

        if (response.StatusCode == HttpStatusCode.InternalServerError)
        {
            throw new Exception("HTTP Internal server error");
        }

        throw new Exception("Unknown error occurred.");
    }
}