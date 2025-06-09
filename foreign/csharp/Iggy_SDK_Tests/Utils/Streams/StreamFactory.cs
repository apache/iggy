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

using Apache.Iggy.Contracts.Http;

namespace Apache.Iggy.Tests.Utils.Streams;

internal static class StreamFactory
{
    internal static (int id, int topicsCount, ulong sizeBytes, ulong messagesCount, string name, ulong createdAt)
        CreateStreamsResponseFields()
    {
        int id = Random.Shared.Next(1, 69);
        var topicsCount = Random.Shared.Next(1, 69);
        var sizeBytes = (ulong)Random.Shared.Next(69, 42069);
        var messageCount = (ulong)Random.Shared.Next(2, 3);
        var name = "Stream " + Random.Shared.Next(1, 4) + Utility.RandomString(3).ToLower();
        var createdAt = (ulong)Random.Shared.Next(69, 42069);
        return (id, topicsCount, sizeBytes, messageCount, name, createdAt);
    }
    internal static StreamRequest CreateStreamRequest()
    {
        return new StreamRequest
        {
            Name = "test-stream" + Random.Shared.Next(1, 69) + Utility.RandomString(12).ToLower(),
            StreamId = Random.Shared.Next(1, 2137),
        };
    }

    internal static UpdateStreamRequest CreateUpdateStreamRequest()
    {
        return new UpdateStreamRequest { Name = "updated-stream" + Random.Shared.Next(1, 69) };
    }
    internal static StreamResponse CreateStreamsResponse()
    {
        return new StreamResponse
        {
            Id = Random.Shared.Next(1, 10),
            Size = (ulong)Random.Shared.Next(1, 10),
            MessagesCount = (ulong)Random.Shared.Next(1, 10),
            CreatedAt = DateTimeOffset.UtcNow,
            Name = "Test Topic" + Random.Shared.Next(1, 69),
            TopicsCount = Random.Shared.Next(1, 10),
        };
    }
}