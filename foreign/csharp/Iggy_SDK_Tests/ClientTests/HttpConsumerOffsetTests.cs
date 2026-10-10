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
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;

namespace Apache.Iggy.Tests.ClientTests;

public sealed class HttpConsumerOffsetTests
{
    [Fact]
    public async Task given_a_polled_context_when_storing_an_offset_should_send_it()
    {
        var handler = new HttpTopicOptionsTests.StubHandler(string.Empty);
        var client = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        await client.StoreOffsetAsync(Consumer.New(1), Identifier.Numeric(1), Identifier.Numeric(2), 100, 3,
            new PartitionContext(17, 9, 52), TestContext.Current.CancellationToken);

        Assert.Contains("\"context\":{\"incarnation\":17,\"owner_generation\":9,\"metadata_op\":52}",
            handler.RequestBody);
    }
}
