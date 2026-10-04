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

using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Iggy_SDK.Examples.Shared;
using Moq;

namespace Apache.Iggy.Tests.ClientTests;

public sealed class ExampleHelpersTests
{
    private const int StreamNameAlreadyExists = 1012;
    private const int TopicNameAlreadyExists = 2013;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task EnsureResourceExists_WhenCreatedConcurrently_ShouldSucceed(bool topic)
    {
        var error = new IggyInvalidStatusCodeException(
            topic ? TopicNameAlreadyExists : StreamNameAlreadyExists, "Created by another client", true);

        await EnsureResourceExists(topic, error);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task EnsureResourceExists_WhenFailureIsNotAServerNameConflict_ShouldPropagate(bool topic, bool fromServer)
    {
        var matchingCode = topic ? TopicNameAlreadyExists : StreamNameAlreadyExists;
        var otherCode = topic ? StreamNameAlreadyExists : TopicNameAlreadyExists;
        var error = new IggyInvalidStatusCodeException(
            fromServer ? otherCode : matchingCode, "Creation failed", fromServer);

        var actual = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => EnsureResourceExists(topic, error));

        Assert.Same(error, actual);
    }

    private static async Task EnsureResourceExists(bool topic, IggyInvalidStatusCodeException error)
    {
        var client = new Mock<IIggyClient>();
        var streamId = Identifier.String("example-stream");
        var topicId = Identifier.String("example-topic");
        var token = TestContext.Current.CancellationToken;
        if (topic)
        {
            client.Setup(value => value.CreateTopicAsync(streamId, topicId.GetString(), 1,
                    CompressionAlgorithm.None, null, 0, null, token))
                .ThrowsAsync(error);
            await ExampleHelpers.EnsureTopicExists(client.Object, streamId, topicId, topicId.GetString(), 1, token);
        }
        else
        {
            client.Setup(value => value.CreateStreamAsync(streamId.GetString(), token))
                .ThrowsAsync(error);
            await ExampleHelpers.EnsureStreamExists(client.Object, streamId, streamId.GetString(), token);
        }
        client.VerifyAll();
    }
}
