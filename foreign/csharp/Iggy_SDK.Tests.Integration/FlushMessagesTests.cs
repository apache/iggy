// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Tests.Integrations.Attributes;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Apache.Iggy.Tests.Integrations.Helpers;
using Shouldly;

namespace Apache.Iggy.Tests.Integrations;

public class FlushMessagesTests
{
    [ClassDataSource<FlushMessageFixture>(Shared = SharedType.PerClass)]
    public required FlushMessageFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task FlushUnsavedBuffer_WithFsync_Should_Flush_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(() =>
            Fixture.Clients[protocol].FlushUnsavedBufferAsync(
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), 0, true));
    }

    [Test]
    [DependsOn(nameof(FlushUnsavedBuffer_WithFsync_Should_Flush_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task FlushUnsavedBuffer_WithOutFsync_Should_Flush_Successfully(Protocol protocol)
    {
        await Should.NotThrowAsync(() =>
            Fixture.Clients[protocol].FlushUnsavedBufferAsync(
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), 0, false));
    }

    [Test]
    [DependsOn(nameof(FlushUnsavedBuffer_WithOutFsync_Should_Flush_Successfully))]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task FlushUnsavedBuffer_Should_Throw_WhenStream_DoesNotExist(Protocol protocol)
    {
        await Should.ThrowAsync<IggyInvalidStatusCodeException>(() =>
            Fixture.Clients[protocol].FlushUnsavedBufferAsync(
                Identifier.String(Fixture.StreamId.GetWithProtocol(protocol)),
                Identifier.String(Fixture.TopicRequest.Name), 55, false));
    }
}
