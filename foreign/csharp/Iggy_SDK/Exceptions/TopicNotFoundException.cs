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

namespace Apache.Iggy.Exceptions;

/// <summary>
///     Thrown when a topic is not found in a stream.
/// </summary>
public sealed class TopicNotFoundException : Exception
{
    /// <summary>
    ///     Topic identifier that was not found.
    /// </summary>
    public Identifier TopicId { get; }

    /// <summary>
    ///     Stream identifier where the topic was not found.
    /// </summary>
    public Identifier StreamId { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="TopicNotFoundException" /> class.
    /// </summary>
    public TopicNotFoundException(Identifier topicId, Identifier streamId)
        : base($"Topic {topicId} does not exist in stream {streamId} and auto-creation is disabled")
    {
        TopicId = topicId;
        StreamId = streamId;
    }
}
