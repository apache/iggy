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

using System.Text.Json.Serialization;
using Apache.Iggy.JsonConverters;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Contracts.Http;

[JsonConverter(typeof(MessagesConverter))]
internal sealed class MessageSendRequest
{
    public required Identifier StreamId { get; init; }
    public required Identifier TopicId { get; init; }
    public required Partitioning Partitioning { get; init; }
    public required IList<Message> Messages { get; init; }
}
