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


namespace Apache.Iggy.Contracts.Http;

public sealed class StreamResponse
{
    public required int Id { get; init; }
    public required string Name { get; init; }
    public required ulong Size { get; init; }
    public required DateTimeOffset CreatedAt { get; init; }
    public required ulong MessagesCount { get; init; }
    public required int TopicsCount { get; init; }
    public IEnumerable<TopicResponse> Topics { get; init; } = [];
}