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


using Iggy_SDK.Enums;
using Iggy_SDK.Headers;

namespace Iggy_SDK.Contracts.Http;

public sealed class MessageResponse<T>
{
    public required ulong Offset { get; init; }
    public required MessageState State { get; init; }
    public required ulong Timestamp { get; init; }
    public Guid Id { get; init; }
    public required uint Checksum { get; init; }
    public Dictionary<HeaderKey, HeaderValue>? Headers { get; init; }
    public required T Message { get; init; }
}