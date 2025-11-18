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

using Apache.Iggy.Headers;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Response from the server containing a message payload.
/// </summary>
public sealed class MessageResponse
{
    /// <summary>
    ///     Message header.
    /// </summary>
    public required MessageHeader Header { get; set; }

    /// <summary>
    ///     Message payload.
    /// </summary>
    public required byte[] Payload { get; set; } = [];

    /// <summary>
    ///     Headers defined by the user.
    /// </summary>
    public Dictionary<HeaderKey, HeaderValue>? UserHeaders { get; init; }
}
