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

namespace Apache.Iggy.Enums;

//By offset (using the indexes)
//By timestamp (using the time indexes)
//First/Last N messages
//Next N messages for the specific consumer

/// <summary>
///     Polling strategy for fetching messages from a topic.
/// </summary>
public enum MessagePolling
{
    /// <summary>
    ///     By offset (using the indexes).
    /// </summary>
    Offset,

    /// <summary>
    ///     By timestamp (using the time indexes).
    /// </summary>
    Timestamp,

    /// <summary>
    ///     Get first N messages.
    /// </summary>
    First,

    /// <summary>
    ///     Get last N messages.
    /// </summary>
    Last,

    /// <summary>
    ///     Get next N messages for the specific consumer.
    /// </summary>
    Next
}
