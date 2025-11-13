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

using Apache.Iggy.Messages;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Event arguments for failed message batch send events.
/// </summary>
public class MessageBatchFailedEventArgs : EventArgs
{
    /// <summary>
    ///     Gets the exception that caused the batch to fail.
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    ///     Gets the array of messages that failed to send.
    /// </summary>
    public Message[] FailedMessages { get; }

    /// <summary>
    ///     Gets the UTC timestamp when the failure occurred.
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    ///     Gets the number of retry attempts that were made before failing.
    /// </summary>
    public int AttemptedRetries { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MessageBatchFailedEventArgs" /> class.
    /// </summary>
    /// <param name="exception">The exception that caused the batch to fail.</param>
    /// <param name="failedMessages">The array of messages that failed to send.</param>
    /// <param name="attemptedRetries">The number of retry attempts that were made.</param>
    public MessageBatchFailedEventArgs(Exception exception, Message[] failedMessages, int attemptedRetries = 0)
    {
        Exception = exception;
        FailedMessages = failedMessages;
        Timestamp = DateTime.UtcNow;
        AttemptedRetries = attemptedRetries;
    }
}
