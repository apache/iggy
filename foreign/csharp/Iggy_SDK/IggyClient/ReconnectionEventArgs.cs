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

namespace Apache.Iggy.IggyClient;

/// <summary>
///     Event arguments for client connection state changes.
///     Fired for every connection state transition from the ConnectionState enum.
/// </summary>
public class ConnectionStateChangedEventArgs : EventArgs
{
    /// <summary>
    ///     Gets the previous connection state
    /// </summary>
    public ConnectionState PreviousState { get; }

    /// <summary>
    ///     Gets the current connection state
    /// </summary>
    public ConnectionState CurrentState { get; }

    /// <summary>
    ///     Gets the timestamp when the state change occurred
    /// </summary>
    public DateTimeOffset Timestamp { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConnectionStateChangedEventArgs" /> class
    /// </summary>
    /// <param name="previousState">The previous connection state</param>
    /// <param name="currentState">The current connection state</param>
    public ConnectionStateChangedEventArgs(ConnectionState previousState, ConnectionState currentState)
    {
        PreviousState = previousState;
        CurrentState = currentState;
        Timestamp = DateTimeOffset.UtcNow;
    }
}
