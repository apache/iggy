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

using Apache.Iggy.Encryption;

namespace Apache.Iggy.IggyClient;

/// <summary>
///     Defines the primary client interface for interacting with an Iggy message streaming platform.
///     Combines all functional interfaces including publishing, streaming, consuming, user management, and system
///     operations.
/// </summary>
public interface IIggyClient : IIggyPublisher, IIggyStream, IIggyTopic, IIggyConsumer, IIggyOffset, IIggyConsumerGroup,
    IIggySystem, IIggyPartition, IIggySegment, IIggyUsers, IIggyPersonalAccessToken, IDisposable
{
    /// <summary>
    ///     The message encryptor configured on this client, or null when encryption is disabled. When set, the client
    ///     encrypts on send and decrypts on poll for the whole connection. Defaults to null so existing
    ///     implementations keep compiling.
    /// </summary>
    IMessageEncryptor? MessageEncryptor => null;

    /// <summary>
    ///     Subscribes to connection state change events.
    /// </summary>
    /// <remarks>
    ///     The callback will be invoked whenever the connection state changes (connected, disconnected, reconnecting, etc.).
    ///     Multiple callbacks can be registered for the same event.
    /// </remarks>
    /// <param name="callback">The method to be invoked when a connection state change occurs.</param>
    void SubscribeConnectionEvents(Func<ConnectionStateChangedEventArgs, Task> callback);

    /// <summary>
    ///     Unsubscribes from connection state change events.
    /// </summary>
    /// <remarks>
    ///     Removes a previously registered callback from connection state change notifications.
    /// </remarks>
    /// <param name="callback">The method previously registered for connection event notifications to be removed.</param>
    void UnsubscribeConnectionEvents(Func<ConnectionStateChangedEventArgs, Task> callback);

    /// <summary>
    ///     Gets the current address of the client.
    /// </summary>
    /// <returns>The current address of the client.</returns>
    string GetCurrentAddress();

    /// <summary>
    ///     Sends a command code with a payload and returns the raw response bytes.
    /// </summary>
    /// <remarks>
    ///     Session-control codes are rejected with an invalid-command error.
    ///     HTTP clients report that this operation is unavailable.
    /// </remarks>
    /// <param name="code">The numeric command code to send.</param>
    /// <param name="payload">The request payload.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    /// <returns>A task that represents the asynchronous operation and returns the raw response payload bytes.</returns>
    Task<byte[]> SendRawWithResponseAsync(uint code, byte[] payload, CancellationToken token = default);
}
