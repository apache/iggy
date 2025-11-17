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

using Apache.Iggy.Kinds;

namespace Apache.Iggy.Contracts.Auth;

/// <summary>
///     Global permissions applied to all streams.
/// </summary>
public sealed class GlobalPermissions
{
    /// <summary>
    ///     Permission allows to manage the servers and includes all the permissions of <see cref="ReadServers" />.
    /// </summary>
    public required bool ManageServers { get; init; }

    /// <summary>
    ///     Permission allows to read server information.
    ///     <para>Allowed methods:</para>
    ///     <list type="bullet">
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggySystem.GetStatsAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggySystem.GetClientsAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggySystem.GetClientByIdAsync" />
    ///         </item>
    ///     </list>
    /// </summary>
    public required bool ReadServers { get; init; }

    /// <summary>
    ///     Permission allows to manage users and includes all the permissions of <see cref="ReadUsers" />.
    ///     <para>Additionally, allowed methods:</para>
    ///     <list type="bullet">
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyUsers.CreateUser" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyUsers.DeleteUser" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyUsers.UpdateUser" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyUsers.UpdatePermissions" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyUsers.ChangePassword" />
    ///         </item>
    ///     </list>
    /// </summary>
    public required bool ManageUsers { get; init; }

    /// <summary>
    ///     Permission allows to read user information.
    ///     <para>Allowed methods:</para>
    ///     <list type="bullet">
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyUsers.GetUser" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyUsers.GetUsers" />
    ///         </item>
    ///     </list>
    /// </summary>
    public required bool ReadUsers { get; init; }

    /// <summary>
    ///     Permission allows to manage streams and includes all the permissions of <see cref="ReadStreams" /> and
    ///     <see cref="ManageTopics" />.
    ///     <para>Additionally, allowed methods:</para>
    ///     <list type="bullet">
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyStream.CreateStreamAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyStream.UpdateStreamAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyStream.DeleteStreamAsync" />
    ///         </item>
    ///     </list>
    /// </summary>
    public required bool ManageStreams { get; init; }

    /// <summary>
    ///     Permission allows to read streams and includes all the permissions of <see cref="ReadTopics" />.
    ///     <para>Additionally, allowed methods:</para>
    ///     <list type="bullet">
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyStream.GetStreamByIdAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyStream.GetStreamsAsync" />
    ///         </item>
    ///     </list>
    /// </summary>
    public required bool ReadStreams { get; init; }

    /// <summary>
    ///     Permission allows to manage topics and includes all the permissions of <see cref="ReadTopics" />.
    ///     <para>Additionally, allowed methods:</para>
    ///     <list type="bullet">
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyTopic.CreateTopicAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyTopic.UpdateTopicAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyTopic.DeleteTopicAsync" />
    ///         </item>
    ///     </list>
    /// </summary>
    public required bool ManageTopics { get; init; }

    /// <summary>
    ///     Permission allows to read topics and manage consumer groups. Includes all the permissions of
    ///     <see cref="PollMessages" />.
    ///     <para>Allowed methods:</para>
    ///     <list type="bullet">
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyTopic.GetTopicByIdAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyTopic.GetTopicsAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyConsumerGroup.GetConsumerGroupsAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyConsumerGroup.GetConsumerGroupByIdAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyConsumerGroup.CreateConsumerGroupAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyConsumerGroup.DeleteConsumerGroupAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyConsumerGroup.JoinConsumerGroupAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyConsumerGroup.LeaveConsumerGroupAsync" />
    ///         </item>
    ///     </list>
    /// </summary>
    public required bool ReadTopics { get; init; }

    /// <summary>
    ///     Permission allows to poll messages and manage consumer offsets.
    ///     <para>Allowed methods:</para>
    ///     <list type="bullet">
    ///         <item>
    ///             <see
    ///                 cref="Apache.Iggy.IggyClient.IIggyConsumer.PollMessagesAsync(Identifier, Identifier, uint?, Consumer, PollingStrategy, uint, bool,CancellationToken)" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyOffset.GetOffsetAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyOffset.StoreOffsetAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyOffset.DeleteOffsetAsync" />
    ///         </item>
    ///     </list>
    /// </summary>
    public required bool PollMessages { get; init; }

    /// <summary>
    ///     Permission allows to send messages.
    ///     <para>Allowed methods:</para>
    ///     <list type="bullet">
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyPublisher.SendMessagesAsync" />
    ///         </item>
    ///         <item>
    ///             <see cref="Apache.Iggy.IggyClient.IIggyPublisher.FlushUnsavedBufferAsync" />
    ///         </item>
    ///     </list>
    /// </summary>
    public required bool SendMessages { get; init; }
}
