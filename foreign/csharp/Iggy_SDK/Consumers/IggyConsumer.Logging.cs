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
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

public partial class IggyConsumer
{
    [LoggerMessage(EventId = 100,
        Level = LogLevel.Information,
        Message = "Creating consumer group '{GroupName}' for stream {StreamId}, topic {TopicId}")]
    private partial void LogCreatingConsumerGroup(string groupName, Identifier streamId, Identifier topicId);

    [LoggerMessage(EventId = 101,
        Level = LogLevel.Information,
        Message = "Successfully created consumer group '{GroupName}'")]
    private partial void LogConsumerGroupCreated(string groupName);

    [LoggerMessage(EventId = 102,
        Level = LogLevel.Information,
        Message = "Joining consumer group '{GroupName}' for stream {StreamId}, topic {TopicId}")]
    private partial void LogJoiningConsumerGroup(string groupName, Identifier streamId, Identifier topicId);

    [LoggerMessage(EventId = 103,
        Level = LogLevel.Information,
        Message = "Successfully joined consumer group '{GroupName}'")]
    private partial void LogConsumerGroupJoined(string groupName);

    [LoggerMessage(EventId = 200,
        Level = LogLevel.Trace,
        Message = "Left consumer group '{GroupName}'")]
    private partial void LogLeftConsumerGroup(string groupName);

    [LoggerMessage(EventId = 302,
        Level = LogLevel.Warning,
        Message = "Failed to leave consumer group '{GroupName}'")]
    private partial void LogFailedToLeaveConsumerGroup(Exception exception, string groupName);

    [LoggerMessage(EventId = 303,
        Level = LogLevel.Warning,
        Message = "Failed to logout user or dispose client")]
    private partial void LogFailedToLogoutOrDispose(Exception exception);

    [LoggerMessage(EventId = 400,
        Level = LogLevel.Error,
        Message = "Failed to initialize consumer group '{GroupName}'")]
    private partial void LogFailedToInitializeConsumerGroup(Exception exception, string groupName);

    [LoggerMessage(EventId = 401,
        Level = LogLevel.Error,
        Message = "Failed to create consumer group '{GroupName}'")]
    private partial void LogFailedToCreateConsumerGroup(Exception exception, string groupName);

    [LoggerMessage(EventId = 1,
        Level = LogLevel.Debug,
        Message = "Polling task cancelled")]
    private partial void LogPollingTaskCancelled();

    [LoggerMessage(EventId = 2,
        Level = LogLevel.Debug,
        Message = "Message polling stopped")]
    private partial void LogMessagePollingStopped();

    [LoggerMessage(EventId = 3,
        Level = LogLevel.Debug,
        Message = "Consumer group not joined yet. Skipping polling")]
    partial void LogConsumerGroupNotJoinedYetSkippingPolling();

    [LoggerMessage(EventId = 4,
        Level = LogLevel.Debug,
        Message = "Consumer group name is empty. Skipping rejoining consumer group")]
    partial void LogConsumerGroupNameIsEmptySkippingRejoiningConsumerGroup();

    [LoggerMessage(EventId = 402,
        Level = LogLevel.Error,
        Message = "Failed to decrypt message with offset {Offset}")]
    private partial void LogFailedToDecryptMessage(Exception exception, ulong offset);

    [LoggerMessage(EventId = 403,
        Level = LogLevel.Error,
        Message = "Failed to poll messages")]
    private partial void LogFailedToPollMessages(Exception exception);

    [LoggerMessage(EventId = 404,
        Level = LogLevel.Error,
        Message = "Failed to rejoin consumer group '{GroupName}' after reconnection")]
    private partial void LogFailedToRejoinConsumerGroup(Exception exception, string groupName);

    [LoggerMessage(EventId = 105,
        Level = LogLevel.Information,
        Message = "Client connection state changed: {PreviousState} -> {CurrentState}")]
    private partial void LogConnectionStateChanged(ConnectionState previousState, ConnectionState currentState);

    [LoggerMessage(EventId = 300,
        Level = LogLevel.Warning,
        Message = "Returned monotonic time went backwards, now < lastPolledAt: ({Now} < {LastPolledAt})")]
    private partial void LogMonotonicTimeWentBackwards(long now, long lastPolledAt);

    [LoggerMessage(EventId = 201,
        Level = LogLevel.Trace,
        Message = "No need to wait before polling messages. {Now} - {LastPolledAt} = {Elapsed}")]
    private partial void LogNoNeedToWaitBeforePolling(long now, long lastPolledAt, long elapsed);

    [LoggerMessage(EventId = 202,
        Level = LogLevel.Trace,
        Message = "Waiting for {Remaining} milliseconds before polling messages")]
    private partial void LogWaitingBeforePolling(long remaining);

    [LoggerMessage(EventId = 301,
        Level = LogLevel.Warning,
        Message = "PartitionId is ignored when ConsumerType is ConsumerGroup")]
    partial void LogPartitionIdIsIgnoredWhenConsumerTypeIsConsumerGroup();

    [LoggerMessage(EventId = 106,
        Level = LogLevel.Information,
        Message = "Rejoining consumer group {ConsumerGroupName} after reconnection")]
    partial void LogRejoiningConsumerGroupConsumerGroupNameAfterReconnection(string consumerGroupName);
}
