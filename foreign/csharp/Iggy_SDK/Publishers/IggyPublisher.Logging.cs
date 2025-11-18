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

using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Publishers;

public partial class IggyPublisher
{
    // Debug logs
    [LoggerMessage(EventId = 1,
        Level = LogLevel.Debug,
        Message = "Initializing background message sending with queue capacity: {Capacity}, batch size: {BatchSize}")]
    private partial void LogInitializingBackgroundSending(int capacity, int batchSize);

    [LoggerMessage(EventId = 2,
        Level = LogLevel.Debug,
        Message = "Publisher already initialized")]
    private partial void LogPublisherAlreadyInitialized();

    [LoggerMessage(EventId = 3,
        Level = LogLevel.Debug,
        Message = "User {Login} logged in successfully")]
    private partial void LogUserLoggedIn(string login);

    [LoggerMessage(EventId = 4,
        Level = LogLevel.Debug,
        Message = "Stream {StreamId} already exists")]
    private partial void LogStreamAlreadyExists(Identifier streamId);

    [LoggerMessage(EventId = 5,
        Level = LogLevel.Debug,
        Message = "Topic {TopicId} already exists in stream {StreamId}")]
    private partial void LogTopicAlreadyExists(Identifier topicId, Identifier streamId);

    [LoggerMessage(EventId = 7,
        Level = LogLevel.Debug,
        Message = "Successfully sent {Count} messages")]
    private partial void LogSuccessfullySentMessages(int count);

    [LoggerMessage(EventId = 8,
        Level = LogLevel.Debug,
        Message = "Waiting for all pending messages to be sent")]
    private partial void LogWaitingForPendingMessages();

    [LoggerMessage(EventId = 9,
        Level = LogLevel.Debug,
        Message = "All pending messages have been sent")]
    private partial void LogAllPendingMessagesSent();

    [LoggerMessage(EventId = 14,
        Level = LogLevel.Debug,
        Message = "Disposing publisher")]
    private partial void LogDisposingPublisher();

    // Information logs
    [LoggerMessage(EventId = 100,
        Level = LogLevel.Information,
        Message = "Initializing publisher for stream: {StreamId}, topic: {TopicId}")]
    private partial void LogInitializingPublisher(Identifier streamId, Identifier topicId);

    [LoggerMessage(EventId = 101,
        Level = LogLevel.Information,
        Message = "Background message sending started")]
    private partial void LogBackgroundSendingStarted();

    [LoggerMessage(EventId = 102,
        Level = LogLevel.Information,
        Message = "Publisher initialized successfully")]
    private partial void LogPublisherInitialized();

    [LoggerMessage(EventId = 103,
        Level = LogLevel.Information,
        Message = "Creating stream {StreamId} with name: {StreamName}")]
    private partial void LogCreatingStream(Identifier streamId, string streamName);

    [LoggerMessage(EventId = 104,
        Level = LogLevel.Information,
        Message = "Stream {StreamId} created successfully")]
    private partial void LogStreamCreated(Identifier streamId);

    [LoggerMessage(EventId = 105,
        Level = LogLevel.Information,
        Message = "Creating topic {TopicId} with name: {TopicName} in stream {StreamId}")]
    private partial void LogCreatingTopic(Identifier topicId, string topicName, Identifier streamId);

    [LoggerMessage(EventId = 106,
        Level = LogLevel.Information,
        Message = "Topic {TopicId} created successfully in stream {StreamId}")]
    private partial void LogTopicCreated(Identifier topicId, Identifier streamId);


    [LoggerMessage(EventId = 108,
        Level = LogLevel.Information,
        Message = "Publisher disposed")]
    private partial void LogPublisherDisposed();

    // Trace logs
    [LoggerMessage(EventId = 200,
        Level = LogLevel.Trace,
        Message = "Queuing {Count} messages for background sending")]
    private partial void LogQueuingMessages(int count);


    // Error logs
    [LoggerMessage(EventId = 400,
        Level = LogLevel.Error,
        Message = "Stream {StreamId} does not exist and auto-creation is disabled")]
    private partial void LogStreamDoesNotExist(Identifier streamId);

    [LoggerMessage(EventId = 401,
        Level = LogLevel.Error,
        Message = "Topic {TopicId} does not exist in stream {StreamId} and auto-creation is disabled")]
    private partial void LogTopicDoesNotExist(Identifier topicId, Identifier streamId);

    [LoggerMessage(EventId = 402,
        Level = LogLevel.Error,
        Message = "Attempted to send messages before publisher initialization")]
    private partial void LogSendBeforeInitialization();

    [LoggerMessage(EventId = 406,
        Level = LogLevel.Error,
        Message = "Failed to logout or dispose client")]
    private partial void LogFailedToLogoutOrDispose(Exception exception);
}
