/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Async client interfaces for Apache Iggy message streaming.
 *
 * <p>This package defines the async client API where all operations return
 * {@link java.util.concurrent.CompletableFuture} for non-blocking execution.
 * The interfaces decouple the API contract from transport-specific implementations
 * (see {@link org.apache.iggy.client.async.tcp} for the TCP/Netty implementation).
 *
 * <h2>Core Interfaces</h2>
 * <ul>
 *   <li>{@link org.apache.iggy.client.async.MessagesClient} — send and poll messages</li>
 *   <li>{@link org.apache.iggy.client.async.StreamsClient} — manage streams</li>
 *   <li>{@link org.apache.iggy.client.async.TopicsClient} — manage topics</li>
 *   <li>{@link org.apache.iggy.client.async.UsersClient} — authentication</li>
 *   <li>{@link org.apache.iggy.client.async.ConsumerGroupsClient} — consumer group membership</li>
 * </ul>
 *
 * <h2>Getting Started</h2>
 * <pre>{@code
 * var client = AsyncIggyTcpClient.builder()
 *     .host("localhost")
 *     .port(8090)
 *     .credentials("iggy", "iggy")
 *     .buildAndLogin()
 *     .join();
 *
 * client.messages().sendMessages(
 *     StreamId.of(1L), TopicId.of(1L),
 *     Partitioning.balanced(),
 *     List.of(Message.of("hello")))
 *     .join();
 *
 * client.close().join();
 * }</pre>
 *
 * @see org.apache.iggy.client.async.tcp.AsyncIggyTcpClient
 */
package org.apache.iggy.client.async;
