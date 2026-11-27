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

package org.apache.iggy.client.async.reader;

import org.apache.iggy.message.Checkpoint;
import org.apache.iggy.message.Message;

/**
 * One message that an {@link IggyConsumer} handed over.
 *
 * @param partitionId the partition of the message
 * @param message     the message
 * @param checkpoint  the position after the message, for a commit or a mark
 * @param endsBatch   whether it is the last message of the batch that the reader read
 */
public record ReceivedMessage(long partitionId, Message message, Checkpoint checkpoint, boolean endsBatch) {}
