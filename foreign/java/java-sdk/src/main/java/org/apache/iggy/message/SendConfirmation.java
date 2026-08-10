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

package org.apache.iggy.message;

import java.math.BigInteger;

/**
 * Confirmation of a committed send: the partition the batch landed on and the
 * offset of its first message.
 *
 * @param streamId    the numeric stream ID the batch was written to
 * @param topicId     the numeric topic ID the batch was written to
 * @param partitionId the partition the batch landed on
 * @param baseOffset  the offset assigned to the first message of the batch
 */
public record SendConfirmation(Long streamId, Long topicId, Long partitionId, BigInteger baseOffset) {}
