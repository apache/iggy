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

package org.apache.iggy.system;

import java.math.BigInteger;

public record Stats(
        Long processId,
        Float cpuUsage,
        Float totalCpuUsage,
        String memoryUsage,
        String totalMemory,
        String availableMemory,
        BigInteger runTime,
        BigInteger startTime,
        String readBytes,
        String writtenBytes,
        String messagesSizeBytes,
        Long streamsCount,
        Long topicsCount,
        Long partitionsCount,
        Long segmentsCount,
        BigInteger messagesCount,
        Long clientsCount,
        Long consumerGroupsCount,
        String hostname,
        String osName,
        String osVersion,
        String kernelVersion) {}
