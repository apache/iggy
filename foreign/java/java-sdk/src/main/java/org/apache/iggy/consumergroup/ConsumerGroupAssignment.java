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

package org.apache.iggy.consumergroup;

import java.util.List;

/**
 * A consumer-group member's current partition assignment and the group
 * generation it belongs to, as returned by the sync-consumer-group command.
 *
 * <p>The generation advances on every rebalance; a member holding an
 * assignment from an older generation is fenced by the server when it polls.
 * An assignment with no partitions still means the client is a group member,
 * just one that currently owns nothing.
 *
 * @param generation the group generation this assignment belongs to
 * @param partitions the partition IDs this member may poll, possibly empty
 */
public record ConsumerGroupAssignment(long generation, List<Long> partitions) {}
