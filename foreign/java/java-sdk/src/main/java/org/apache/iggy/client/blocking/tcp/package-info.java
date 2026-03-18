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
 * Blocking TCP client implementation for Apache Iggy.
 *
 * <p>This package provides blocking (synchronous) wrappers over the
 * {@link org.apache.iggy.client.async.tcp async TCP client} implementation.
 * Each blocking sub-client delegates to its async counterpart and unwraps
 * the {@link java.util.concurrent.CompletableFuture} result via
 * {@code FutureUtil.resolve()}.
 */
@NonNullApi
package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.NonNullApi;
