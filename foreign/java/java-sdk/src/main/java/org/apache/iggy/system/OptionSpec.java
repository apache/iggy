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

import org.apache.iggy.message.HeaderValue;

/**
 * One entry of a resource's option catalog.
 *
 * <p>This is the discovery surface for the option keys a create command accepts: a key outside the
 * catalog is refused at create, and the binary transports carry only the error code back, so there
 * is no other way to learn which keys a given server knows.
 *
 * @param key the option key the create command accepts
 * @param defaultValue the key's default, carrying the kind the server encodes it under, or empty for
 *     a key with no default. Reuses {@link HeaderValue} because options ride the user-headers codec.
 * @param description what the option does, including the bounds its value is checked against
 */
public record OptionSpec(String key, HeaderValue defaultValue, String description) {}
