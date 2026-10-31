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
import java.util.Optional;

/**
 * A record of a reply that does not decode.
 *
 * @param error  why the record does not decode
 * @param offset the offset of the record, known when its frame decoded and a later check failed. Empty when the
 *               framing is malformed, so no record can be isolated.
 * @param record the record as the reply carried it, the frame header, the payload and the user headers. For
 *               malformed framing, the remaining bytes of the reply from where decoding stopped.
 */
public record DecodeFailure(RuntimeException error, Optional<BigInteger> offset, byte[] record) {}
