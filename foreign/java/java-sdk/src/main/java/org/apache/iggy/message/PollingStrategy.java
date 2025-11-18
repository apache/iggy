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

public record PollingStrategy(PollingKind kind, BigInteger value) {

    public static PollingStrategy offset(BigInteger value) {
        return new PollingStrategy(PollingKind.Offset, value);
    }

    public static PollingStrategy timestamp(BigInteger value) {
        return new PollingStrategy(PollingKind.Timestamp, value);
    }

    public static PollingStrategy first() {
        return new PollingStrategy(PollingKind.First, BigInteger.ZERO);
    }

    public static PollingStrategy last() {
        return new PollingStrategy(PollingKind.Last, BigInteger.ZERO);
    }

    public static PollingStrategy next() {
        return new PollingStrategy(PollingKind.Next, BigInteger.ZERO);
    }
}
