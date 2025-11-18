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

import org.apache.iggy.identifier.ConsumerId;

public record Consumer(Kind kind, ConsumerId id) {

    public static Consumer of(Long id) {
        return new Consumer(Kind.Consumer, ConsumerId.of(id));
    }

    public static Consumer of(ConsumerId id) {
        return new Consumer(Kind.Consumer, id);
    }

    public static Consumer group(Long id) {
        return new Consumer(Kind.ConsumerGroup, ConsumerId.of(id));
    }

    public static Consumer group(ConsumerId id) {
        return new Consumer(Kind.ConsumerGroup, id);
    }

    public enum Kind {
        Consumer(1),
        ConsumerGroup(2);

        private final int code;

        Kind(int code) {
            this.code = code;
        }

        public int asCode() {
            return code;
        }
    }
}
