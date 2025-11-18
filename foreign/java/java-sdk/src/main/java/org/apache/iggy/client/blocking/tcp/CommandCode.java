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

package org.apache.iggy.client.blocking.tcp;

public interface CommandCode {

    int getValue();

    enum System implements CommandCode {
        PING(1),
        GET_STATS(10),
        GET_ME(20),
        GET_CLIENT(21),
        GET_ALL_CLIENTS(22);

        private final int value;

        System(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }
    }

    enum User implements CommandCode {
        GET(31),
        GET_ALL(32),
        CREATE(33),
        DELETE(34),
        UPDATE(35),
        UPDATE_PERMISSIONS(36),
        CHANGE_PASSWORD(37),
        LOGIN(38),
        LOGOUT(39);

        private final int value;

        User(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }
    }

    enum PersonalAccessToken implements CommandCode {
        GET_ALL(41),
        CREATE(42),
        DELETE(43),
        LOGIN(44);

        private final int value;

        PersonalAccessToken(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }
    }

    enum Messages implements CommandCode {
        POLL(100),
        SEND(101);

        private final int value;

        Messages(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }
    }

    enum ConsumerOffset implements CommandCode {
        GET(120),
        STORE(121);

        private final int value;

        ConsumerOffset(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }
    }

    enum Stream implements CommandCode {
        GET(200),
        GET_ALL(201),
        CREATE(202),
        DELETE(203),
        UPDATE(204);

        private final int value;

        Stream(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }
    }

    enum Topic implements CommandCode {
        GET(300),
        GET_ALL(301),
        CREATE(302),
        DELETE(303),
        UPDATE(304),
        PURGE(305);

        private final int value;

        Topic(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }
    }

    enum Partition implements CommandCode {
        CREATE(402),
        DELETE(403);

        private final int value;

        Partition(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }
    }

    enum ConsumerGroup implements CommandCode {
        GET(600),
        GET_ALL(601),
        CREATE(602),
        DELETE(603),
        JOIN(604),
        LEAVE(605);

        private final int value;

        ConsumerGroup(int value) {
            this.value = value;
        }

        @Override
        public int getValue() {
            return value;
        }
    }
}
