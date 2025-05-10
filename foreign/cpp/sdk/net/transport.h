// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once

namespace icp {
namespace net {
namespace transport {

/**
 * @brief Available network transports in the client library.
 */
enum Transport {
    /**
     * @brief Modern networking protocol from Google built on top of UDP.
     *
     * @ref [Wikipedia](https://en.wikipedia.org/wiki/QUIC)
     */
    QUIC,

    /**
     * @brief Classic HTTP REST encoded as JSON. Not recommended for high performance applications.
     */
    HTTP,

    /**
     * @brief Binary protocol over TCP/IP.
     */
    TCP
};

};  // namespace transport
};  // namespace net
};  // namespace icp
