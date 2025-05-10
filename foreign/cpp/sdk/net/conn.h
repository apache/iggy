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

#include <libwebsockets.h>

namespace icp {
namespace net {

class Connection;

namespace internal {

namespace lws {

class Context {
private:
    lws_context_creation_info info;
    lws_context* context;

    friend class icp::net::Connection;

public:
    Context() = default;
};

class Logger {
private:
    lws_log_cx_t context;

public:
    Logger();
};

};  // namespace lws

};  // namespace internal

/**
 * @brief Base class for all network connections.
 *
 * Shared base for HTTP, raw TCP and QUIC connections. It relies on the @ref icp::net::ssl
 * for low-level set up of cryptography and SSL/TLS context, currently built on top
 * of wolfSSL, and LWS for all low-level networking protocol support.
 *
 */
class Connection {
private:
    icp::net::internal::lws::Context context;
};

};  // namespace net
};  // namespace icp
