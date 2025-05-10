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
