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
