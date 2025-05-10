#pragma once

#include "serialization.h"

namespace icp {
namespace serialization {
/**
 * @namespace binary
 * @brief All related types for the binary format used in Iggy's TCP and QUIC transports.
 *
 * The definitions \htmlonly <a href="https://docs.iggy.rs/specification/binary">binary specification</a> \endhtmlonly
 * which should be taken as canonical; E2E conformance tests will be used to ensure client stays in line. I am not sure
 * about this design yet; ideally I want minimal duplication between the binary and JSON variations, and so may
 * introduce format-specific serializers. This is less natural in C++ than Rust.
 */
namespace binary {

/**
 * @enum CommandCode
 * @brief Complete list of supported integer codes for the Iggy protocol.
 */
enum CommandCode {
    PING = 1,
    GET_STATS = 10,
    GET_ME = 20,
    GET_CLIENT = 21,
    GET_CLIENTS = 22,
    POLL_MESSAGES = 100,
    SEND_MESSAGES = 101,
    GET_CONSUMER_OFFSET = 120,
    STORE_CONSUMER_OFFSET = 121,
    GET_STREAM = 200,
    GET_STREAMS = 201,
    CREATE_STREAM = 202,
    DELETE_STREAM = 203,
    GET_TOPIC = 300,
    GET_TOPICS = 301,
    CREATE_TOPIC = 302,
    DELETE_TOPIC = 303,
    CREATE_PARTITIONS = 402,
    DELETE_PARTITIONS = 403,
    GET_CONSUMER_GROUP = 600,
    GET_CONSUMER_GROUPS = 601,
    CREATE_CONSUMER_GROUP = 602,
    DELETE_CONSUMER_GROUP = 603,
    JOIN_CONSUMER_GROUP = 604,
    LEAVE_CONSUMER_GROUP = 605
};

/**
 * @class BinaryWireFormat
 * @brief Simple binary serialization and deserialization for Iggy's protocol.
 */
class BinaryWireFormat : icp::serialization::WireFormat {
public:
    BinaryWireFormat() = default;
}

}  // namespace binary
}  // namespace serialization
}  // namespace icp
