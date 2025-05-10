#pragma once

#include <nlohmann/json.hpp>
#include "serialization.h"

using json = nlohmann::json;

namespace icp {
namespace serialization {

/**
 * @brief All related types for the JSON format used in Iggy's HTTP REST transport.
 */
namespace json {

/**
 * @brief JSON serialization and deserialization for Iggy's wire protocol.
 */
class JsonWireFormat : icp::serialization::WireFormat {
public:
    JsonWireFormat() = default;
};

}  // namespace json
}  // namespace serialization
}  // namespace icp
