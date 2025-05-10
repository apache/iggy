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
