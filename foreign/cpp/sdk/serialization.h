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

#include <string>
#include "command.h"
#include "model.h"

namespace icp {
namespace serialization {

/***
 * @brief Helper function to ensure C++ strings are UTF-8 clean.
 */
std::string convertToUTF8(const std::string& source, bool strict = true);

/***
 * @brief Base class for
 */
class WireFormat {
public:
    virtual ~WireFormat() = 0;

    template <typename T, typename std::enable_if<std::is_base_of<icp::model::Model, T>::value>::type* = nullptr>
    T readModel(std::istream& in);

    template <typename T, typename std::enable_if<std::is_base_of<icp::command::Command, T>::value>::type* = nullptr>
    void writeModel(std::ostream& out, const T& value);
};
}  // namespace serialization
}  // namespace icp
