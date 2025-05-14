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
#include "serialization.h"
#include <fmt/format.h>
#include <utf8h/utf8.h>
#include <stdexcept>

std::string icp::serialization::convertToUTF8(const std::string& source, bool strict) {
    std::unique_ptr<char8_t[]> data(new char8_t[source.size() + 1]);
    std::copy(source.begin(), source.end(), data.get());
    data[source.size()] = u8'\0';
    if (utf8valid(data.get()) == 0) {
        return source;
    } else {
        if (strict) {
            throw std::invalid_argument(fmt::format("The input string is not a valid UTF-8 string: '{}'", source));
        } else {
            utf8makevalid(data.get(), '?');
            return std::string(reinterpret_cast<char*>(data.get()));
        }
    }
}
