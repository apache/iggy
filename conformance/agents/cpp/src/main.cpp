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

// The conformance agent of the C++ SDK. It reads JSON requests from stdin, calls the SDK and writes
// one JSON answer per request to stdout. It never judges a result. See conformance/README.md for the
// protocol.

#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>
#include <utility>

#include "agent.hpp"

namespace {

using agent::Json;

constexpr int kProtocolVersion = 1;

Json Hello() {
    const auto capabilities =
        Json::array({"reader", "consumer", "resolve_operation", "abandon_operation", "raw_command",
                     "raw_command_replica", "describe_options", "metadata_changes"});
    return {{"hello",
             {{"sdk", "cpp"},
              {"sdk_version", IGGY_CPP_SDK_VERSION},
              {"protocol", kProtocolVersion},
              {"transports", Json::array({"tcp"})},
              {"capabilities", capabilities}}}};
}

class Output {
  public:
    void Write(const Json &line) {
        const auto text = line.dump(-1, ' ', false, Json::error_handler_t::replace);
        const std::lock_guard<std::mutex> lock(mutex_);
        std::cout << text << '\n' << std::flush;
    }

  private:
    std::mutex mutex_;
};

}  // namespace

int main() {
    Output output;
    output.Write(Hello());

    agent::Threads threads;
    agent::Agent handler(threads);
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.find_first_not_of(" \t\r") == std::string::npos) {
            continue;
        }
        auto request = Json::parse(line, nullptr, false);
        if (!request.is_object() || !request.contains("id") || !request.at("id").is_number_unsigned()) {
            std::cerr << "cannot read the request " << line << '\n';
            continue;
        }
        const auto id = request.at("id").get<std::uint64_t>();
        threads.Spawn(
            [&output, &handler, id, request = std::move(request)] { output.Write(handler.Handle(request).Line(id)); });
    }
    handler.Reset();
    threads.Wait();
    return 0;
}
