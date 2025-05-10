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
#include "client.h"
#include <reproc++/reproc.hpp>
#include <string>
#include <vector>

icp::client::Client::Client(const Options& options) {
    // to make more natural interface for setting options we use a struct, so need to validate it.
    options.validate();
}

void icp::client::Client::ping() {
    // temporary! using the Rust CLI from Docker to execute the ping to demonstrate test fixture
    reproc::process ping_process;
    reproc::options options;
    options.redirect.parent = true;
    std::vector<std::string> ping_args = {"docker", "exec", "iggy_test", "/iggy/iggy", "ping"};
    ping_process.start(ping_args, options);
    ping_process.wait(reproc::milliseconds(5000));
}

icp::model::sys::Stats icp::client::Client::getStats() {
    // temporary! need to send GetStats command to server then use WireFormat to read the response
    return icp::model::sys::Stats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, "", "", "", "");
}
