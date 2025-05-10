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
#include "e2e_testutils.h"
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

IggyRunner::IggyRunner() {
    // start the Docker process with stdout redirected to parent process
    std::vector<std::string> arguments = {"docker", "run", "-d", "--name", "iggy_test", "iggyrs/iggy:latest"};
    reproc::options options;
    options.redirect.parent = true;
    auto err = process.start(arguments, options);
    if (err) {
        throw std::runtime_error("Failed to start Docker process");
    }

    // need to figure out a polling mechanism to check if the server is up and running
    std::this_thread::sleep_for(std::chrono::seconds(5));
}

IggyRunner::~IggyRunner() {
    // stop the Docker process
    process.stop(reproc::stop_actions{{reproc::stop::terminate, reproc::milliseconds(5000)},
                                      {reproc::stop::kill, reproc::milliseconds(2000)},
                                      {reproc::stop::wait, reproc::milliseconds(2000)}});

    // remove the Docker container
    reproc::process remove_process;
    std::vector<std::string> remove_arguments = {"docker", "rm", "-f", "iggy_test"};
    remove_process.start(remove_arguments);
    remove_process.wait(reproc::milliseconds(5000));
}
