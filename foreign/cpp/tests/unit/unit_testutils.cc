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
#include "unit_testutils.h"
#include <fmt/format.h>
#include <iostream>
#include <random>
#include <reproc++/drain.hpp>
#include <reproc++/reproc.hpp>
#include <stdexcept>
#include <vector>

icp::testutil::SelfSignedCertificate::SelfSignedCertificate()
    : certificatePath(generateRandomTempPath("cert.pem"))
    , keyPath(generateRandomTempPath("key.pem")) {
    std::vector<std::string> arguments = {"openssl",  "req",
                                          "-x509",    "-newkey",
                                          "rsa:2048", "-nodes",
                                          "-keyout",  this->keyPath.string(),
                                          "-out",     this->certificatePath.string(),
                                          "-days",    "365",
                                          "-subj",    "/CN=localhost"};
    reproc::options options;
    options.redirect.discard = true;
    reproc::process process;

    std::string output;
    reproc::sink::string sink(output);
    auto err = process.start(arguments, options);
    if (err) {
        throw std::runtime_error(fmt::format("Failed to run OpenSSL: {}", err.message()));
    }
    err = reproc::drain(process, sink, reproc::sink::null);
    if (err) {
        throw std::runtime_error(fmt::format("Failed to read OpenSSL output: {}", err.message()));
    }
    std::cout << output << std::flush;
}

icp::testutil::SelfSignedCertificate::~SelfSignedCertificate() {
    std::filesystem::remove(this->certificatePath);
    std::filesystem::remove(this->keyPath);
}

std::filesystem::path icp::testutil::SelfSignedCertificate::generateRandomTempPath(const std::string& baseName) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> distrib(0, RAND_MAX);
    return std::filesystem::temp_directory_path() / (std::to_string(distrib(gen)) + baseName);
}
