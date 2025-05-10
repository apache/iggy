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

#include <catch.hpp>
#include <reproc++/reproc.hpp>

const char E2E_TAG[] = "[E2E Tests]";

/**
 * @brief Test fixture that starts and stops a Docker container for each test.
 *
 * This test fixture is meant for use in end-to-end (E2E) tests for the Iggy C++ client.
 * It will start up the latest Iggy server inside a Docker container, allow you to
 * interact with it, then stop and remove the container in the TearDown() method.
 */
class IggyRunner {
private:
    reproc::process process;

public:
    IggyRunner();
    ~IggyRunner();
};
