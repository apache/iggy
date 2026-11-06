# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# The conformance agent of the C++ SDK. It links the cxx bridge of foreign/cpp, which cargo
# builds, so no Bazel is needed.
cmake_minimum_required(VERSION 3.24)
project(iggy_conformance_agent LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

get_filename_component(IGGY_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
set(SDK_DIR "${IGGY_ROOT}/foreign/cpp")

find_program(CARGO cargo REQUIRED)

# Cargo writes a Cargo.lock next to the manifest it builds. A copy of the SDK manifest in the
# build tree, with absolute dependency paths and the sources linked in, keeps foreign/cpp
# untouched. The lock starts from the workspace lock, so the SDK crates resolve to the same
# versions as the server under test.
set(BRIDGE_CRATE "${CMAKE_BINARY_DIR}/iggy-cpp")
file(READ "${SDK_DIR}/Cargo.toml" bridge_manifest)
string(REGEX MATCH "\nversion = \"([^\"]+)\"" sdk_version "${bridge_manifest}")
set(SDK_VERSION "${CMAKE_MATCH_1}")
string(REPLACE "path = \"../../" "path = \"${IGGY_ROOT}/" bridge_manifest "${bridge_manifest}")
file(WRITE "${BRIDGE_CRATE}/Cargo.toml" "${bridge_manifest}\n[workspace]\n")
file(CREATE_LINK "${SDK_DIR}/src" "${BRIDGE_CRATE}/src" SYMBOLIC)
file(CREATE_LINK "${SDK_DIR}/build.rs" "${BRIDGE_CRATE}/build.rs" SYMBOLIC)
if(NOT EXISTS "${BRIDGE_CRATE}/Cargo.lock")
    file(COPY_FILE "${IGGY_ROOT}/Cargo.lock" "${BRIDGE_CRATE}/Cargo.lock")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CARGO_PROFILE debug)
    set(CARGO_PROFILE_FLAG "")
else()
    set(CARGO_PROFILE release)
    set(CARGO_PROFILE_FLAG --release)
endif()
set(CARGO_TARGET_DIR "${CMAKE_BINARY_DIR}/cargo")
set(BRIDGE_LIBRARY "${CARGO_TARGET_DIR}/${CARGO_PROFILE}/libiggy_cpp.a")
# cxx-build links its generated headers and sources here, independent of the profile.
set(BRIDGE_GENERATED "${CARGO_TARGET_DIR}/cxxbridge")
set(BRIDGE_SOURCE "${BRIDGE_GENERATED}/iggy-cpp/src/lib.rs.cc")

# Cargo runs on every build and decides itself what changed in the SDK.
add_custom_target(iggy_cpp_bridge
    COMMAND "${CMAKE_COMMAND}" -E env --unset=MAKEFLAGS --unset=MFLAGS "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
            "${CARGO}" build ${CARGO_PROFILE_FLAG} --manifest-path "${BRIDGE_CRATE}/Cargo.toml"
    BYPRODUCTS "${BRIDGE_LIBRARY}" "${BRIDGE_SOURCE}"
    WORKING_DIRECTORY "${BRIDGE_CRATE}"
    USES_TERMINAL)

include(FetchContent)
# The SDK's Bazel tests use the same nlohmann_json release.
FetchContent_Declare(nlohmann_json
    URL https://github.com/nlohmann/json/releases/download/v3.12.0/json.tar.xz
    URL_HASH SHA256=42f6e95cad6ec532fd372391373363b62a14af6d771056dbfc86160e6dfff7aa)
FetchContent_MakeAvailable(nlohmann_json)

find_package(Threads REQUIRED)

add_executable(iggy-agent
    src/main.cpp
    src/agent.cpp
    src/calls.cpp
    src/args.cpp
    src/canonical.cpp
    src/metadata.cpp
    "${BRIDGE_SOURCE}")
add_dependencies(iggy-agent iggy_cpp_bridge)
target_include_directories(iggy-agent PRIVATE "${BRIDGE_GENERATED}")
target_compile_definitions(iggy-agent PRIVATE IGGY_CPP_SDK_VERSION="${SDK_VERSION}")
target_compile_options(iggy-agent PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(iggy-agent PRIVATE
    nlohmann_json::nlohmann_json
    "${BRIDGE_LIBRARY}"
    Threads::Threads
    ${CMAKE_DL_LIBS}
    m)
