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
cmake_minimum_required(VERSION 3.14)
project(iggy-cpp-client)

option(BUILD_TESTS "Build and run unit tests" OFF)
option(BUILD_DOCS "Build documentation" OFF)
option(ENABLE_CODE_COVERAGE "Enable coverage reporting" OFF)

# avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
  cmake_policy(SET CMP0135 NEW)
endif()

# global settings
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# set up library dependencies
find_package(ada CONFIG REQUIRED)
find_package(libuv CONFIG REQUIRED)
find_package(nlohmann_json CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(unofficial-sodium CONFIG REQUIRED)

# these do not correctly support CMake
find_path(ADA_INCLUDE_DIR ada.h REQUIRED)
find_path(JSON_INCLUDE_DIR nlohmann/json.hpp REQUIRED)
find_path(SODIUM_INCLUDE_DIR sodium.h REQUIRED)
find_path(UTF8H_INCLUDE_DIRS "utf8h/utf8.h" REQUIRED)

# customize the builds of key networking components; WolfSSL is not
# well supported in vcpkg and we want to have more control here
include(ExternalProject)
execute_process(COMMAND nproc OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE NPROC)
externalproject_add(wolfssl
    GIT_REPOSITORY https://github.com/wolfSSL/wolfssl
    GIT_TAG v5.7.0-stable
    PREFIX ${CMAKE_BINARY_DIR}/wolfssl
    BUILD_IN_SOURCE 1
    CONFIGURE_COMMAND autoreconf -i COMMAND <SOURCE_DIR>/configure --prefix=<INSTALL_DIR> --enable-tls13 --enable-tls12 --enable-tlsx --enable-quic --enable-harden --enable-keylog-export --enable-static --enable-sys-ca-certs --enable-libwebsockets --disable-ech --disable-psk --disable-dtls --disable-anonymous --disable-nullcipher --disable-oldtls --disable-sslv3 --disable-webserver --disable-crypttests
    BUILD_COMMAND make -j ${NPROC}
    BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/wolfssl/lib/libwolfssl.a
    INSTALL_COMMAND make install
    UPDATE_COMMAND ""
)
externalproject_get_property(wolfssl INSTALL_DIR)
set(WOLFSSL_INSTALL_DIR ${INSTALL_DIR})

# customize lws to link to our local wolfSSL library
externalproject_add(lws
    GIT_REPOSITORY https://github.com/warmcat/libwebsockets.git
    GIT_TAG v4.3.3
    PREFIX ${CMAKE_BINARY_DIR}/lws
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=<INSTALL_DIR> -DLWS_WITH_SHARED=OFF -DLWS_WITH_WOLFSSL=1 -DLWS_WOLFSSL_INCLUDE_DIRS=${WOLFSSL_INSTALL_DIR}/include -DLWS_WOLFSSL_LIBRARIES=${WOLFSSL_INSTALL_DIR}/lib/libwolfssl.a ..
    BUILD_BYPRODUCTS ${CMAKE_BINARY_DIR}/lws/lib/libwebsockets.a
    DEPENDS wolfssl
)
externalproject_get_property(lws INSTALL_DIR)
set(LWS_INSTALL_DIR ${INSTALL_DIR})

set(WOLFSSL_INCLUDE_DIR ${CMAKE_BINARY_DIR}/wolfssl/include)
set(WOLFSSL_LIB_DIR ${CMAKE_BINARY_DIR}/wolfssl/lib)
set(LWS_INCLUDE_DIR ${CMAKE_BINARY_DIR}/lws/include)
set(LWS_LIB_DIR ${CMAKE_BINARY_DIR}/lws/lib)

set(IGGY_INCLUDE_DIRS
  ${ADA_INCLUDE_DIR}
  ${JSON_INCLUDE_DIR}
  ${LWS_INCLUDE_DIR}
  ${SODIUM_INCLUDE_DIR}
  ${UTF8H_INCLUDE_DIRS}
  ${WOLFSSL_INCLUDE_DIR}
)

file(GLOB_RECURSE IGGY_SOURCES "sdk/*.cc")
add_library(
  iggy
  ${IGGY_SOURCES}
)
target_include_directories(iggy PRIVATE ${IGGY_INCLUDE_DIRS})
add_dependencies(iggy lws)
target_link_libraries(
  iggy PRIVATE

  ada::ada
  fmt::fmt
  libuv::uv_a
  nlohmann_json::nlohmann_json
  unofficial-sodium::sodium
  ${WOLFSSL_LIB_DIR}/libwolfssl.a
  ${LWS_LIB_DIR}/libwebsockets.a
)

# even though this is related to unit tests, to get a full report we need to ensure that
# all library files are compiled with --coverage so gcno is generated properly
if(ENABLE_CODE_COVERAGE)
  include(cmake/modules/CodeCoverage.cmake)
  append_coverage_compiler_flags()
endif()

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

if(BUILD_DOCS)
  add_subdirectory(docs)
endif()
