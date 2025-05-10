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

#include <sys/types.h>
#include <cstdint>

/// @brief Mapping for fractional values between 0 and 1.
typedef float percent_t;

/// @brief Mapping for absolute times and time durations; currently in seconds.
typedef uint64_t time_val_t;

/// @brief Mapping for values that measure byte counts.
typedef uint64_t byte_cnt_t;

/// @brief Mapping for values that measure object counts.
typedef uint32_t obj_cnt_t;

/// @brief Mapping for values that measure (potentially very large) message counts.
typedef uint64_t msg_cnt_t;

/// @brief Mapping for a 128-bit integer support; supports gcc, clang and ICC, not MSVC.
/// @ref
/// [https://quuxplusone.github.io/blog/2019/02/28/is-int128-integral](https://quuxplusone.github.io/blog/2019/02/28/is-int128-integral)
using I128 = __int128;
typedef unsigned __int128 uint128_t;
