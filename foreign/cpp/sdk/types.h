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
