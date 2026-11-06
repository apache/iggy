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

#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "agent.hpp"

namespace agent {

// The arguments of one call. Finish() refuses a field that no read took, so a new argument shows up
// as a failure instead of being ignored.
class Args {
  public:
    // `value` is an object, or null for no arguments.
    explicit Args(const Json &value);

    // The field, or nullptr when it is absent or null.
    const Json *Optional(std::string_view key);
    const Json &Required(std::string_view key);
    void Finish() const;

  private:
    const Json &value_;
    std::set<std::string, std::less<>> taken_;
};

std::string Text(const Json &value, std::string_view what);
std::uint64_t Unsigned(const Json &value, std::string_view what);
std::uint32_t Unsigned32(const Json &value, std::string_view what);
std::uint8_t Unsigned8(const Json &value, std::string_view what);
bool Boolean(const Json &value, std::string_view what);

rust::Vec<std::uint8_t> RustBytes(const std::vector<std::uint8_t> &bytes);

// A number is an id, text is a name.
ffi::Identifier Identifier(const Json &value);
ffi::Identifier NumericIdentifier(std::uint32_t id);

struct ConsumerArg {
    std::string kind;
    ffi::Identifier id;
};

// `{consumer: id}` or `{group: id}`. Without one, the default consumer of the Rust SDK.
ConsumerArg Consumer(const Json *value);

struct PartitioningArg {
    std::string kind;
    rust::Vec<std::uint8_t> value;
};

PartitioningArg Partitioning(const Json &value);
ffi::PollingStrategy Strategy(const Json &value);
// Sets the commit policy `manual` (the default), `on_read` or `after_processing`.
void SetCommit(ffi::PollOptions &options, const Json *value);

struct ExpiryArg {
    std::string kind;
    std::uint64_t value = 0;
};

ExpiryArg Expiry(const Json &value);
std::string TopicSize(const Json &value);
rust::Vec<rust::String> ResetKeys(const Json *value);
ffi::UserStatus UserStatus(const Json &value);
ffi::Permissions Permissions(const Json &value);
rust::Vec<ffi::IggyMessageToSend> Messages(const Json &value);
// Sets the start of a metadata changes query, with the cursor or page token in its text form.
void SetStart(ffi::MetadataChangesQuery &query, const Json &value);
rust::Vec<std::uint8_t> Groups(const Json *value);

}  // namespace agent
