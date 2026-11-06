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

// The canonical result forms of core/conformance/src/canonical.rs. The orchestrator decodes each
// result into its form and refuses any field that the form does not have.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "agent.hpp"

namespace agent {

Json StreamJson(const ffi::Stream &stream);
Json StreamDetailsJson(const ffi::StreamDetails &stream);
Json TopicJson(const ffi::Topic &topic);
Json TopicDetailsJson(const ffi::TopicDetails &topic);
Json PolledJson(const ffi::PolledMessages &polled);
Json MessageJson(const ffi::IggyMessagePolled &message);
Json GroupJson(const ffi::ConsumerGroupDetails &group);
Json OptionSpecJson(const ffi::OptionSpec &spec);
Json UserJson(const ffi::UserInfoDetails &user);
Json GlobalPermissionsJson(const ffi::GlobalPermissions &global);
std::string StatusName(ffi::UserStatus status);
// The HTTP JSON form of metadata changes, as core/common/fixtures/metadata_changes.json names it
// "expected".
Json MetadataChangesJson(const ffi::MetadataChanges &changes);

// An option map: the keys that the client set and the keys that the server derived, each with its
// value as reversible text, its kind and its explicit flag.
Json OptionsJson(const rust::Vec<ffi::HeaderEntry> &explicit_entries, const rust::Vec<ffi::HeaderEntry> &derived);

std::string KindName(std::uint8_t kind);
std::uint8_t KindCode(std::string_view name);
// The reversible text of a value: UTF-8 for strings, true or false, base-10 integers, padded
// standard base64 for raw bytes, and 0x with the IEEE-754 bits in fixed-width lowercase hex for
// floats.
std::string ValueText(const ffi::HeaderField &field);
// The bytes of a value from its text, the reverse of ValueText().
std::vector<std::uint8_t> ValueBytes(std::uint8_t kind, std::string_view text);
// A header or option key as text.
std::string KeyText(const ffi::HeaderField &key);

std::string Hex(const std::uint8_t *data, std::size_t size);
std::vector<std::uint8_t> FromHex(std::string_view text);
std::string Base64(const std::uint8_t *data, std::size_t size);
bool IsUtf8(const std::uint8_t *data, std::size_t size);
// A u128, given as its two halves, as 32 lowercase hex characters.
std::string Hex128(std::uint64_t high, std::uint64_t low);
// A u128, given as its two halves, as decimal text.
std::string Decimal128(std::uint64_t high, std::uint64_t low);
// The halves, high first, of a u128 in decimal text.
std::pair<std::uint64_t, std::uint64_t> ParseDecimal128(std::string_view text);

template <typename Bytes>
std::string Hex(const Bytes &bytes) {
    return Hex(bytes.data(), bytes.size());
}

template <typename Bytes>
std::string Base64(const Bytes &bytes) {
    return Base64(bytes.data(), bytes.size());
}

}  // namespace agent
