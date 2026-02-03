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

#include "iggy.hpp"

namespace iggy {

Identifier::~Identifier() noexcept { destroy(); }

Identifier::Identifier(Identifier &&other) noexcept
    : identifier_(std::exchange(other.identifier_, nullptr)) {}

Identifier &Identifier::operator=(Identifier &&other) noexcept {
  if (this != &other) {
    destroy();
    identifier_ = std::exchange(other.identifier_, nullptr);
  }
  return *this;
}

Identifier::Identifier(iggy::ffi::Identifier *identifier)
    : identifier_(identifier) {}

void Identifier::destroy() noexcept {
  if (!identifier_)
    return;
  iggy::ffi::delete_identifier(identifier_);
  identifier_ = nullptr;
}

Identifier Identifier::from_string(const std::string &name) {
  try {
    rust::String str(name);
    iggy::ffi::Identifier *identifier =
        iggy::ffi::identifier_from_named(std::move(str));
    if (!identifier)
      throw IggyException("iggy::ffi::identifier_from_named returned null");
    return Identifier(identifier);
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

Identifier Identifier::from_uint32(std::uint32_t id) {
  try {
    iggy::ffi::Identifier *identifier = iggy::ffi::identifier_from_numeric(id);
    if (!identifier)
      throw IggyException("iggy::ffi::identifier_from_numeric returned null");
    return Identifier(identifier);
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::string Identifier::value() const {
  try {
    return std::string(identifier_->get_value());
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}
} // namespace iggy
