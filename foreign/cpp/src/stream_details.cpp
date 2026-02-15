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

StreamDetails::~StreamDetails() noexcept { destroy(); }

StreamDetails::StreamDetails(StreamDetails &&other) noexcept
    : stream_details_(std::exchange(other.stream_details_, nullptr)) {}

StreamDetails &StreamDetails::operator=(StreamDetails &&other) noexcept {
  if (this != &other) {
    destroy();
    stream_details_ = std::exchange(other.stream_details_, nullptr);
  }
  return *this;
}

StreamDetails::StreamDetails(iggy::ffi::StreamDetails *stream_details)
    : stream_details_(stream_details) {}

void StreamDetails::destroy() noexcept {
  if (!stream_details_)
    return;
  iggy::ffi::delete_stream_details(stream_details_);
  stream_details_ = nullptr;
}

std::uint32_t StreamDetails::stream_id() const {
  try {
    return stream_details_->id();
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::string StreamDetails::name() const {
  try {
    return std::string(stream_details_->name());
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint64_t StreamDetails::messages_count() const {
  try {
    return stream_details_->messages_count();
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint32_t StreamDetails::topics_count() const {
  try {
    return stream_details_->topics_count();
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

} // namespace iggy
