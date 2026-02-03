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

TopicDetails::~TopicDetails() noexcept { destroy(); }

TopicDetails::TopicDetails(TopicDetails &&other) noexcept
    : topic_details_(std::exchange(other.topic_details_, nullptr)) {}

TopicDetails &TopicDetails::operator=(TopicDetails &&other) noexcept {
  if (this != &other) {
    destroy();
    topic_details_ = std::exchange(other.topic_details_, nullptr);
  }
  return *this;
}

TopicDetails::TopicDetails(iggy::ffi::TopicDetails *p) : topic_details_(p) {}

void TopicDetails::destroy() noexcept {
  if (!topic_details_)
    return;
  iggy::ffi::delete_topic_details(topic_details_);
  topic_details_ = nullptr;
}

std::uint32_t TopicDetails::topic_id() const {
  try {
    return topic_details_->id();
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::string TopicDetails::name() const {
  try {
    return std::string(topic_details_->name());
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint64_t TopicDetails::messages_count() const {
  try {
    return topic_details_->messages_count();
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

std::uint32_t TopicDetails::partitions_count() const {
  try {
    return topic_details_->partitions_count();
  } catch (const rust::Error &err) {
    throw IggyException(err.what());
  } catch (const std::exception &ex) {
    throw IggyException(ex.what());
  }
}

} // namespace iggy
