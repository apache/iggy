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

#include "iggy.hpp"

namespace iggy {

IggyBlockingConsumer::IggyBlockingConsumer(IggyBlockingConsumer &&other) noexcept
    : consumer_(std::exchange(other.consumer_, nullptr)) {}

IggyBlockingConsumer &IggyBlockingConsumer::operator=(IggyBlockingConsumer &&other) noexcept {
    if (this != &other) {
        Reset();
        consumer_ = std::exchange(other.consumer_, nullptr);
    }
    return *this;
}

IggyBlockingConsumer::~IggyBlockingConsumer() {
    Reset();
}

IggyBlockingConsumer::IggyBlockingConsumer(ffi::Consumer *consumer) : consumer_(consumer) {
    if (consumer_ == nullptr) {
        throw IggyException("Could not create Iggy consumer");
    }
}

ffi::Consumer *IggyBlockingConsumer::Handle() const {
    if (consumer_ == nullptr) {
        throw IggyException("Cannot use a moved-from IggyBlockingConsumer");
    }
    return consumer_;
}

void IggyBlockingConsumer::Reset() noexcept {
    if (consumer_ == nullptr) {
        return;
    }

    ffi::Consumer *consumer{std::exchange(consumer_, nullptr)};
    ffi::delete_consumer(consumer);
}

}  // namespace iggy
