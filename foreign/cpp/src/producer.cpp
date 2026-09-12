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

IggyBlockingProducer::IggyBlockingProducer(IggyBlockingProducer &&other) noexcept
    : producer_(std::exchange(other.producer_, nullptr)) {}

IggyBlockingProducer &IggyBlockingProducer::operator=(IggyBlockingProducer &&other) noexcept {
    if (this != &other) {
        Reset();
        producer_ = std::exchange(other.producer_, nullptr);
    }
    return *this;
}

IggyBlockingProducer::~IggyBlockingProducer() {
    Reset();
}

IggyBlockingProducer::IggyBlockingProducer(ffi::Producer *producer) : producer_(producer) {
    if (producer_ == nullptr) {
        throw IggyException("Could not create Iggy producer");
    }
}

ffi::Producer *IggyBlockingProducer::Handle() const {
    if (producer_ == nullptr) {
        throw IggyException("Cannot use a moved-from IggyBlockingProducer");
    }
    return producer_;
}

void IggyBlockingProducer::Reset() noexcept {
    if (producer_ == nullptr) {
        return;
    }

    ffi::Producer *producer{std::exchange(producer_, nullptr)};
    ffi::delete_producer(producer);
}

}  // namespace iggy
