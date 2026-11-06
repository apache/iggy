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

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>

#include <nlohmann/json.hpp>

#include "iggy-cpp/src/lib.rs.h"
#include "rust/cxx.h"

namespace agent {

using Json    = nlohmann::json;
namespace ffi = iggy::ffi;

// One wait of the SDK, after which a call that waits checks whether it was cancelled.
constexpr std::uint64_t kWaitSliceMicros = 100'000;

struct SdkError {
    // Zero, which no IggyError has, stands for an error that the SDK reports by its message only.
    std::uint32_t code = 0;
    std::optional<std::uint64_t> operation_id;
    std::optional<std::uint32_t> partition_id;
    std::string message;
};

// Ends a call with an error of the SDK.
class SdkFailure : public std::exception {
  public:
    explicit SdkFailure(SdkError error) : error_(std::move(error)) {}
    [[nodiscard]] const SdkError &Error() const noexcept { return error_; }
    [[nodiscard]] const char *what() const noexcept override { return error_.message.c_str(); }

  private:
    SdkError error_;
};

// Ends a call with a fault of the agent itself, such as an argument that it cannot read.
class AgentFailure : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Ends a call that needs a method, an argument value or a transport that the SDK lacks.
class Unsupported : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

// Exactly one answer of a request: a result, an SDK error, an unsupported call or a failure.
class Reply {
  public:
    static Reply Result(Json value) { return Reply("result", std::move(value)); }
    static Reply Error(const SdkError &error);
    static Reply NotSupported(const std::string &reason) { return Reply("unsupported", reason); }
    static Reply Failure(const std::string &reason) { return Reply("failure", reason); }

    [[nodiscard]] Json Line(std::uint64_t id) const { return {{"id", id}, {field_, value_}}; }

  private:
    Reply(std::string field, Json value) : field_(std::move(field)), value_(std::move(value)) {}

    std::string field_;
    Json value_;
};

// Runs `call` and answers with its result, or with the answer that ended it.
Reply Answer(const std::function<Json()> &call);

SdkError ToSdkError(const ffi::ErrorInfo &info, const char *message);

// Runs a bridge call with an ErrorInfo of its own, and throws the error that the call raised as an
// SdkFailure.
template <typename Call>
auto Sdk(Call &&call) {
    ffi::ErrorInfo info{};
    try {
        return call(info);
    } catch (const rust::Error &error) {
        throw SdkFailure(ToSdkError(info, error.what()));
    }
}

// Sdk() for the reads of a reader or a consumer, whose errors name their partition.
template <typename Call>
auto SdkRead(Call &&call) {
    ffi::ReadErrorInfo info{};
    try {
        return call(info);
    } catch (const rust::Error &error) {
        auto failed = ToSdkError(info.error, error.what());
        if (!failed.partition_id && info.has_partition_id) {
            failed.partition_id = info.partition_id;
        }
        throw SdkFailure(std::move(failed));
    }
}

// Runs each request on a thread of its own, so a long call never delays a reset or a cancellation
// behind it, and waits for every thread at exit.
class Threads {
  public:
    void Spawn(std::function<void()> work);
    void Wait();

  private:
    std::mutex mutex_;
    std::condition_variable idle_;
    std::size_t running_ = 0;
};

// A client owns its bridge handle. Each call keeps the client alive, because the bridge forbids
// deleting a client while a call on it runs.
class ClientEntity {
  public:
    explicit ClientEntity(ffi::Client *client) : client_(client) {}
    ~ClientEntity() { ffi::delete_client(client_); }
    ClientEntity(const ClientEntity &)            = delete;
    ClientEntity &operator=(const ClientEntity &) = delete;

    [[nodiscard]] const ffi::Client &Handle() const { return *client_; }

  private:
    ffi::Client *client_;
};

struct ReaderEntity {
    rust::Box<ffi::PartitionReader> handle;
};

struct ConsumerEntity {
    rust::Box<ffi::Consumer> handle;
};

class Agent {
  public:
    explicit Agent(Threads &threads) : threads_(threads) {}

    Reply Handle(const Json &request);
    // Cancels every task and closes every entity.
    void Reset();

    void KeepReader(const std::string &entity, rust::Box<ffi::PartitionReader> reader);
    void KeepConsumer(const std::string &entity, rust::Box<ffi::Consumer> consumer);

  private:
    struct Task {
        std::shared_ptr<std::atomic<bool>> cancelled;
        std::future<Reply> answer;
    };

    Json CreateClient(const Json &request);
    Json Call(const std::string &entity,
              const std::string &method,
              const Json &args,
              const std::string &save_as,
              const std::atomic<bool> &cancelled);
    Reply Start(const Json &request);
    std::optional<Task> TakeTask(const std::string &name);
    void Close(const std::string &entity);

    Threads &threads_;
    std::mutex mutex_;
    std::map<std::string, std::shared_ptr<ClientEntity>> clients_;
    std::map<std::string, std::shared_ptr<ReaderEntity>> readers_;
    std::map<std::string, std::shared_ptr<ConsumerEntity>> consumers_;
    std::map<std::string, Task> tasks_;
};

struct ClientCall {
    Agent &agent;
    const ffi::Client &client;
    // The entity that the call creates, empty for a call that creates none.
    const std::string &save_as;
    const std::atomic<bool> &cancelled;
};

Json CallClient(const ClientCall &call, const std::string &method, const Json &args);
Json NextBatch(const ffi::PartitionReader &reader, const Json &args, const std::atomic<bool> &cancelled);
Json NextMessage(const ffi::Consumer &consumer, const Json &args, const std::atomic<bool> &cancelled);

}  // namespace agent
