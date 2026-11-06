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

#include "agent.hpp"

#include <iostream>
#include <thread>
#include <vector>

#include "args.hpp"

namespace agent {

namespace {

// A call that no request can cancel.
const std::atomic<bool> kNeverCancelled{false};

std::string Field(const Json &request, const char *name) {
    const auto field = request.find(name);
    return field != request.end() && field->is_string() ? field->get<std::string>() : std::string();
}

const Json &ArgsOf(const Json &request) {
    static const Json kNoArgs;
    const auto args = request.find("args");
    return args == request.end() ? kNoArgs : *args;
}

// Closes one part of an entity. A failed close still lets the other parts close.
template <typename Close>
void CloseQuietly(const std::string &entity, Close &&close) {
    try {
        Sdk(std::forward<Close>(close));
    } catch (const SdkFailure &failure) {
        std::cerr << "cannot close " << entity << ": " << failure.what() << '\n';
    }
}

}  // namespace

Reply Reply::Error(const SdkError &error) {
    Json value = {{"code", error.code}, {"message", error.message}};
    if (error.operation_id) {
        value["operation_id"] = *error.operation_id;
    }
    if (error.partition_id) {
        value["partition_id"] = *error.partition_id;
    }
    return Reply("error", std::move(value));
}

Reply Answer(const std::function<Json()> &call) {
    try {
        return Reply::Result(call());
    } catch (const SdkFailure &failure) {
        return Reply::Error(failure.Error());
    } catch (const Unsupported &unsupported) {
        return Reply::NotSupported(unsupported.what());
    } catch (const std::exception &failure) {
        return Reply::Failure(failure.what());
    }
}

SdkError ToSdkError(const ffi::ErrorInfo &info, const char *message) {
    SdkError error;
    error.code    = info.code;
    error.message = message;
    if (info.has_operation_id) {
        error.operation_id = info.operation_id;
    }
    if (info.has_blocked_partition_id) {
        error.partition_id = info.blocked_partition_id;
    }
    return error;
}

void Threads::Spawn(std::function<void()> work) {
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        ++running_;
    }
    std::thread([this, work = std::move(work)] {
        work();
        const std::lock_guard<std::mutex> lock(mutex_);
        if (--running_ == 0) {
            idle_.notify_all();
        }
    }).detach();
}

void Threads::Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    idle_.wait(lock, [this] { return running_ == 0; });
}

Reply Agent::Handle(const Json &request) {
    const auto op = Field(request, "op");
    if (op == "create") {
        return Answer([&] { return CreateClient(request); });
    }
    if (op == "call") {
        return Answer([&] {
            return Call(Field(request, "entity"), Field(request, "method"), ArgsOf(request), Field(request, "save_as"),
                        kNeverCancelled);
        });
    }
    if (op == "start") {
        return Start(request);
    }
    if (op == "await") {
        auto task = TakeTask(Field(request, "task"));
        if (!task) {
            return Reply::Failure("no task " + Field(request, "task"));
        }
        // Answer() turns every exception of the call into an answer, so the task always sets one.
        return task->answer.get();
    }
    if (op == "cancel") {
        const auto task = TakeTask(Field(request, "task"));
        if (!task) {
            return Reply::Failure("no task " + Field(request, "task"));
        }
        task->cancelled->store(true);
        return Reply::Result(nullptr);
    }
    if (op == "close") {
        Close(Field(request, "entity"));
        return Reply::Result(nullptr);
    }
    if (op == "reset") {
        Reset();
        return Reply::Result(nullptr);
    }
    return Reply::Failure("unknown op " + op);
}

Json Agent::CreateClient(const Json &request) {
    const auto kind = Field(request, "kind");
    if (kind != "client") {
        throw Unsupported("creating a " + kind + " with create");
    }
    Args args(ArgsOf(request));
    const auto transport = Text(args.Required("transport"), "transport");
    const auto address   = Text(args.Required("address"), "address");
    const auto *login    = args.Optional("login");
    args.Finish();
    if (transport != "tcp") {
        throw Unsupported("the " + transport + " transport");
    }
    std::string username;
    std::string password;
    if (login != nullptr) {
        Args credentials(*login);
        username = Text(credentials.Required("username"), "username");
        password = Text(credentials.Required("password"), "password");
        credentials.Finish();
    }

    ffi::IggyClientConfig config{};
    config.server_address  = address;
    config.auto_login_kind = ffi::AutoLoginKind::Disabled;
    const auto client =
        std::make_shared<ClientEntity>(Sdk([&](ffi::ErrorInfo &) { return ffi::new_connection(std::move(config)); }));
    Sdk([&](ffi::ErrorInfo &) { client->Handle().connect(); });
    if (login != nullptr) {
        Sdk([&](ffi::ErrorInfo &info) { return client->Handle().login_user(username, password, info); });
    }
    const std::lock_guard<std::mutex> lock(mutex_);
    clients_[Field(request, "entity")] = client;
    return nullptr;
}

Json Agent::Call(const std::string &entity,
                 const std::string &method,
                 const Json &args,
                 const std::string &save_as,
                 const std::atomic<bool> &cancelled) {
    std::shared_ptr<ClientEntity> client;
    std::shared_ptr<ReaderEntity> reader;
    std::shared_ptr<ConsumerEntity> consumer;
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        if (const auto found = clients_.find(entity); found != clients_.end()) {
            client = found->second;
        }
        if (const auto found = readers_.find(entity); found != readers_.end()) {
            reader = found->second;
        }
        if (const auto found = consumers_.find(entity); found != consumers_.end()) {
            consumer = found->second;
        }
    }
    if (client) {
        return CallClient(ClientCall{*this, client->Handle(), save_as, cancelled}, method, args);
    }
    if (!reader && !consumer) {
        throw AgentFailure("no entity " + entity);
    }
    if (method != "next") {
        throw AgentFailure(method + " is not a method of a reader or a consumer");
    }
    return reader ? NextBatch(*reader->handle, args, cancelled) : NextMessage(*consumer->handle, args, cancelled);
}

Reply Agent::Start(const Json &request) {
    const auto name = Field(request, "task");
    auto cancelled  = std::make_shared<std::atomic<bool>>(false);
    auto answer     = std::make_shared<std::promise<Reply>>();
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        if (const auto previous = tasks_.find(name); previous != tasks_.end()) {
            previous->second.cancelled->store(true);
        }
        tasks_[name] = Task{cancelled, answer->get_future()};
    }
    threads_.Spawn([this, cancelled, answer, entity = Field(request, "entity"), method = Field(request, "method"),
                    args = ArgsOf(request)] {
        answer->set_value(Answer([&] { return Call(entity, method, args, "", *cancelled); }));
    });
    return Reply::Result(nullptr);
}

std::optional<Agent::Task> Agent::TakeTask(const std::string &name) {
    const std::lock_guard<std::mutex> lock(mutex_);
    const auto found = tasks_.find(name);
    if (found == tasks_.end()) {
        return std::nullopt;
    }
    auto task = std::move(found->second);
    tasks_.erase(found);
    return task;
}

void Agent::KeepReader(const std::string &entity, rust::Box<ffi::PartitionReader> reader) {
    auto kept = std::make_shared<ReaderEntity>(ReaderEntity{std::move(reader)});
    const std::lock_guard<std::mutex> lock(mutex_);
    readers_[entity] = std::move(kept);
}

void Agent::KeepConsumer(const std::string &entity, rust::Box<ffi::Consumer> consumer) {
    auto kept = std::make_shared<ConsumerEntity>(ConsumerEntity{std::move(consumer)});
    const std::lock_guard<std::mutex> lock(mutex_);
    consumers_[entity] = std::move(kept);
}

void Agent::Close(const std::string &entity) {
    std::shared_ptr<ClientEntity> client;
    std::shared_ptr<ReaderEntity> reader;
    std::shared_ptr<ConsumerEntity> consumer;
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        if (const auto found = clients_.find(entity); found != clients_.end()) {
            client = std::move(found->second);
            clients_.erase(found);
        }
        if (const auto found = readers_.find(entity); found != readers_.end()) {
            reader = std::move(found->second);
            readers_.erase(found);
        }
        if (const auto found = consumers_.find(entity); found != consumers_.end()) {
            consumer = std::move(found->second);
            consumers_.erase(found);
        }
    }
    // A close stores the offsets that the commit policy counted, so a group member resumes after the
    // messages that it handed over.
    if (consumer) {
        CloseQuietly(entity, [&](ffi::ErrorInfo &info) { consumer->handle->close(info); });
    }
    if (reader) {
        CloseQuietly(entity, [&](ffi::ErrorInfo &info) { reader->handle->close(info); });
    }
    if (client) {
        CloseQuietly(entity, [&](ffi::ErrorInfo &) { client->Handle().shutdown(); });
    }
}

void Agent::Reset() {
    std::map<std::string, Task> tasks;
    std::vector<std::string> entities;
    {
        const std::lock_guard<std::mutex> lock(mutex_);
        tasks.swap(tasks_);
        for (const auto &[entity, reader] : readers_) {
            entities.push_back(entity);
        }
        for (const auto &[entity, consumer] : consumers_) {
            entities.push_back(entity);
        }
        for (const auto &[entity, client] : clients_) {
            entities.push_back(entity);
        }
    }
    for (auto &[name, task] : tasks) {
        task.cancelled->store(true);
    }
    for (const auto &entity : entities) {
        Close(entity);
    }
}

}  // namespace agent
