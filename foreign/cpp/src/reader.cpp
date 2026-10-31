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

using detail::RethrowAsIggyException;

namespace {

std::optional<CommitEvery> CommitEveryFromFfi(const ffi::PollOptions &options) {
    if (!options.has_auto_every) {
        return std::nullopt;
    }
    switch (options.auto_every) {
        case ffi::CommitEvery::Message:
            return CommitEvery::Message();
        case ffi::CommitEvery::Messages:
            return CommitEvery::Messages(options.auto_every_messages);
        case ffi::CommitEvery::Batch:
            return CommitEvery::Batch();
    }
    return std::nullopt;
}

AutoCommit AutoCommitFromFfi(const ffi::PollOptions &options) {
    AutoCommit auto_commit;
    auto_commit.SetInterval(options.has_auto_interval
                                ? std::optional(std::chrono::microseconds(options.auto_interval_micros))
                                : std::nullopt);
    auto_commit.SetEvery(CommitEveryFromFfi(options));
    auto_commit.SetAfter(static_cast<CommitAfter>(options.auto_after));
    return auto_commit;
}

ffi::PollingStrategy ToFfiStrategy(const PollingStrategy &strategy) {
    const auto kind = strategy.Kind();
    return ffi::PollingStrategy{rust::String(kind.data(), kind.size()), strategy.Value()};
}

}  // namespace

AutoCommit::AutoCommit() {
    const auto defaults = ffi::default_poll_options();
    if (defaults.has_auto_interval) {
        interval_ = std::chrono::microseconds(defaults.auto_interval_micros);
    }
    every_ = CommitEveryFromFfi(defaults);
    after_ = static_cast<CommitAfter>(defaults.auto_after);
}

PollOptions::PollOptions() : options_(ffi::default_poll_options()) {}

AutoCommit PollOptions::AutoCommitSettings() const {
    return AutoCommitFromFfi(options_);
}

PollOptions &PollOptions::SetAutoCommit(const AutoCommit &auto_commit) {
    const auto interval        = auto_commit.Interval();
    const auto &every          = auto_commit.Every();
    options_.commit            = ffi::CommitPolicy::Auto;
    options_.has_auto_interval = interval.has_value();
    options_.auto_interval_micros =
        interval && interval->count() > 0 ? static_cast<std::uint64_t>(interval->count()) : 0;
    options_.has_auto_every      = every.has_value();
    options_.auto_every          = every ? static_cast<ffi::CommitEvery>(every->GetKind()) : ffi::CommitEvery::Message;
    options_.auto_every_messages = every ? every->Count() : 0;
    options_.auto_after          = static_cast<ffi::CommitAfter>(auto_commit.After());
    return *this;
}

PolledMessages::PolledMessages(ffi::PolledMessages batch)
    : partition_id_(batch.partition_id), current_offset_(batch.current_offset), batch_(std::move(batch)) {
    messages_.reserve(batch_.messages.size());
    for (auto &message : batch_.messages) {
        messages_.push_back(IggyMessagePolled::FromFfi(std::move(message)));
    }
    batch_.messages.clear();
}

::iggy::Checkpoint PolledMessages::Checkpoint() const {
    return ::iggy::Checkpoint(RethrowAsIggyException([this] { return batch_.checkpoint(); }));
}

::iggy::Checkpoint PolledMessages::CheckpointAt(std::size_t index) const {
    return ::iggy::Checkpoint(RethrowAsIggyException([this, index] { return batch_.checkpoint_at(index); }));
}

::iggy::Continuation PolledMessages::Continuation() const {
    return ::iggy::Continuation(batch_.continuation());
}

ReadException::ReadException(const std::string &message, ffi::ReadErrorInfo &error)
    : IggyException(message, error.error) {
    if (error.has_partition_id) {
        partition_id_ = error.partition_id;
    }
    if (!error.skip.empty()) {
        skip_ = std::shared_ptr<const ::iggy::Continuation>(new ::iggy::Continuation(error.skip[0]->copy()));
    }
}

ffi::ReaderOptions ReaderOptions::ToFfi() const {
    ffi::ReaderOptions options{};
    options.partitions.reserve(partitions_.size());
    for (const auto partition_id : partitions_) {
        options.partitions.push_back(partition_id);
    }
    if (start_) {
        options.has_start = true;
        options.start     = ToFfiStrategy(*start_);
    }
    options.has_poll_options      = poll_options_.has_value();
    options.poll_options          = poll_options_ ? poll_options_->options_ : ffi::default_poll_options();
    options.has_prefetch_bytes    = prefetch_bytes_.has_value();
    options.prefetch_bytes        = prefetch_bytes_.value_or(0);
    options.has_prefetch_messages = prefetch_messages_.has_value();
    options.prefetch_messages     = prefetch_messages_.value_or(0);
    return options;
}

std::optional<PolledMessages> PartitionReader::Next(std::chrono::microseconds timeout) {
    const auto timeout_micros = timeout.count() > 0 ? static_cast<std::uint64_t>(timeout.count()) : 0;
    ffi::ReadErrorInfo error{};
    auto batch = [this, timeout_micros, &error] {
        try {
            return reader_->next(timeout_micros, error);
        } catch (const std::exception &failure) {
            throw ReadException(failure.what(), error);
        }
    }();
    if (batch.messages.empty()) {
        return std::nullopt;
    }
    return PolledMessages(std::move(batch));
}

void PartitionReader::Commit(const Checkpoint &checkpoint) {
    RethrowAsIggyException(
        [this, &checkpoint](ffi::ErrorInfo &error_info) { reader_->commit(*checkpoint.inner_, error_info); });
}

void PartitionReader::Mark(const Checkpoint &checkpoint) {
    RethrowAsIggyException(
        [this, &checkpoint](ffi::ErrorInfo &error_info) { reader_->mark(*checkpoint.inner_, error_info); });
}

void PartitionReader::Seek(std::uint32_t partition_id, const PollingStrategy &start) {
    RethrowAsIggyException([this, partition_id, &start](ffi::ErrorInfo &error_info) {
        reader_->seek(partition_id, ToFfiStrategy(start), error_info);
    });
}

void PartitionReader::Seek(std::uint32_t partition_id, const Continuation &continuation) {
    RethrowAsIggyException([this, partition_id, &continuation](ffi::ErrorInfo &error_info) {
        reader_->seek(partition_id, *continuation.inner_, error_info);
    });
}

std::vector<std::uint32_t> PartitionReader::Partitions() const {
    const auto partitions = RethrowAsIggyException([this] { return reader_->partitions(); });
    return std::vector<std::uint32_t>(partitions.begin(), partitions.end());
}

void PartitionReader::Close() {
    RethrowAsIggyException([this](ffi::ErrorInfo &error_info) { reader_->close(error_info); });
}

std::optional<PolledMessages> PendingPoll::Wait(std::chrono::microseconds timeout) {
    const auto timeout_micros = timeout.count() > 0 ? static_cast<std::uint64_t>(timeout.count()) : 0;
    auto outcome              = RethrowAsIggyException(
        [this, timeout_micros](ffi::ErrorInfo &error_info) { return poll_->wait(timeout_micros, error_info); });
    if (!outcome.ready) {
        return std::nullopt;
    }
    return PolledMessages(std::move(outcome.batch));
}

void PendingPoll::Cancel() {
    poll_->cancel();
}

ReceivedMessage::ReceivedMessage(ffi::ReceivedMessage &received)
    : partition_id_(received.partition_id),
      current_offset_(received.current_offset),
      message_(IggyMessagePolled::FromFfi(std::move(received.message))),
      checkpoint_(received.checkpoint->copy()) {}

ffi::ConsumerOptions ConsumerOptions::ToFfi() const {
    ffi::ConsumerOptions options{};
    options.partitions.reserve(partitions_.size());
    for (const auto partition_id : partitions_) {
        options.partitions.push_back(partition_id);
    }
    if (start_) {
        options.has_start = true;
        options.start     = ToFfiStrategy(*start_);
    }
    options.has_poll_options      = poll_options_.has_value();
    options.poll_options          = poll_options_ ? poll_options_->options_ : ffi::default_poll_options();
    options.has_prefetch_bytes    = prefetch_bytes_.has_value();
    options.prefetch_bytes        = prefetch_bytes_.value_or(0);
    options.has_prefetch_messages = prefetch_messages_.has_value();
    options.prefetch_messages     = prefetch_messages_.value_or(0);
    options.allow_replay          = allow_replay_;
    return options;
}

std::optional<ReceivedMessage> IggyConsumer::Next(std::chrono::microseconds timeout) {
    const auto timeout_micros = timeout.count() > 0 ? static_cast<std::uint64_t>(timeout.count()) : 0;
    ffi::ReadErrorInfo error{};
    auto received = [this, timeout_micros, &error] {
        try {
            return consumer_->next(timeout_micros, error);
        } catch (const std::exception &failure) {
            throw ReadException(failure.what(), error);
        }
    }();
    if (received.empty()) {
        return std::nullopt;
    }
    return ReceivedMessage(received[0]);
}

void IggyConsumer::Commit(const Checkpoint &checkpoint) {
    RethrowAsIggyException(
        [this, &checkpoint](ffi::ErrorInfo &error_info) { consumer_->commit(*checkpoint.inner_, error_info); });
}

void IggyConsumer::Mark(const Checkpoint &checkpoint) {
    RethrowAsIggyException(
        [this, &checkpoint](ffi::ErrorInfo &error_info) { consumer_->mark(*checkpoint.inner_, error_info); });
}

void IggyConsumer::Seek(std::uint32_t partition_id, const PollingStrategy &start) {
    RethrowAsIggyException([this, partition_id, &start](ffi::ErrorInfo &error_info) {
        consumer_->seek(partition_id, ToFfiStrategy(start), error_info);
    });
}

void IggyConsumer::Seek(std::uint32_t partition_id, const Continuation &continuation) {
    RethrowAsIggyException([this, partition_id, &continuation](ffi::ErrorInfo &error_info) {
        consumer_->seek(partition_id, *continuation.inner_, error_info);
    });
}

void IggyConsumer::Close() {
    RethrowAsIggyException([this](ffi::ErrorInfo &error_info) { consumer_->close(error_info); });
}

PolledMessages IggyBlockingClient::PollMessages(const Consumer &consumer,
                                                const Identifier &stream,
                                                const Identifier &topic,
                                                const std::uint32_t partition_id,
                                                const PollingStrategy &strategy,
                                                const PollOptions &options) {
    return RethrowAsIggyException([&](ffi::ErrorInfo &error_info) {
        return PolledMessages(Handle()->poll_messages(stream.ToFfi(), topic.ToFfi(), partition_id,
                                                      std::string(consumer.KindName()), consumer.Id().ToFfi(),
                                                      ToFfiStrategy(strategy), options.options_, error_info));
    });
}

PolledMessages IggyBlockingClient::PollMessages(const Consumer &consumer,
                                                const Identifier &stream,
                                                const Identifier &topic,
                                                const std::uint32_t partition_id,
                                                const Continuation &continuation,
                                                const PollOptions &options) {
    return RethrowAsIggyException([&](ffi::ErrorInfo &error_info) {
        return PolledMessages(Handle()->poll_messages(stream.ToFfi(), topic.ToFfi(), partition_id,
                                                      std::string(consumer.KindName()), consumer.Id().ToFfi(),
                                                      *continuation.inner_, options.options_, error_info));
    });
}

PartitionReader IggyBlockingClient::Reader(const Consumer &consumer,
                                           const Identifier &stream,
                                           const Identifier &topic,
                                           const ReaderOptions &options) {
    return RethrowAsIggyException([&](ffi::ErrorInfo &error_info) {
        return PartitionReader(Handle()->reader(stream.ToFfi(), topic.ToFfi(), std::string(consumer.KindName()),
                                                consumer.Id().ToFfi(), options.ToFfi(), error_info));
    });
}

PendingPoll IggyBlockingClient::StartPollMessages(const Consumer &consumer,
                                                  const Identifier &stream,
                                                  const Identifier &topic,
                                                  const std::uint32_t partition_id,
                                                  const PollingStrategy &strategy,
                                                  const PollOptions &options) {
    return RethrowAsIggyException([&] {
        return PendingPoll(Handle()->start_poll_messages(stream.ToFfi(), topic.ToFfi(), partition_id,
                                                         std::string(consumer.KindName()), consumer.Id().ToFfi(),
                                                         ToFfiStrategy(strategy), options.options_));
    });
}

IggyConsumer IggyBlockingClient::StandaloneConsumer(const std::string &name,
                                                    const Identifier &stream,
                                                    const Identifier &topic,
                                                    const ConsumerOptions &options) {
    return RethrowAsIggyException([&](ffi::ErrorInfo &error_info) {
        return IggyConsumer(
            Handle()->consumer(name, stream.ToFfi(), topic.ToFfi(), false, options.ToFfi(), error_info));
    });
}

IggyConsumer IggyBlockingClient::GroupConsumer(const std::string &name,
                                               const Identifier &stream,
                                               const Identifier &topic,
                                               const ConsumerOptions &options) {
    return RethrowAsIggyException([&](ffi::ErrorInfo &error_info) {
        return IggyConsumer(Handle()->consumer(name, stream.ToFfi(), topic.ToFfi(), true, options.ToFfi(), error_info));
    });
}

}  // namespace iggy
