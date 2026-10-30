<?php
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


// Stubs for iggy-php

namespace Iggy {
    /**
     * When CommitPolicy::auto() stores. It also stores on close, on shutdown and before a
     * rebalance takes a partition away. A store only moves forward.
     */
    class AutoCommit {
        /**
         * Stores the latest counted position every `interval_micros`, when it moved, and also on
         * the event `every`. A message counts as `after` says, CommitAfter::Processing when null.
         * Throws for a zero interval, and without an interval and an event, which would store
         * only on close.
         *
         * @param mixed $interval_micros
         * @param \Iggy\CommitEvery|null $every
         * @param \Iggy\CommitAfter|null $after
         */
        public function __construct(mixed $interval_micros = null, ?\Iggy\CommitEvery $every = null, ?\Iggy\CommitAfter $after = null) {}

        /**
         * Every 10,000 messages or every 5 seconds, whichever comes first, after processing.
         *
         * @return \Iggy\AutoCommit
         */
        public static function default(): \Iggy\AutoCommit {}
    }

    /**
     * A PHP class representing the position after one delivered message.
     *
     * A commit stores its offset, so it covers every earlier message of the partition.
     */
    class Checkpoint {
        /**
         * The offset of the delivered message, which a commit stores.
         *
         * @var int
         */
        public readonly int $offset;

        public readonly int $partition_id;

        public function __construct() {}
    }

    /**
     * A PHP class representing the Iggy client.
     */
    class Client {
        /**
         * Constructs a new IggyClient from a TCP server address.
         *
         * @param string|null $conn
         * @param mixed $pending_operations
         */
        public function __construct(?string $conn = null, mixed $pending_operations = null) {}

        /**
         * Releases a retained operation without claiming whether it committed.
         *
         * @param string $operation_id
         * @return void
         */
        public function abandonOperation(string $operation_id): void {}

        /**
         * Connects the IggyClient to its service.
         *
         * @return void
         */
        public function connect(): void {}

        /**
         * Creates and initializes a standalone consumer of `partitions`, identified by `name`: it
         * stores its offsets under that consumer name and joins no group. `partitions` must not be
         * empty. The other arguments are those of consumerGroup().
         *
         * @param string $name
         * @param string $stream
         * @param string $topic
         * @param array $partitions
         * @param \Iggy\PollingStrategy|null $polling_strategy
         * @param \Iggy\PollOptions|null $poll_options
         * @param mixed $prefetch_bytes
         * @param mixed $prefetch_messages
         * @param mixed $polling_retry_interval_micros
         * @param mixed $init_retries
         * @param mixed $init_retry_interval_micros
         * @param bool $allow_replay
         * @return \Iggy\Consumer
         */
        public function consumer(string $name, string $stream, string $topic, array $partitions, ?\Iggy\PollingStrategy $polling_strategy = null, ?\Iggy\PollOptions $poll_options = null, mixed $prefetch_bytes = null, mixed $prefetch_messages = null, mixed $polling_retry_interval_micros = null, mixed $init_retries = null, mixed $init_retry_interval_micros = null, bool $allow_replay = false): \Iggy\Consumer {}

        /**
         * Creates and initializes a consumer group consumer.
         *
         * The member reads the partitions that the server assigns to it. `polling_strategy` is
         * the start of a partition without a stored offset, PollingStrategy::next() when null, and
         * a continuation is refused. `poll_options` defaults to a wait of 5 seconds for up to 1000
         * messages per request: a consumer that waits 0 polls an idle topic in a busy loop.
         * The `commit` of `poll_options` says when offsets are stored. CommitPolicy::manual(), the
         * default, stores nothing: commit checkpoints after processing, or each restart reads the
         * same messages again. With CommitPolicy::onRead() the server stores the offset of each
         * batch that it reads, and CommitPolicy::auto() stores counted positions.
         * `prefetch_bytes` and `prefetch_messages` are those of reader().
         *
         * @param string $name
         * @param string $stream
         * @param string $topic
         * @param \Iggy\PollingStrategy|null $polling_strategy
         * @param \Iggy\PollOptions|null $poll_options
         * @param mixed $prefetch_bytes
         * @param mixed $prefetch_messages
         * @param bool $create_consumer_group_if_not_exists
         * @param mixed $polling_retry_interval_micros
         * @param mixed $init_retries
         * @param mixed $init_retry_interval_micros
         * @param bool $allow_replay
         * @return \Iggy\Consumer
         */
        public function consumerGroup(string $name, string $stream, string $topic, ?\Iggy\PollingStrategy $polling_strategy = null, ?\Iggy\PollOptions $poll_options = null, mixed $prefetch_bytes = null, mixed $prefetch_messages = null, bool $create_consumer_group_if_not_exists = true, mixed $polling_retry_interval_micros = null, mixed $init_retries = null, mixed $init_retry_interval_micros = null, bool $allow_replay = false): \Iggy\Consumer {}

        /**
         * Creates a new stream.
         *
         * `encryption_key` is the stream's own data key: 32 random bytes as canonical padded
         * standard base64, for example `base64_encode(random_bytes(32))`. Raw bytes or any other
         * text are refused before anything is sent. The key travels in the request, so protecting
         * it in transit with TLS is the caller's responsibility. It is marked
         * `#[\SensitiveParameter]`, so exception traces never hold it. The server stores it wrapped
         * and never returns it, reads report only its source and fingerprints. Without a key, a
         * server with encryption enabled generates one. `raw` sends string-valued keys this
         * build does not know, and the server decides whether to accept them. It is marked
         * `#[\SensitiveParameter]` too, because an unknown key may hold a secret.
         *
         * @param string $name
         * @param string|null $encryption_key
         * @param array|null $raw
         * @return void
         */
        public function createStream(string $name, ?string $encryption_key = null, ?array $raw = null): void {}

        /**
         * Creates a topic.
         *
         * Every option left null resolves against the server default at admission. The 64-bit
         * sizes and durations take an int or a decimal string, so never-expire and unlimited
         * (18446744073709551615) can be sent. `raw` sends string-valued keys this build does not
         * know, and the server decides whether to accept them. It is marked
         * `#[\SensitiveParameter]`, because an unknown key may hold a secret.
         *
         * @param mixed $stream
         * @param string $name
         * @param int $partitions_count
         * @param string|null $compression_algorithm
         * @param mixed $message_expiry_micros
         * @param mixed $max_topic_size
         * @param mixed $segment_size
         * @param \Iggy\Durability|null $durability
         * @param \Iggy\Durability|null $consumer_offset_durability
         * @param mixed $messages_required_to_save
         * @param mixed $size_of_messages_required_to_save
         * @param bool|null $preallocate_segments
         * @param string|null $partition_resize_policy
         * @param array|null $raw
         * @return void
         */
        public function createTopic(mixed $stream, string $name, int $partitions_count, ?string $compression_algorithm = null, mixed $message_expiry_micros = null, mixed $max_topic_size = null, mixed $segment_size = null, ?\Iggy\Durability $durability = null, ?\Iggy\Durability $consumer_offset_durability = null, mixed $messages_required_to_save = null, mixed $size_of_messages_required_to_save = null, ?bool $preallocate_segments = null, ?string $partition_resize_policy = null, ?array $raw = null): void {}

        /**
         * Deletes the offset stored on `partition_id` for a standalone consumer, or for a consumer
         * group when `group` is true. Throws ConsumerOffsetNotFound (3021) when nothing is stored.
         * Prefer deleteConsumerPosition() with a checkpoint.
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param mixed $consumer
         * @param int $partition_id
         * @param bool|null $group
         * @return void
         */
        public function deleteConsumerOffset(mixed $stream, mixed $topic, mixed $consumer, int $partition_id, ?bool $group = null): void {}

        /**
         * Deletes the stored offset of the partition of a checkpoint for a standalone consumer, or
         * for a consumer group when `group` is true, under the partition history that delivered
         * the checkpoint. The offset of the checkpoint is not used.
         *
         * Throws HistoryUnavailable (87) after the partition was deleted and created again,
         * ConsumerGroupPartitionNotOwned (5009) after its consumer group owner changed, and
         * ConsumerOffsetNotFound (3021) when no offset is stored. HTTP sends no partition history,
         * so there the server deletes the offset of the history it holds.
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param mixed $consumer
         * @param \Iggy\Checkpoint $checkpoint
         * @param bool|null $group
         * @return void
         */
        public function deleteConsumerPosition(mixed $stream, mixed $topic, mixed $consumer, \Iggy\Checkpoint $checkpoint, ?bool $group = null): void {}

        /**
         * Deletes a stream by id or name.
         *
         * @param mixed $stream_id
         * @return void
         */
        public function deleteStream(mixed $stream_id): void {}

        /**
         * Deletes a topic by stream and topic id/name.
         *
         * @param mixed $stream_id
         * @param mixed $topic_id
         * @return void
         */
        public function deleteTopic(mixed $stream_id, mixed $topic_id): void {}

        /**
         * The server's option catalog for one resource: each key's kind and whether an update
         * may set or reset it, whether it is secret, read-only or critical.
         *
         * @param \Iggy\OptionsScope $scope
         * @return array
         */
        public function describeOptions(\Iggy\OptionsScope $scope): array {}

        /**
         * Fingerprint of a stream key given as canonical padded standard base64 of 32 bytes, the
         * value `StreamDetails::$encryption_key_fingerprint` reports for a stream created with it.
         * The key is marked `#[\SensitiveParameter]`, so exception traces never hold it.
         *
         * @param string $key
         * @return string
         */
        public static function encryptionKeyFingerprint(string $key): string {}

        /**
         * Constructs a new IggyClient from a connection string.
         *
         * The string is marked `#[\SensitiveParameter]`, so exception traces never hold it.
         *
         * @param string $connection_string
         * @return \Iggy\Client
         */
        public static function fromConnectionString(string $connection_string): \Iggy\Client {}

        /**
         * Returns the offset stored on `partition_id` for a standalone consumer, or for a consumer
         * group when `group` is true, or null when nothing is stored. In a cluster the node of the
         * connection answers from its own replica, which can lag a store that just returned.
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param mixed $consumer
         * @param int $partition_id
         * @param bool|null $group
         * @return \Iggy\ConsumerOffsetInfo|null
         */
        public function getConsumerOffset(mixed $stream, mixed $topic, mixed $consumer, int $partition_id, ?bool $group = null): ?\Iggy\ConsumerOffsetInfo {}

        /**
         * Reads one page of the metadata change log, or of the current state.
         *
         * Needs root or the global `manage_servers` permission. `groups` takes
         * `Iggy\MetadataChangeGroup` cases, empty for every group, and a current state scan
         * needs the parent groups of each group. `since` and `until` are microseconds as an int
         * or a decimal string, and only apply to `earliest` and `after`. `limit` is 1 to 1000,
         * 100 when null. A query the server would refuse throws `Invalid command` (code 3)
         * before anything is sent.
         *
         * @param \Iggy\MetadataChangeStart $start
         * @param array $groups
         * @param mixed $since
         * @param mixed $until
         * @param mixed $limit
         * @return \Iggy\MetadataChanges
         */
        public function getMetadataChanges(\Iggy\MetadataChangeStart $start, array $groups = [], mixed $since = null, mixed $until = null, mixed $limit = null): \Iggy\MetadataChanges {}

        /**
         * Gets a stream by id or name.
         *
         * @param mixed $stream_id
         * @return \Iggy\StreamDetails|null
         */
        public function getStream(mixed $stream_id): ?\Iggy\StreamDetails {}

        /**
         * Gets a topic by stream and topic id/name.
         *
         * @param mixed $stream_id
         * @param mixed $topic_id
         * @return \Iggy\TopicDetails|null
         */
        public function getTopic(mixed $stream_id, mixed $topic_id): ?\Iggy\TopicDetails {}

        /**
         * Builds a reader of the partitions that a consumer group assigns to this member.
         *
         * The group must exist: the reader joins it, joins again after a session loss, and leaves
         * it in close(). HTTP cannot join a group, so it throws FeatureUnavailable. The other
         * arguments are those of reader().
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param mixed $group
         * @param \Iggy\PollingStrategy|null $start
         * @param \Iggy\PollOptions|null $options
         * @param mixed $prefetch_bytes
         * @param mixed $prefetch_messages
         * @return \Iggy\PartitionReader
         */
        public function groupReader(mixed $stream, mixed $topic, mixed $group, ?\Iggy\PollingStrategy $start = null, ?\Iggy\PollOptions $options = null, mixed $prefetch_bytes = null, mixed $prefetch_messages = null): \Iggy\PartitionReader {}

        /**
         * Logs in the user with the given credentials.
         *
         * The password is marked `#[\SensitiveParameter]`, so exception traces never hold it.
         *
         * @param string $username
         * @param string $password
         * @return void
         */
        public function loginUser(string $username, string $password): void {}

        /**
         * Sends a ping request to the server.
         *
         * @return void
         */
        public function ping(): void {}

        /**
         * Polls messages from one partition of a topic, as the standalone consumer `consumer`.
         *
         * PollingStrategy::next() starts at the stored offset of `consumer`. `options` defaults to
         * `new PollOptions()`: up to 1000 messages and no wait. With CommitPolicy::onRead(),
         * the server stores the offset of the batch for `consumer`.
         *
         * To read on, pass `$polled->continuation()` as the next strategy. It keeps the partition
         * history of the batch, so a read after the partition was recreated fails with
         * HistoryUnavailable (87), and a read of another partition refuses it with InvalidCommand
         * (3).
         *
         * A fresh strategy uses the context of its route. It can fail once with
         * HistoryUnavailable (87) after another client deleted and recreated the partition. The
         * failed route is dropped, and the next call routes again.
         *
         * `consumer` polls as a standalone consumer, or as a member of the consumer group of that
         * id when `group` is true, which reads only the partitions assigned to it.
         *
         * Under CommitPolicy::onRead(), a lost reply throws FetchProgressUncertain (14010),
         * because the server can still store the offset. The partition stays blocked, and the
         * next poll of it throws PartitionLaneBlocked (14009), until abandonOperation().
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param int $partition_id
         * @param mixed $consumer
         * @param \Iggy\PollingStrategy $polling_strategy
         * @param \Iggy\PollOptions|null $options
         * @param bool|null $group
         * @return \Iggy\PolledMessages
         */
        public function pollMessages(mixed $stream, mixed $topic, int $partition_id, mixed $consumer, \Iggy\PollingStrategy $polling_strategy, ?\Iggy\PollOptions $options = null, ?bool $group = null): \Iggy\PolledMessages {}

        /**
         * Builds a reader of chosen partitions as a standalone consumer.
         *
         * `partitions` must not be empty. `start` is the start of a partition without a stored
         * offset, PollingStrategy::next() when null, and a continuation is refused. `options`
         * defaults to a wait of 5 seconds for up to 1000 messages per request: a reader that waits
         * 0 polls an idle topic in a busy loop. `prefetch_bytes`, 16 MiB when null, and
         * `prefetch_messages`, the larger of 16000 and `count` when null, bound the requests in
         * flight and the queued batches.
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param mixed $consumer
         * @param array $partitions
         * @param \Iggy\PollingStrategy|null $start
         * @param \Iggy\PollOptions|null $options
         * @param mixed $prefetch_bytes
         * @param mixed $prefetch_messages
         * @return \Iggy\PartitionReader
         */
        public function reader(mixed $stream, mixed $topic, mixed $consumer, array $partitions, ?\Iggy\PollingStrategy $start = null, ?\Iggy\PollOptions $options = null, mixed $prefetch_bytes = null, mixed $prefetch_messages = null): \Iggy\PartitionReader {}

        /**
         * Replays an exact retained operation. A committed rejection raises its original error
         * and releases its lane; uncertainty keeps it retained. HTTP is unsupported.
         * A fetch progress block raises code 14010 without replay. Only explicit
         * abandonOperation releases it, accepting the unknown server outcome.
         * Operation IDs use decimal strings to preserve the full unsigned 64-bit range.
         *
         * @param string $operation_id
         * @param mixed $timeout_micros
         * @return void
         */
        public function resolveOperation(string $operation_id, mixed $timeout_micros = null): void {}

        /**
         * Sends a command code with a payload and returns the raw response bytes.
         * Session-control codes return an invalid-command exception.
         *
         * @param int $code
         * @param string $payload
         * @return string
         */
        public function sendBinaryRequest(int $code, string $payload): string {}

        /**
         * Sends a command code with a payload to the replica with the given id, as
         * cluster metadata names it, and returns the raw response bytes.
         * Replicated commands, polls, consumer-offset reads and session-control
         * codes return an invalid-command exception, and a replica that cluster
         * metadata does not name returns an invalid-identifier exception.
         *
         * @param int $code
         * @param string $payload
         * @param int $replica_id
         * @return string
         */
        public function sendBinaryRequestToReplica(int $code, string $payload, int $replica_id): string {}

        /**
         * Sends messages to a topic and returns the commit confirmations, one per
         * partition the batch landed in.
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param int $partition_id
         * @param array $messages
         * @return \Iggy\SendMessagesResponse
         */
        public function sendMessages(mixed $stream, mixed $topic, int $partition_id, array $messages): \Iggy\SendMessagesResponse {}

        /**
         * Starts the poll of pollMessages() in the background and returns at once, so the caller
         * can wait for its batch in steps with PendingPoll::wait() or cancel it with
         * PendingPoll::cancel(). Invalid arguments throw here, a failed poll throws from wait().
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param int $partition_id
         * @param mixed $consumer
         * @param \Iggy\PollingStrategy $polling_strategy
         * @param \Iggy\PollOptions|null $options
         * @param bool|null $group
         * @return \Iggy\PendingPoll
         */
        public function startPollMessages(mixed $stream, mixed $topic, int $partition_id, mixed $consumer, \Iggy\PollingStrategy $polling_strategy, ?\Iggy\PollOptions $options = null, ?bool $group = null): \Iggy\PendingPoll {}

        /**
         * Stores `offset` on `partition_id` for a standalone consumer, or for a consumer group
         * when `group` is true, under the partition history of its route. After another client
         * deleted and created the partition again, it can throw HistoryUnavailable (87) once, and
         * the next call routes again. Prefer storeConsumerPosition() with a checkpoint.
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param mixed $consumer
         * @param int $partition_id
         * @param int $offset
         * @param bool|null $group
         * @return void
         */
        public function storeConsumerOffset(mixed $stream, mixed $topic, mixed $consumer, int $partition_id, int $offset, ?bool $group = null): void {}

        /**
         * Stores the offset of a checkpoint for a standalone consumer, or for a consumer group
         * when `group` is true, under the partition history that delivered the checkpoint, which
         * covers every earlier message of its partition.
         *
         * Throws HistoryUnavailable (87) after the partition was deleted and created again, and
         * ConsumerGroupPartitionNotOwned (5009) after its consumer group owner changed, instead of
         * storing the offset on the new partition.
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param mixed $consumer
         * @param \Iggy\Checkpoint $checkpoint
         * @param bool|null $group
         * @return void
         */
        public function storeConsumerPosition(mixed $stream, mixed $topic, mixed $consumer, \Iggy\Checkpoint $checkpoint, ?bool $group = null): void {}

        /**
         * Renames a stream.
         *
         * A null name keeps the current one. No stream option is resettable, so a non-empty
         * `reset` list, or an update that changes nothing, is refused before anything is sent.
         * `raw` and `raw_reset` reach keys this build does not know, and the server decides
         * whether each one may be set or reset. `raw` is marked `#[\SensitiveParameter]`,
         * because an unknown key may hold a secret. Renaming keeps the stream's id, key and
         * messages.
         *
         * @param mixed $stream
         * @param string|null $name
         * @param array $reset
         * @param array|null $raw
         * @param array|null $raw_reset
         * @return void
         */
        public function updateStream(mixed $stream, ?string $name = null, array $reset = [], ?array $raw = null, ?array $raw_reset = null): void {}

        /**
         * Renames a topic or changes its message expiry and size cap.
         *
         * Null arguments keep the stored values. `message_expiry_micros` and `max_topic_size`
         * take an int or a decimal string, 18446744073709551615 meaning never expire and
         * unlimited, and 0 is refused: list the key in `reset` to return it to the server
         * default. `raw` and `raw_reset` reach keys this build does not know, and the server
         * decides whether each one may be set or reset. `raw` is marked
         * `#[\SensitiveParameter]`, because an unknown key may hold a secret.
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param string|null $name
         * @param mixed $message_expiry_micros
         * @param mixed $max_topic_size
         * @param array $reset
         * @param array|null $raw
         * @param array|null $raw_reset
         * @return void
         */
        public function updateTopic(mixed $stream, mixed $topic, ?string $name = null, mixed $message_expiry_micros = null, mixed $max_topic_size = null, array $reset = [], ?array $raw = null, ?array $raw_reset = null): void {}
    }

    /**
     * When a message counts for CommitPolicy::auto().
     */
    enum CommitAfter: string {
    /**
     * When the application asks for the next message or batch, returns from its callback, or
     * closes the reader. A callback that throws leaves its message uncounted.
     */
      case Processing = 'processing';
    /**
     * When the message or the batch is handed over.
     */
      case Delivery = 'delivery';
    /**
     * Only when the application calls mark() with its checkpoint.
     */
      case Marked = 'marked';
    }

    /**
     * The event on which CommitPolicy::auto() also stores.
     */
    class CommitEvery {
        public function __construct() {}

        /**
         * The last message of each batch.
         *
         * @return \Iggy\CommitEvery
         */
        public static function batch(): \Iggy\CommitEvery {}

        /**
         * Each counted message, or each counted batch of a reader.
         *
         * @return \Iggy\CommitEvery
         */
        public static function message(): \Iggy\CommitEvery {}

        /**
         * `count` counted messages of a partition since its last store. Throws for zero.
         *
         * @param int $count
         * @return \Iggy\CommitEvery
         */
        public static function messages(int $count): \Iggy\CommitEvery {}
    }

    /**
     * When the offsets of the messages that a read returns are stored.
     */
    class CommitPolicy {
        /**
         * `manual`, `on_read` or `auto`.
         *
         * @var string
         */
        public readonly string $kind;

        public function __construct() {}

        /**
         * A reader or a consumer stores the latest counted position, at least once with
         * CommitAfter::Processing. pollMessages() refuses it with InvalidConfiguration (2).
         *
         * @param \Iggy\AutoCommit $auto
         * @return \Iggy\CommitPolicy
         */
        public static function auto(\Iggy\AutoCommit $auto): \Iggy\CommitPolicy {}

        /**
         * Nothing is stored. The application commits checkpoints. The default.
         *
         * @return \Iggy\CommitPolicy
         */
        public static function manual(): \Iggy\CommitPolicy {}

        /**
         * The server stores the offset of the last message of each read as part of the read: one
         * round trip, at most once.
         *
         * @return \Iggy\CommitPolicy
         */
        public static function onRead(): \Iggy\CommitPolicy {}
    }

    /**
     * A PHP class representing the Iggy consumer.
     */
    class Consumer {
        public function __construct() {}

        /**
         * Stores the offset of a checkpoint that this consumer delivered, which covers every
         * earlier message of its partition.
         *
         * Pass $message->checkpoint() to commit a message after later polls or a rebalance. A
         * checkpoint of an older generation of its partition, after seek() or a new assignment, or
         * at or below the last commit of its generation, stores nothing. One from a deleted or
         * recreated partition fails with HistoryUnavailable (87), and one from a previous owner
         * fails with ConsumerGroupPartitionNotOwned (5009). A checkpoint that this consumer did not
         * deliver, and every commit under CommitPolicy::onRead(), fail with InvalidCommand (3).
         *
         * @param \Iggy\Checkpoint $checkpoint
         * @return void
         */
        public function commit(\Iggy\Checkpoint $checkpoint): void {}

        /**
         * Consumes messages with a PHP callback, and returns how many it consumed.
         *
         * The callback is called as callback(ReceiveMessage $message). A finite limit is required.
         * The run ends after `limit` messages, after shutdown(), or with an exception.
         *
         * Under CommitPolicy::auto(), a message counts before its callback with
         * CommitAfter::Delivery, and after its callback returns with CommitAfter::Processing. An
         * exception of the callback ends the run, and consumeMessages() throws it. Its message
         * then stays uncounted under CommitAfter::Processing, so the next run reads it again.
         *
         * The consumer retries a failed read by itself and does not throw it. An error that ends
         * the run throws Iggy\Exception\ReadException: HistoryUnavailable (87), which stops its
         * partition until seek(), and under CommitPolicy::onRead() an unknown outcome, which
         * stops its partition until its operation is abandoned and the partition is sought. A
         * blocked lane, a shut down client and any error while the client cannot reconnect by
         * itself also end the run.
         *
         * With `idle_timeout_micros`, a run that gets no message for that long ends and returns
         * the count of the messages it consumed, so a single-threaded worker regains control on an
         * idle topic. Null waits without a limit.
         *
         * The callback can call commit(), mark(), storeOffset(), deleteOffset() and the getters. seek(),
         * shutdown(), iterMessages() and consumeMessages() throw in the callback, because the run
         * holds the consumer.
         *
         * @param callable $callback
         * @param int $limit
         * @param mixed $idle_timeout_micros
         * @return int
         */
        public function consumeMessages(callable $callback, int $limit, mixed $idle_timeout_micros = null): int {}

        /**
         * Deletes the stored offset for the provided partition id.
         *
         * If partition_id is null, at least one message must have been polled first.
         *
         * The delete uses the context of its route. It can fail once with HistoryUnavailable
         * (87), or ConsumerGroupPartitionNotOwned (5009), after another client deleted and
         * recreated the partition. The failed route is dropped, and the next call routes again.
         *
         * @param int|null $partition_id
         * @return void
         */
        public function deleteOffset(?int $partition_id = null): void {}

        /**
         * Get the last consumed offset or null if no offset has been consumed yet.
         *
         * @param int $partition_id
         * @return int|null
         */
        public function getLastConsumedOffset(int $partition_id): ?int {}

        /**
         * Get the last stored offset or null if no offset has been stored yet.
         *
         * @param int $partition_id
         * @return int|null
         */
        public function getLastStoredOffset(int $partition_id): ?int {}

        /**
         * Returns an iterator over messages for use with foreach.
         *
         * With `idle_timeout_micros`, the iteration ends when no message arrives for that long,
         * and a later foreach over the same iterator reads on. Null waits without a limit.
         *
         * Under CommitPolicy::auto(), a message counts when the iterator yields it with
         * CommitAfter::Delivery, and when the iterator moves past it with CommitAfter::Processing. A
         * failed read throws Iggy\Exception\ReadException. The consumer stays usable and retries by
         * itself. HistoryUnavailable (87) stops its partition until seek().
         *
         * @param mixed $idle_timeout_micros
         * @return \Iggy\MessageIterator
         */
        public function iterMessages(mixed $idle_timeout_micros = null): \Iggy\MessageIterator {}

        /**
         * Counts a checkpoint that this consumer delivered under CommitPolicy::auto() and stores
         * nothing now: the policy stores the latest counted position of its partition. With
         * CommitAfter::Marked only marks count. It works also in a consumeMessages() callback.
         * Without CommitPolicy::auto(), and for a checkpoint that this consumer did not deliver,
         * it throws InvalidCommand (3).
         *
         * @param \Iggy\Checkpoint $checkpoint
         * @return void
         */
        public function mark(\Iggy\Checkpoint $checkpoint): void {}

        /**
         * Gets the name of the consumer group.
         *
         * @return string
         */
        public function name(): string {}

        /**
         * Gets the current partition id or 0 if no messages have been polled yet.
         *
         * @return int
         */
        public function partitionId(): int {}

        /**
         * Sets the next read position of a partition and drops the rest of its current batch. It
         * stores no offset.
         *
         * It also resumes a partition that stopped, for example after HistoryUnavailable (87), or
         * after a message that does not decode: seek to the `skip` of its ReadException. A
         * partition that the consumer does not read, and a continuation of another partition, fail
         * with InvalidCommand (3). Under CommitPolicy::onRead(), a read of the partition with
         * an unknown outcome fails with PartitionLaneBlocked (14009): abandon its operation first.
         *
         * @param int $partition_id
         * @param \Iggy\PollingStrategy $start
         * @return void
         */
        public function seek(int $partition_id, \Iggy\PollingStrategy $start): void {}

        /**
         * Stops the consumer after its queued offset stores.
         *
         * Under CommitPolicy::auto(), it stores the latest counted position of each partition. With
         * CommitPolicy::onRead(), it moves the offset of each
         * partition back to the last delivered message, so the next run reads the prefetched
         * messages again. A group member then leaves its group. Afterwards consumeMessages()
         * returns at once and iterMessages() ends, and a second call does nothing. Destroying the
         * consumer without shutdown() sends only the queued stores and does not leave the group.
         *
         * The first failed store of the OnRead rewind is thrown. Otherwise a partition with an
         * unknown outcome throws PartitionLaneBlocked (14009): abandon its operation before a new
         * consumer reads the partition.
         *
         * @return void
         */
        public function shutdown(): void {}

        /**
         * Stores the provided offset for the provided partition id.
         *
         * If partition_id is null, at least one message must have been polled first.
         *
         * It always sends, also an offset below the stored one. The offset is committed under the
         * partition context of the latest message consumed from that partition. After the
         * partition was recreated or changed owner, that context can be newer than the message's,
         * and the offset then lands on the newer partition. Use commit($message->checkpoint()) to
         * commit under the context that delivered the message.
         *
         * When nothing was consumed from that partition, the store uses the context of its route
         * and can fail once with HistoryUnavailable (87), or ConsumerGroupPartitionNotOwned
         * (5009), after another client deleted and recreated the partition. The failed route is
         * dropped, and the next call routes again.
         *
         * @param int $offset
         * @param int|null $partition_id
         * @return void
         */
        public function storeOffset(int $offset, ?int $partition_id = null): void {}

        /**
         * Gets the stream identifier this consumer is configured for.
         *
         * @return string
         */
        public function stream(): string {}

        /**
         * Gets the topic identifier this consumer is configured for.
         *
         * @return string
         */
        public function topic(): string {}
    }

    /**
     * The offset that a consumer stored on a partition.
     */
    class ConsumerOffsetInfo {
        /**
         * The offset of the last message of the partition when the offset was read.
         *
         * @var int
         */
        public readonly int $current_offset;

        /**
         * The partition of the offset.
         *
         * @var int
         */
        public readonly int $partition_id;

        /**
         * The stored offset.
         *
         * @var int
         */
        public readonly int $stored_offset;

        public function __construct() {}
    }

    enum Durability: string {
      case Replicated = 'replicated';
      case Persisted = 'persisted';
    }

    class MessageIterator implements \Iterator {
        public function __construct() {}

        /**
         * @return \Iggy\ReceiveMessage|null
         */
        public function current(): ?\Iggy\ReceiveMessage {}

        /**
         * @return int
         */
        public function key(): int {}

        /**
         * @return void
         */
        public function next(): void {}

        /**
         * Reads the first message, and on a started iterator the message after the last one it
         * yielded, as a stream has no earlier messages to go back to.
         *
         * @return void
         */
        public function rewind(): void {}

        /**
         * @return bool
         */
        public function valid(): bool {}
    }

    /**
     * Position of the last change record a reader consumed or scanned. Send it back in
     * `MetadataChangeStart::after()` to read the records after it.
     *
     * Casting it to string gives its `mc1_` text, which `parse()` reads back.
     */
    class MetadataChangeCursor {
        /**
         * Prepare checksum of `op` as a decimal string.
         *
         * @var string
         */
        public readonly string $checksum;

        /**
         * Prepare checksum of op 1 as a decimal string, which names the history the cursor
         * belongs to.
         *
         * @var string
         */
        public readonly string $log_id;

        /**
         * The operation of the last record, 0 before the first record.
         *
         * @var mixed
         */
        public readonly mixed $op;

        /**
         * Position of the record within `op`.
         *
         * @var int
         */
        public readonly int $ordinal;

        /**
         * Builds a cursor from its fields: 128-bit values as decimal strings, `op` as an int or
         * a decimal string.
         *
         * @param string $log_id
         * @param mixed $op
         * @param string $checksum
         * @param int $ordinal
         */
        public function __construct(string $log_id, mixed $op, string $checksum, int $ordinal) {}

        /**
         * @return string
         */
        public function __toString(): string {}

        /**
         * Reads a cursor from its `mc1_` text. Anything else throws `Invalid command` (code 3).
         *
         * @param string $text
         * @return \Iggy\MetadataChangeCursor
         */
        public static function parse(string $text): \Iggy\MetadataChangeCursor {}
    }

    /**
     * Family of change records and the unit of filtering. Values are the HTTP API names.
     */
    enum MetadataChangeGroup: string {
      case Streams = 'streams';
      case Topics = 'topics';
      case Partitions = 'partitions';
      case ConsumerGroups = 'consumer_groups';
      case ConsumerGroupMembers = 'consumer_group_members';
      case Users = 'users';
      case PersonalAccessTokens = 'personal_access_tokens';
      case Sessions = 'sessions';
    }

    /**
     * Kind of a change record. Values are the HTTP API names.
     */
    enum MetadataChangeKind: string {
      case StreamCreated = 'stream_created';
      case StreamUpdated = 'stream_updated';
      case StreamDeleted = 'stream_deleted';
      case TopicCreated = 'topic_created';
      case TopicUpdated = 'topic_updated';
      case TopicDeleted = 'topic_deleted';
      case PartitionsChanged = 'partitions_changed';
      case ConsumerGroupCreated = 'consumer_group_created';
      case ConsumerGroupDeleted = 'consumer_group_deleted';
      case ConsumerGroupMemberJoined = 'consumer_group_member_joined';
      case ConsumerGroupMemberLeft = 'consumer_group_member_left';
      case ConsumerGroupPartitionHandedOver = 'consumer_group_partition_handed_over';
      case UserCreated = 'user_created';
      case UserUpdated = 'user_updated';
      case UserDeleted = 'user_deleted';
      case UserPasswordChanged = 'user_password_changed';
      case UserPermissionsUpdated = 'user_permissions_updated';
      case PersonalAccessTokenCreated = 'personal_access_token_created';
      case PersonalAccessTokenDeleted = 'personal_access_token_deleted';
      case SessionRegistered = 'session_registered';
      case SessionClosed = 'session_closed';
    }

    /**
     * Whether a page carries change records or current state items.
     */
    enum MetadataChangePhase: string {
      case Changes = 'changes';
      case Current = 'current';
    }

    /**
     * One change record: the committed operation that produced it, its position within that
     * operation and the typed body of what is true after it.
     */
    class MetadataChangeRecord {
        /**
         * The acting user, null for server actions.
         *
         * @var int|null
         */
        public readonly ?int $actor_user_id;

        /**
         * The typed body `kind` names, for example a `MetadataStreamChange` for
         * `StreamCreated` and `StreamUpdated`, or a `MetadataUnknownChange`.
         *
         * @var mixed
         */
        public readonly mixed $change;

        /**
         * Prepare checksum of `op` as a decimal string.
         *
         * @var string
         */
        public readonly string $checksum;

        /**
         * The group, null for a record this build does not know.
         *
         * @var \Iggy\MetadataChangeGroup|null
         */
        public readonly ?\Iggy\MetadataChangeGroup $group;

        /**
         * The kind, null for a record this build does not know.
         *
         * @var \Iggy\MetadataChangeKind|null
         */
        public readonly ?\Iggy\MetadataChangeKind $kind;

        /**
         * The metadata operation that produced the record.
         *
         * @var mixed
         */
        public readonly mixed $op;

        /**
         * Position of the record within `op`.
         *
         * @var int
         */
        public readonly int $ordinal;

        /**
         * Prepare timestamp of `op` in microseconds.
         *
         * @var mixed
         */
        public readonly mixed $timestamp;

        public function __construct() {}
    }

    /**
     * Where `Client::getMetadataChanges()` starts reading.
     */
    class MetadataChangeStart {
        public function __construct() {}

        /**
         * The records after `cursor`.
         *
         * @param \Iggy\MetadataChangeCursor $cursor
         * @return \Iggy\MetadataChangeStart
         */
        public static function after(\Iggy\MetadataChangeCursor $cursor): \Iggy\MetadataChangeStart {}

        /**
         * Current state pages: null starts a scan, a token continues it.
         *
         * @param \Iggy\MetadataCurrentPageToken|null $page_token
         * @return \Iggy\MetadataChangeStart
         */
        public static function current(?\Iggy\MetadataCurrentPageToken $page_token = null): \Iggy\MetadataChangeStart {}

        /**
         * The oldest operation the requested groups still hold completely.
         *
         * @return \Iggy\MetadataChangeStart
         */
        public static function earliest(): \Iggy\MetadataChangeStart {}

        /**
         * No records, only a cursor at the newest retained record.
         *
         * @return \Iggy\MetadataChangeStart
         */
        public static function latest(): \Iggy\MetadataChangeStart {}
    }

    /**
     * Outcome of a change log query.
     *
     * `Ok`: apply the records and keep `next`. `Behind`: the node has not applied the position
     * yet, ask again later or ask another node. `Expired` and `Reset`: rebuild from current state.
     */
    enum MetadataChangeStatus: string {
      case Ok = 'ok';
      case Behind = 'behind';
      case Expired = 'expired';
      case Reset = 'reset';
    }

    /**
     * One page of the metadata change log, or of the current state, as
     * `Client::getMetadataChanges()` returns it.
     *
     * Unsigned 64-bit values are ints, or decimal strings above `PHP_INT_MAX`. 128-bit values
     * (log ids, checksums, client ids) are decimal strings.
     */
    class MetadataChanges {
        /**
         * Continuation of a current state scan, null once it is complete.
         *
         * @var \Iggy\MetadataCurrentPageToken|null
         */
        public readonly ?\Iggy\MetadataCurrentPageToken $current_page_token;

        /**
         * More entries remain after `next`, or another state page exists.
         *
         * @var bool
         */
        public readonly bool $has_more;

        /**
         * Highest metadata operation the answering node applied.
         *
         * @var mixed
         */
        public readonly mixed $head_op;

        /**
         * Current state items, empty on change pages.
         *
         * @var array
         */
        public readonly array $items;

        /**
         * This node's log id as a decimal string, null before it applied op 1.
         *
         * @var string|null
         */
        public readonly ?string $log_id;

        /**
         * The cursor to send back. Present on `Ok`, echoed on `Behind`, null on `Expired` and
         * `Reset`. The last state page returns the anchor to follow from.
         *
         * @var \Iggy\MetadataChangeCursor|null
         */
        public readonly ?\Iggy\MetadataChangeCursor $next;

        /**
         * Lowest whole operation from which the requested groups are complete.
         *
         * @var mixed
         */
        public readonly mixed $oldest_op;

        /**
         * `Changes` for `earliest`, `latest` and `after`, `Current` for state pages.
         *
         * @var \Iggy\MetadataChangePhase
         */
        public readonly \Iggy\MetadataChangePhase $phase;

        /**
         * Change records in `(op, ordinal)` order, empty on state pages.
         *
         * @var array
         */
        public readonly array $records;

        /**
         * The requested groups are complete for every timestamp above this value in
         * microseconds, 0 when nothing was evicted.
         *
         * @var mixed
         */
        public readonly mixed $retained_since;

        public readonly \Iggy\MetadataChangeStatus $status;

        public function __construct() {}

        /**
         * Decodes the response payload of the raw `metadata.changes` command (code 17), as
         * `Client::sendBinaryRequest()` returns it.
         *
         * @param string $payload
         * @return \Iggy\MetadataChanges
         */
        public static function decode(string $payload): \Iggy\MetadataChanges {}

        /**
         * Reads the JSON body `GET /metadata/changes` returns.
         *
         * @param string $json
         * @return \Iggy\MetadataChanges
         */
        public static function fromJson(string $json): \Iggy\MetadataChanges {}
    }

    /**
     * A created consumer group with the topic's resulting group count.
     */
    class MetadataConsumerGroupCreated {
        /**
         * The topic's consumer group count after the change.
         *
         * @var int
         */
        public readonly int $consumer_groups_count;

        public readonly int $group_id;

        public readonly string $name;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A removed consumer group with the topic's resulting group count.
     */
    class MetadataConsumerGroupDeleted {
        /**
         * The topic's consumer group count after the change.
         *
         * @var int
         */
        public readonly int $consumer_groups_count;

        public readonly int $group_id;

        public readonly string $name;

        /**
         * The operation of the delete request when the delete completed later, else null.
         *
         * @var mixed
         */
        public readonly mixed $requested_op;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A `consumer_groups` current state item.
     */
    class MetadataConsumerGroupItem {
        public readonly int $group_id;

        public readonly string $name;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * One current state item of a `MetadataChangeStart::current()` page.
     */
    class MetadataCurrentItem {
        /**
         * The typed body `group` names, or a `MetadataUnknownItem`.
         *
         * @var mixed
         */
        public readonly mixed $body;

        /**
         * The group, null for an item this build does not know.
         *
         * @var \Iggy\MetadataChangeGroup|null
         */
        public readonly ?\Iggy\MetadataChangeGroup $group;

        public function __construct() {}
    }

    /**
     * Opaque continuation of a current state scan. Send it back unchanged in
     * `MetadataChangeStart::current()` with the same groups.
     *
     * Casting it to string gives its `cs1_` text, which `parse()` reads back.
     */
    class MetadataCurrentPageToken {
        /**
         * The cursor captured before the scan's first page.
         *
         * @var \Iggy\MetadataChangeCursor
         */
        public readonly \Iggy\MetadataChangeCursor $anchor;

        /**
         * The groups the scan was started with, in code order.
         *
         * @var array
         */
        public readonly array $groups;

        /**
         * The group of the last item returned.
         *
         * @var \Iggy\MetadataChangeGroup
         */
        public readonly \Iggy\MetadataChangeGroup $last_group;

        /**
         * The key of the last item returned, in the page token key layout.
         *
         * @var string
         */
        public readonly string $last_key;

        /**
         * The highest head any page of the scan reported so far.
         *
         * @var mixed
         */
        public readonly mixed $required_head;

        public function __construct() {}

        /**
         * @return string
         */
        public function __toString(): string {}

        /**
         * Reads a page token from its `cs1_` text. Anything else throws `Invalid command`
         * (code 3).
         *
         * @param string $text
         * @return \Iggy\MetadataCurrentPageToken
         */
        public static function parse(string $text): \Iggy\MetadataCurrentPageToken {}
    }

    /**
     * A `consumer_group_members` current state item.
     */
    class MetadataMemberItem {
        /**
         * The member's client id as a decimal string.
         *
         * @var string
         */
        public readonly string $client_id;

        public readonly int $group_id;

        public readonly int $member_id;

        /**
         * Installed partitions, ascending.
         *
         * @var array
         */
        public readonly array $partition_ids;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A member that joined a consumer group.
     */
    class MetadataMemberJoined {
        /**
         * The member's client id as a decimal string.
         *
         * @var string
         */
        public readonly string $client_id;

        public readonly int $group_id;

        public readonly int $member_id;

        public readonly int $members_count;

        /**
         * Installed partitions, normally none because a handover installs them.
         *
         * @var array
         */
        public readonly array $partition_ids;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A member that left a consumer group.
     */
    class MetadataMemberLeft {
        /**
         * The member's client id as a decimal string.
         *
         * @var string
         */
        public readonly string $client_id;

        public readonly int $group_id;

        public readonly int $member_id;

        public readonly int $members_count;

        public readonly \Iggy\MetadataMemberLeftReason $reason;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * Why a consumer group member was removed.
     */
    enum MetadataMemberLeftReason: string {
      case Left = 'left';
      case SessionEnded = 'session_ended';
    }

    /**
     * A completed partition handover.
     */
    class MetadataPartitionHandedOver {
        public readonly int $group_id;

        /**
         * The new owner, null when the last member left.
         *
         * @var int|null
         */
        public readonly ?int $member_id;

        public readonly int $partition_id;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A partition count change.
     */
    class MetadataPartitionsChanged {
        /**
         * The partition count after the change.
         *
         * @var int
         */
        public readonly int $partitions_count;

        /**
         * The deleted partition ids, ascending, empty on create.
         *
         * @var array
         */
        public readonly array $removed_partition_ids;

        /**
         * The operation of the delete request when the delete completed later, else null.
         *
         * @var mixed
         */
        public readonly mixed $requested_op;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A `partitions` current state item.
     */
    class MetadataPartitionsItem {
        public readonly int $partitions_count;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * Why a user's public permission set changed.
     */
    enum MetadataPermissionsUpdatedReason: string {
      case Updated = 'updated';
      case ResourceDeleted = 'resource_deleted';
    }

    /**
     * A registered session, and a `sessions` current state item.
     */
    class MetadataSession {
        /**
         * The session's client id as a decimal string.
         *
         * @var string
         */
        public readonly string $client_id;

        /**
         * The session epoch.
         *
         * @var mixed
         */
        public readonly mixed $session;

        public readonly mixed $user_created_at;

        public readonly int $user_id;

        public function __construct() {}
    }

    /**
     * A closed session.
     */
    class MetadataSessionClosed {
        /**
         * The session's client id as a decimal string.
         *
         * @var string
         */
        public readonly string $client_id;

        public readonly \Iggy\MetadataSessionClosedReason $reason;

        /**
         * The session epoch.
         *
         * @var mixed
         */
        public readonly mixed $session;

        public readonly mixed $user_created_at;

        public readonly int $user_id;

        public function __construct() {}
    }

    /**
     * Why a session was closed.
     */
    enum MetadataSessionClosedReason: string {
      case Logout = 'logout';
      case Expired = 'expired';
    /**
     * The user was deactivated or deleted.
     */
      case Revoked = 'revoked';
    }

    /**
     * A stream after a create or update, and a `streams` current state item.
     */
    class MetadataStreamChange {
        /**
         * Stream creation time in microseconds.
         *
         * @var mixed
         */
        public readonly mixed $created_at;

        /**
         * Options the server filled in, including the key source and fingerprints.
         *
         * @var array|null
         */
        public readonly ?array $derived_options;

        public readonly string $name;

        /**
         * Options the creating client set, as `StreamDetails::$options` returns them, or null
         * when `options_unreadable`. Key material is never returned.
         *
         * @var array|null
         */
        public readonly ?array $options;

        /**
         * The server could not read the stream's stored options. Keep the previous values.
         *
         * @var bool
         */
        public readonly bool $options_unreadable;

        /**
         * The operation that created the stream, which tells it from an earlier stream with
         * the same id.
         *
         * @var mixed
         */
        public readonly mixed $stream_created_op;

        /**
         * Stream id. Ids are reused after a delete.
         *
         * @var int
         */
        public readonly int $stream_id;

        public function __construct() {}
    }

    /**
     * A removed stream.
     */
    class MetadataStreamDeleted {
        public readonly mixed $created_at;

        public readonly string $name;

        /**
         * The operation of the delete request when the delete completed later, else null.
         *
         * @var mixed
         */
        public readonly mixed $requested_op;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public function __construct() {}
    }

    /**
     * A created personal access token, and a `personal_access_tokens` current state item.
     * Never the token itself.
     */
    class MetadataTokenChange {
        /**
         * Expiry in microseconds, null for a token that never expires.
         *
         * @var mixed
         */
        public readonly mixed $expiry;

        public readonly string $name;

        public readonly mixed $user_created_at;

        public readonly int $user_id;

        public function __construct() {}
    }

    /**
     * A removed personal access token.
     */
    class MetadataTokenDeleted {
        public readonly string $name;

        public readonly \Iggy\MetadataTokenDeletedReason $reason;

        public readonly mixed $user_created_at;

        public readonly int $user_id;

        public function __construct() {}
    }

    /**
     * Why a personal access token was removed.
     */
    enum MetadataTokenDeletedReason: string {
      case Deleted = 'deleted';
      case Expired = 'expired';
    }

    /**
     * A topic after a create or update.
     */
    class MetadataTopicChange {
        public readonly mixed $created_at;

        /**
         * Options the server filled in from its catalog defaults.
         *
         * @var array|null
         */
        public readonly ?array $derived_options;

        public readonly string $name;

        /**
         * Options the creating or updating client set, or null when `options_unreadable`.
         *
         * @var array|null
         */
        public readonly ?array $options;

        /**
         * The server could not read the topic's stored options. Keep the previous values.
         *
         * @var bool
         */
        public readonly bool $options_unreadable;

        public readonly int $partitions_count;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        /**
         * The operation that created the topic, which tells it from an earlier topic with the
         * same id.
         *
         * @var mixed
         */
        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A removed topic.
     */
    class MetadataTopicDeleted {
        public readonly mixed $created_at;

        public readonly string $name;

        /**
         * The operation of the delete request when the delete completed later, else null.
         *
         * @var mixed
         */
        public readonly mixed $requested_op;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A `topics` current state item. Partition counts come as `partitions` items.
     */
    class MetadataTopicItem {
        public readonly mixed $created_at;

        /**
         * Options the server filled in from its catalog defaults.
         *
         * @var array|null
         */
        public readonly ?array $derived_options;

        public readonly string $name;

        /**
         * Options the creating or updating client set, or null when `options_unreadable`.
         *
         * @var array|null
         */
        public readonly ?array $options;

        /**
         * The server could not read the topic's stored options.
         *
         * @var bool
         */
        public readonly bool $options_unreadable;

        public readonly mixed $stream_created_op;

        public readonly int $stream_id;

        public readonly mixed $topic_created_op;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A record of a group or kind this build does not know. A mirror stops on it rather than
     * skip a possible state change.
     */
    class MetadataUnknownChange {
        /**
         * The raw body on binary transports, or the record's whole JSON object over HTTP.
         *
         * @var string
         */
        public readonly string $body;

        /**
         * The group code on binary transports, or the group name over HTTP.
         *
         * @var mixed
         */
        public readonly mixed $group;

        /**
         * The kind code on binary transports, or the kind name over HTTP.
         *
         * @var mixed
         */
        public readonly mixed $kind;

        public function __construct() {}
    }

    /**
     * A current state item of a group this build does not know.
     */
    class MetadataUnknownItem {
        /**
         * The raw body on binary transports, or the item's whole JSON object over HTTP.
         *
         * @var string
         */
        public readonly string $body;

        /**
         * The group code on binary transports, or the group name over HTTP.
         *
         * @var mixed
         */
        public readonly mixed $group;

        public function __construct() {}
    }

    /**
     * A user after a create or update, and a `users` current state item.
     */
    class MetadataUserChange {
        /**
         * Options the server filled in.
         *
         * @var array|null
         */
        public readonly ?array $derived_options;

        /**
         * Options the creating client set, or null when `options_unreadable`.
         *
         * @var array|null
         */
        public readonly ?array $options;

        /**
         * The server could not read the user's stored options.
         *
         * @var bool
         */
        public readonly bool $options_unreadable;

        /**
         * The public permission set in the shape `GET /users/{id}` returns, null when the
         * user has none.
         *
         * @var array|null
         */
        public readonly ?array $permissions;

        /**
         * `active` or `inactive`.
         *
         * @var string
         */
        public readonly string $status;

        /**
         * User creation time in microseconds, which tells the user from an earlier user with
         * the same id.
         *
         * @var mixed
         */
        public readonly mixed $user_created_at;

        /**
         * User id. Ids are reused after a delete.
         *
         * @var int
         */
        public readonly int $user_id;

        public readonly string $username;

        public function __construct() {}
    }

    /**
     * A deleted user, or one whose password changed.
     */
    class MetadataUserIdentity {
        public readonly mixed $user_created_at;

        public readonly int $user_id;

        public readonly string $username;

        public function __construct() {}
    }

    /**
     * A user's public permission set after a change.
     */
    class MetadataUserPermissionsUpdated {
        /**
         * The public permission set after the change, null when the user has none.
         *
         * @var array|null
         */
        public readonly ?array $permissions;

        public readonly \Iggy\MetadataPermissionsUpdatedReason $reason;

        public readonly mixed $user_created_at;

        public readonly int $user_id;

        public readonly string $username;

        public function __construct() {}
    }

    /**
     * One option key as the server's catalog describes it.
     */
    class OptionSpec {
        /**
         * A node that does not understand the key must not serve the resource.
         *
         * @var bool
         */
        public readonly bool $critical;

        /**
         * The default in the kind's binary encoding, an empty string when the key has none.
         *
         * @var string
         */
        public readonly string $default_value;

        public readonly string $description;

        public readonly string $key;

        /**
         * The value kind, for example `string`, `uint64` or `raw`.
         *
         * @var string
         */
        public readonly string $kind;

        /**
         * Computed by the server and returned on reads, never accepted.
         *
         * @var bool
         */
        public readonly bool $read_only;

        /**
         * An update may reset the key to its default.
         *
         * @var bool
         */
        public readonly bool $resettable;

        /**
         * Accepted on create, never returned by a read.
         *
         * @var bool
         */
        public readonly bool $secret;

        /**
         * An update may set the key.
         *
         * @var bool
         */
        public readonly bool $updatable;

        public function __construct() {}
    }

    /**
     * The resource whose option catalog `Client::describeOptions()` reads.
     */
    enum OptionsScope: string {
      case Topic = 'topic';
      case Stream = 'stream';
      case User = 'user';
    }

    /**
     * Reads the partitions of a topic in batches, with routing, prefetch and the position of each
     * partition inside. Build one with Client::reader() or Client::groupReader().
     *
     * Each partition delivers its batches in offset order, from its first read, from each seek()
     * and from each new assignment to a group member. The `commit` of its PollOptions says when
     * offsets are stored. With CommitPolicy::manual() and CommitPolicy::auto(), commit() stores the
     * offset of a checkpoint, and CommitPolicy::auto() also stores the latest counted position by
     * itself, which mark() counts under CommitAfter::Marked. With CommitPolicy::onRead(), the server
     * stores the offset of each reply, and close() moves the offset back to the last delivered
     * message. Destroying the reader without
     * close() stops it without waiting, moves no offset back and does not leave the group.
     */
    class PartitionReader {
        public function __construct() {}

        /**
         * Stops the reader. It waits at most until the deadlines of the reads in flight. Under
         * CommitPolicy::manual() it commits nothing, and under CommitPolicy::auto() it stores the
         * latest counted position of each partition. A group reader then leaves its group. A second call does nothing, and
         * every other method of a closed reader throws.
         *
         * Under CommitPolicy::onRead(), it moves the offset of each partition with undelivered
         * batches back to its last delivered message, and the first failed store is thrown.
         * Otherwise a partition with an unknown outcome throws PartitionLaneBlocked (14009):
         * abandon its operation before a new reader reads the partition.
         *
         * @return void
         */
        public function close(): void {}

        /**
         * Stores the offset of a checkpoint that this reader delivered. The commits of one
         * partition go out in order.
         *
         * A checkpoint of an older generation of its partition, or at or below the last commit of
         * its generation, stores nothing. A checkpoint that this reader did not deliver, and every
         * commit under CommitPolicy::onRead(), fail with InvalidCommand (3).
         *
         * @param \Iggy\Checkpoint $checkpoint
         * @return void
         */
        public function commit(\Iggy\Checkpoint $checkpoint): void {}

        /**
         * Counts a checkpoint that this reader delivered under CommitPolicy::auto() and stores
         * nothing now: the policy stores the latest counted position of its partition. Without
         * CommitPolicy::auto(), and for a checkpoint that this reader did not deliver, it throws
         * InvalidCommand (3).
         *
         * @param \Iggy\Checkpoint $checkpoint
         * @return void
         */
        public function mark(\Iggy\Checkpoint $checkpoint): void {}

        /**
         * The next batch of one partition. It is never empty. Without `timeout_micros`, the call
         * blocks until a batch arrives. With it, the call returns null when no batch arrives in
         * that time, and a later batch stays in the reader for the next call. A timeout of 0 does
         * not wait: it returns a batch only if one has already arrived.
         *
         * A failed read throws Iggy\Exception\ReadException. The reader stays usable and retries
         * by itself. HistoryUnavailable (87) stops its partition until seek(). Under
         * CommitPolicy::onRead(), an unknown outcome stops its partition until the operation is
         * abandoned on the client and the partition is sought.
         *
         * @param mixed $timeout_micros
         * @return \Iggy\PolledMessages|null
         */
        public function next(mixed $timeout_micros = null): ?\Iggy\PolledMessages {}

        /**
         * The partitions that the reader reads: those of a standalone consumer, or the current
         * assignment of a group member.
         *
         * @return array
         */
        public function partitions(): array {}

        /**
         * Sets the next read position of a partition and starts a new generation of it. It stores
         * no offset, drops the queued batches of the partition and ignores its late replies. It
         * also resumes a stopped partition.
         *
         * A partition that the reader does not read, and a continuation of another partition,
         * fail with InvalidCommand (3). Under CommitPolicy::onRead(), the new position waits
         * for the end of the committing read in flight, and a read with an unknown outcome fails
         * with PartitionLaneBlocked (14009): abandon its operation first.
         *
         * @param int $partition_id
         * @param \Iggy\PollingStrategy $start
         * @return void
         */
        public function seek(int $partition_id, \Iggy\PollingStrategy $start): void {}
    }

    /**
     * Client-wide limits for active and retained mutation frames, including queued writer bytes.
     */
    class PendingOperationsConfig {
        public readonly int $max_bytes;

        public readonly int $max_operations;

        /**
         * Defaults to 256 operations and 64 MiB of headers and bodies.
         *
         * @param mixed $max_operations
         * @param mixed $max_bytes
         */
        public function __construct(mixed $max_operations = null, mixed $max_bytes = null) {}
    }

    /**
     * A poll that runs in the background, from Client::startPollMessages(). Wait for its batch in
     * steps, or cancel it. Destroying it cancels the poll.
     */
    class PendingPoll {
        public function __construct() {}

        /**
         * Drops the request. A parked deferred poll is then cancelled on the server, and one with
         * CommitPolicy::onRead() that the server had not answered stores nothing. It returns after
         * the request was dropped and does nothing after the poll ended.
         *
         * @return void
         */
        public function cancel(): void {}

        /**
         * The batch of the poll. Without `timeout_micros`, the call blocks until the poll ends.
         * With it, the call returns null while the poll still runs. A deferred poll that found no
         * message in its max wait returns an empty batch. A failed poll throws, and so does a call
         * after the batch was returned or the poll was cancelled.
         *
         * @param mixed $timeout_micros
         * @return \Iggy\PolledMessages|null
         */
        public function wait(mixed $timeout_micros = null): ?\Iggy\PolledMessages {}
    }

    /**
     * The limits of one poll of one partition.
     */
    class PollOptions {
        public readonly \Iggy\CommitPolicy $commit;

        public readonly int $count;

        public readonly int $max_bytes;

        public readonly int $max_wait_micros;

        public readonly int $min_count;

        /**
         * Every argument left null keeps its default: up to 1000 messages, no wait, at least 1
         * message, 1 MiB and CommitPolicy::manual().
         *
         * `max_wait_micros` is at most 600 seconds, and the server waits that long for
         * `min_count` messages, which must be 1 to `count`. A reader or a consumer that waits 0
         * polls an idle topic in a busy loop, so give it a wait. A reply holds at most
         * `max_bytes`, and a message larger than that comes alone. Invalid limits are refused
         * here, before anything is sent.
         *
         * @param mixed $count
         * @param mixed $max_wait_micros
         * @param mixed $min_count
         * @param mixed $max_bytes
         * @param \Iggy\CommitPolicy|null $commit
         */
        public function __construct(mixed $count = null, mixed $max_wait_micros = null, mixed $min_count = null, mixed $max_bytes = null, ?\Iggy\CommitPolicy $commit = null) {}
    }

    /**
     * A PHP class representing one batch of one partition.
     */
    class PolledMessages {
        public readonly int $count;

        /**
         * The offset of the newest message in the partition when the batch was read.
         *
         * @var int
         */
        public readonly int $current_offset;

        /**
         * The messages of the batch, in offset order.
         *
         * Each read of this property copies the messages, so keep the result in a variable to
         * read them more than once.
         *
         * @var array
         */
        public readonly array $messages;

        public readonly int $partition_id;

        public function __construct() {}

        /**
         * The position after the last message, to commit the whole batch. Null for an empty
         * batch.
         *
         * @return \Iggy\Checkpoint|null
         */
        public function checkpoint(): ?\Iggy\Checkpoint {}

        /**
         * The position after the message at `index`, to commit a processed part of the batch.
         * Null for an index out of range.
         *
         * @param int $index
         * @return \Iggy\Checkpoint|null
         */
        public function checkpointAt(int $index): ?\Iggy\Checkpoint {}

        /**
         * The position after the batch, for the next read of its partition.
         *
         * After the last message it is the next offset. An empty batch keeps the start of its
         * request, except that first() becomes offset 0, so a start can take more than one read.
         * The continuation belongs to the partition of the batch, and a read of another partition
         * refuses it with InvalidCommand (3). It keeps the partition history of the batch, so a
         * read after the partition was recreated fails with HistoryUnavailable (87).
         *
         * @return \Iggy\PollingStrategy
         */
        public function continuation(): \Iggy\PollingStrategy {}
    }

    class PollingStrategy {
        public function __construct() {}

        /**
         * @return \Iggy\PollingStrategy
         */
        public static function first(): \Iggy\PollingStrategy {}

        /**
         * @return \Iggy\PollingStrategy
         */
        public static function last(): \Iggy\PollingStrategy {}

        /**
         * @return \Iggy\PollingStrategy
         */
        public static function next(): \Iggy\PollingStrategy {}

        /**
         * @param int $value
         * @return \Iggy\PollingStrategy
         */
        public static function offset(int $value): \Iggy\PollingStrategy {}

        /**
         * Poll messages at or after a UNIX timestamp expressed in microseconds.
         *
         * @param int $value
         * @return \Iggy\PollingStrategy
         */
        public static function timestamp(int $value): \Iggy\PollingStrategy {}

        /**
         * Poll messages at or after a UNIX timestamp expressed in microseconds.
         *
         * @param int $value
         * @return \Iggy\PollingStrategy
         */
        public static function timestampMicros(int $value): \Iggy\PollingStrategy {}

        /**
         * Poll messages at or after a UNIX timestamp expressed in seconds.
         *
         * @param int $value
         * @return \Iggy\PollingStrategy
         */
        public static function timestampSeconds(int $value): \Iggy\PollingStrategy {}
    }

    /**
     * A PHP class representing a received message.
     *
     * This class wraps a Rust message, allowing PHP code to access its payload and metadata.
     */
    class ReceiveMessage {
        public function __construct() {}

        /**
         * Retrieves the position after this message, which commits it and every earlier message
         * of its partition.
         *
         * It keeps the partition incarnation and owner that delivered the message, so a commit
         * after the partition was recreated or changed owner is refused instead of landing on
         * the newer partition. Only the reader or the consumer that delivered the message accepts
         * it in commit().
         *
         * @return \Iggy\Checkpoint
         */
        public function checkpoint(): \Iggy\Checkpoint {}

        /**
         * Retrieves the checksum of the received message.
         *
         * The checksum represents the integrity of the message within its topic.
         *
         * @return string
         */
        public function checksum(): string {}

        /**
         * Retrieves the id of the received message.
         *
         * The id represents unique identifier of the message within its topic.
         *
         * @return string
         */
        public function id(): string {}

        /**
         * Retrieves the length of the received message.
         *
         * The length represents the length of the payload.
         *
         * @return int
         */
        public function length(): int {}

        /**
         * Retrieves the offset of the received message.
         *
         * The offset represents the position of the message within its topic.
         *
         * @return int
         */
        public function offset(): int {}

        /**
         * Retrieves the partition this message belongs to.
         *
         * @return int
         */
        public function partitionId(): int {}

        /**
         * Retrieves the payload of the received message.
         *
         * The payload is returned as a PHP string, which can represent both text and binary data.
         * The bytes are copied into a PHP string on each getter call; cache the result in PHP if
         * the payload will be read repeatedly.
         *
         * @return string
         */
        public function payload(): string {}

        /**
         * Retrieves the timestamp of the received message.
         *
         * The timestamp represents the time of the message within its topic.
         *
         * @return int
         */
        public function timestamp(): int {}
    }

    /**
     * A PHP class representing a message to be sent.
     */
    class SendMessage {
        public readonly string $id;

        /**
         * The bytes are copied into a PHP string on each getter call; cache the result in PHP if
         * the payload will be read repeatedly.
         *
         * @var string
         */
        public readonly string $payload;

        /**
         * Constructs a new `SendMessage` instance from a PHP string.
         *
         * PHP strings are byte strings, so this accepts both text and binary payloads.
         *
         * @param string $data
         */
        public function __construct(string $data) {}
    }

    /**
     * A PHP class representing where one partition's batch was committed.
     */
    class SendMessagesConfirmation {
        /**
         * The offset assigned to the first message of the batch in this partition.
         *
         * Delivery is at-least-once, so an earlier retry may already have committed the
         * same batch at a lower offset. The value never implies uniqueness.
         *
         * Confirmation follows VSR quorum commit. Persisted message durability also
         * requires recoverable stable-storage copies on the quorum.
         *
         * @var int
         */
        public readonly int $base_offset;

        public readonly int $partition_id;

        public readonly int $stream_id;

        public readonly int $topic_id;

        public function __construct() {}
    }

    /**
     * A PHP class representing the commit confirmations of a send.
     */
    class SendMessagesResponse {
        /**
         * One confirmation per partition the batch landed in.
         *
         * The list is empty when the server reports no offsets. A server can commit a
         * batch it has no offsets to describe, so check for an empty array instead of
         * indexing.
         *
         * The confirmations are rebuilt on each getter call; cache the result in PHP if
         * they will be read repeatedly.
         *
         * @var array
         */
        public readonly array $confirmations;

        public function __construct() {}
    }

    class StreamDetails {
        /**
         * Options the server filled in, including the read-only encryption projections.
         *
         * @var array
         */
        public readonly array $derived_options;

        /**
         * Fingerprints of the server keys that wrap the stream's key, comma-separated: one
         * outside a server key rotation. Null for a plaintext stream.
         *
         * @var string|null
         */
        public readonly ?string $encryption_kek_fingerprint;

        /**
         * Fingerprint of the stream's key, 16 lowercase hex characters. Compare it with
         * `Client::encryptionKeyFingerprint()` of a key you hold. Null for a plaintext stream.
         *
         * @var string|null
         */
        public readonly ?string $encryption_key_fingerprint;

        /**
         * Who chose the stream's at-rest key: `stream`, `server`, or `none` for a plaintext
         * stream. A source a newer server reports is returned as its text.
         *
         * @var string|null
         */
        public readonly ?string $encryption_key_source;

        public readonly int $id;

        public readonly int $messages_count;

        public readonly string $name;

        /**
         * Options the creating client set, as option name to scalar value. Key material is
         * never returned.
         *
         * @var array
         */
        public readonly array $options;

        public readonly int $topics_count;

        public function __construct() {}
    }

    /**
     * Stream option keys, as the cases an update's reset list takes.
     *
     * No stream option is resettable in this release, so `Client::updateStream()` refuses every
     * case with the error the server would return.
     */
    enum StreamOptionKey: string {
      case Name = 'name';
      case EncryptionKey = 'encryption_key';
      case EncryptionKeySource = 'encryption_key_source';
      case EncryptionKeyFingerprint = 'encryption_key_fingerprint';
      case EncryptionKekFingerprint = 'encryption_kek_fingerprint';
    }

    class TopicDetails {
        /**
         * Options the server filled in from its catalog defaults.
         *
         * @var array
         */
        public readonly array $derived_options;

        public readonly int $id;

        public readonly int $messages_count;

        public readonly string $name;

        /**
         * Options the creating or updating client set, as option name to scalar value.
         * Integers above `PHP_INT_MAX`, such as never-expire and unlimited
         * (18446744073709551615), are decimal strings.
         *
         * @var array
         */
        public readonly array $options;

        public readonly int $partitions_count;

        public function __construct() {}
    }

    /**
     * Topic option keys, as the cases an update's reset list takes.
     *
     * Only `MessageExpiry` and `MaxTopicSize` are resettable in this release. The server refuses
     * every other case with its own category.
     */
    enum TopicOptionKey: string {
      case Name = 'name';
      case CompressionAlgorithm = 'compression_algorithm';
      case MessageExpiry = 'message_expiry';
      case MaxTopicSize = 'max_topic_size';
      case SegmentSize = 'segment_size';
      case Durability = 'durability';
      case ConsumerOffsetDurability = 'consumer_offset_durability';
      case PartitionResizePolicy = 'partition_resize_policy';
      case MessagesRequiredToSave = 'messages_required_to_save';
      case SizeOfMessagesRequiredToSave = 'size_of_messages_required_to_save';
      case PreallocateSegments = 'preallocate_segments';
    }
}

namespace Iggy\Exception {
    class AuthenticationException extends \Iggy\Exception\IggyException {
        public function __construct() {}
    }

    class ConnectionException extends \Iggy\Exception\IggyException {
        public function __construct() {}
    }

    class IggyException extends \Exception {
        public function __construct() {}
    }

    class NotFoundException extends \Iggy\Exception\IggyException {
        public function __construct() {}
    }

    class PendingOperationException extends \Iggy\Exception\IggyException {
        public readonly string $kind;

        /**
         * Exact unsigned 64-bit operation ID, represented as a decimal string.
         *
         * @var string
         */
        public readonly string $operation_id;

        public readonly ?int $partition_id;

        public function __construct() {}
    }

    /**
     * A failed read of a PartitionReader or a Consumer. The reader stays usable and retries by
     * itself.
     *
     * The code is the code of the error, and the previous exception is the error itself, typed as
     * any other call would throw it, for example a PendingOperationException.
     */
    class ReadException extends \Iggy\Exception\IggyException {
        /**
         * The partition of the error, or null for an error of the whole reader.
         *
         * @var int|null
         */
        public readonly ?int $partition_id;

        /**
         * For a message that does not decode: the continuation after it. The reader reads the
         * message again until a seek of its partition to this continuation skips it.
         *
         * @var \Iggy\PollingStrategy|null
         */
        public readonly ?\Iggy\PollingStrategy $skip;

        public function __construct() {}
    }

    class TransientException extends \Iggy\Exception\IggyException {
        public function __construct() {}
    }
}
