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
    class AutoCommit {
        public function __construct() {}

        /**
         * @return \Iggy\AutoCommit
         */
        public static function disabled(): \Iggy\AutoCommit {}

        /**
         * @param int $interval_micros
         * @return \Iggy\AutoCommit
         */
        public static function interval(int $interval_micros): \Iggy\AutoCommit {}

        /**
         * @param int $interval_micros
         * @param \Iggy\AutoCommitWhen $when
         * @return \Iggy\AutoCommit
         */
        public static function intervalOrWhen(int $interval_micros, \Iggy\AutoCommitWhen $when): \Iggy\AutoCommit {}

        /**
         * @param \Iggy\AutoCommitWhen $when
         * @return \Iggy\AutoCommit
         */
        public static function when(\Iggy\AutoCommitWhen $when): \Iggy\AutoCommit {}
    }

    class AutoCommitWhen {
        public function __construct() {}

        /**
         * @return \Iggy\AutoCommitWhen
         */
        public static function consumingAllMessages(): \Iggy\AutoCommitWhen {}

        /**
         * @return \Iggy\AutoCommitWhen
         */
        public static function consumingEachMessage(): \Iggy\AutoCommitWhen {}

        /**
         * @param int $n
         * @return \Iggy\AutoCommitWhen
         */
        public static function consumingEveryNthMessage(int $n): \Iggy\AutoCommitWhen {}

        /**
         * @return \Iggy\AutoCommitWhen
         */
        public static function pollingMessages(): \Iggy\AutoCommitWhen {}
    }

    /**
     * A PHP class representing the Iggy client.
     */
    class Client {
        /**
         * Constructs a new IggyClient from a TCP server address.
         *
         * @param string|null $conn
         */
        public function __construct(?string $conn = null) {}

        /**
         * Connects the IggyClient to its service.
         *
         * @return void
         */
        public function connect(): void {}

        /**
         * Creates and initializes a consumer group consumer.
         *
         * `$partition_id` is ignored for a consumer group: the member reads the partitions
         * the server assigns to it.
         *
         * @param string $name
         * @param string $stream
         * @param string $topic
         * @param int|null $partition_id
         * @param \Iggy\PollingStrategy|null $polling_strategy
         * @param int|null $batch_length
         * @param \Iggy\AutoCommit|null $auto_commit
         * @param bool $create_consumer_group_if_not_exists
         * @param bool $auto_join_consumer_group
         * @param int|null $poll_interval_micros
         * @param int|null $polling_retry_interval_micros
         * @param int|null $init_retries
         * @param int|null $init_retry_interval_micros
         * @param bool $allow_replay
         * @return \Iggy\Consumer
         */
        public function consumerGroup(string $name, string $stream, string $topic, ?int $partition_id = null, ?\Iggy\PollingStrategy $polling_strategy = null, ?int $batch_length = null, ?\Iggy\AutoCommit $auto_commit = null, bool $create_consumer_group_if_not_exists = true, bool $auto_join_consumer_group = true, ?int $poll_interval_micros = null, ?int $polling_retry_interval_micros = null, ?int $init_retries = null, ?int $init_retry_interval_micros = null, bool $allow_replay = false): \Iggy\Consumer {}

        /**
         * Creates a new stream.
         *
         * @param string $name
         * @return void
         */
        public function createStream(string $name): void {}

        /**
         * Creates a topic.
         *
         * Every option left null resolves against the server default at admission.
         *
         * @param mixed $stream
         * @param string $name
         * @param int $partitions_count
         * @param string|null $compression_algorithm
         * @param int|null $message_expiry_micros
         * @param int|null $max_topic_size
         * @param int|null $segment_size
         * @param \Iggy\Durability|null $durability
         * @param \Iggy\Durability|null $consumer_offset_durability
         * @param int|null $messages_required_to_save
         * @param int|null $size_of_messages_required_to_save
         * @param bool|null $preallocate_segments
         * @return void
         */
        public function createTopic(mixed $stream, string $name, int $partitions_count, ?string $compression_algorithm = null, ?int $message_expiry_micros = null, ?int $max_topic_size = null, ?int $segment_size = null, ?\Iggy\Durability $durability = null, ?\Iggy\Durability $consumer_offset_durability = null, ?int $messages_required_to_save = null, ?int $size_of_messages_required_to_save = null, ?bool $preallocate_segments = null): void {}

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
         * Constructs a new IggyClient from a connection string.
         *
         * @param string $connection_string
         * @return \Iggy\Client
         */
        public static function fromConnectionString(string $connection_string): \Iggy\Client {}

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
         * Logs in the user with the given credentials.
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
         * Polls messages from the specified topic and partition.
         *
         * Each message keeps the partition context of this poll. To continue at an offset under
         * that context, pass PollingStrategy::offset($offset)->withContext($message->context()).
         * A stale context fails with HistoryUnavailable (87).
         *
         * Without a context, the poll uses the context of its route. It can fail once with
         * HistoryUnavailable (87) after another client deleted and recreated the partition. The
         * failed route is dropped, and the next call routes again.
         *
         * @param mixed $stream
         * @param mixed $topic
         * @param int $partition_id
         * @param \Iggy\PollingStrategy $polling_strategy
         * @param int $count
         * @param bool $auto_commit
         * @return array
         */
        public function pollMessages(mixed $stream, mixed $topic, int $partition_id, \Iggy\PollingStrategy $polling_strategy, int $count, bool $auto_commit): array {}

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
    }

    /**
     * A PHP class representing the Iggy consumer.
     */
    class Consumer {
        public function __construct() {}

        /**
         * Consumes messages with a PHP callback.
         *
         * The callback is called as callback(ReceiveMessage $message). A finite limit is required.
         *
         * With AutoCommit::when(), offsets may already be queued for commit before the
         * PHP callback runs. Use AutoCommit::disabled() and call
         * storePosition($message->position()) after a successful callback when at-least-once
         * callback processing is required.
         *
         * After another client deleted and recreated a partition, or after its consumer group
         * owner changed, a poll can fail once with HistoryUnavailable (87) or
         * ConsumerGroupPartitionNotOwned (5009). The failed route is dropped, and the next call
         * routes again.
         *
         * @param callable $callback
         * @param int $limit
         * @return int
         */
        public function consumeMessages(callable $callback, int $limit): int {}

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
         * After another client deleted and recreated a partition, or after its consumer group
         * owner changed, a poll can fail once with HistoryUnavailable (87) or
         * ConsumerGroupPartitionNotOwned (5009). The failed route is dropped, and the next call
         * routes again.
         *
         * @return \Iggy\MessageIterator
         */
        public function iterMessages(): \Iggy\MessageIterator {}

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
         * Stores the provided offset for the provided partition id.
         *
         * If partition_id is null, at least one message must have been polled first.
         *
         * The offset is committed under the partition context of the latest message consumed
         * from that partition. After the partition was recreated or changed owner, that context
         * can be newer than the message's, and the offset then lands on the newer partition. Use
         * storePosition($message->position()) to commit under the context that delivered the
         * message.
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
         * Stores a message position under the partition incarnation and owner that delivered it.
         *
         * Pass $message->position() to commit a message after later polls or a rebalance. A
         * position from a deleted or recreated partition fails with HistoryUnavailable (87), and
         * one from a previous owner fails with ConsumerGroupPartitionNotOwned (5009). The stored
         * offset is then unchanged.
         *
         * @param \Iggy\ConsumerPosition $position
         * @return void
         */
        public function storePosition(\Iggy\ConsumerPosition $position): void {}

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
     * A PHP class representing a consumed position to commit with Consumer::storePosition().
     */
    class ConsumerPosition {
        /**
         * The partition incarnation and owner captured by the poll that delivered the message.
         *
         * @var \Iggy\PartitionContext
         */
        public readonly \Iggy\PartitionContext $context;

        /**
         * The inclusive message offset to commit.
         *
         * @var int
         */
        public readonly int $offset;

        public readonly int $partition_id;

        /**
         * Rebuilds a position from values saved with a processed message.
         *
         * @param int $partition_id
         * @param int $offset
         * @param \Iggy\PartitionContext $context
         */
        public function __construct(int $partition_id, int $offset, \Iggy\PartitionContext $context) {}
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
         * @return void
         */
        public function rewind(): void {}

        /**
         * @return bool
         */
        public function valid(): bool {}
    }

    /**
     * A PHP class representing the partition incarnation and owner that a poll captured.
     *
     * Keep it with an offset that is continued or committed later. The server then refuses the
     * request after the partition was deleted and recreated, or after its consumer group owner
     * changed.
     */
    class PartitionContext {
        /**
         * The partition's creation revision. It changes when the partition is deleted and
         * recreated.
         *
         * @var int
         */
        public readonly int $incarnation;

        /**
         * The metadata operation the server must have applied before it serves the request.
         *
         * @var int
         */
        public readonly int $metadata_op;

        /**
         * The consumer group owner generation, or 0 for a poll without a consumer group.
         *
         * @var int
         */
        public readonly int $owner_generation;

        /**
         * Rebuilds a context from values saved with an offset, for example by another process.
         *
         * @param int $incarnation
         * @param int $owner_generation
         * @param int $metadata_op
         */
        public function __construct(int $incarnation, int $owner_generation, int $metadata_op) {}
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

        /**
         * Continues at an offset under the partition incarnation and owner that produced it.
         *
         * A stale context fails the poll instead of reading that offset from a recreated
         * partition. Without a context, the poll uses the context of its route.
         *
         * @param \Iggy\PartitionContext $context
         * @return \Iggy\PollingStrategy
         */
        public function withContext(\Iggy\PartitionContext $context): \Iggy\PollingStrategy {}
    }

    /**
     * A PHP class representing a received message.
     *
     * This class wraps a Rust message, allowing PHP code to access its payload and metadata.
     */
    class ReceiveMessage {
        public function __construct() {}

        /**
         * Retrieves the checksum of the received message.
         *
         * The checksum represents the integrity of the message within its topic.
         *
         * @return string
         */
        public function checksum(): string {}

        /**
         * Retrieves the partition incarnation and owner captured by the poll that delivered
         * this message.
         *
         * @return \Iggy\PartitionContext
         */
        public function context(): \Iggy\PartitionContext {}

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
         * Retrieves the position to commit for this message with Consumer::storePosition().
         *
         * It keeps the partition incarnation and owner that delivered the message, so a commit
         * after the partition was recreated or changed owner is refused instead of landing on
         * the newer partition.
         *
         * @return \Iggy\ConsumerPosition
         */
        public function position(): \Iggy\ConsumerPosition {}

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
        public readonly int $id;

        public readonly int $messages_count;

        public readonly string $name;

        public readonly int $topics_count;

        public function __construct() {}
    }

    class TopicDetails {
        public readonly int $id;

        public readonly int $messages_count;

        public readonly string $name;

        public readonly int $partitions_count;

        public function __construct() {}
    }
}

namespace Iggy\Exception {
    class AuthenticationException extends Iggy\Exception\IggyException {
        public function __construct() {}
    }

    class ConnectionException extends Iggy\Exception\IggyException {
        public function __construct() {}
    }

    class IggyException extends \Exception {
        public function __construct() {}
    }

    class NotFoundException extends Iggy\Exception\IggyException {
        public function __construct() {}
    }

    class TransientException extends Iggy\Exception\IggyException {
        public function __construct() {}
    }
}
