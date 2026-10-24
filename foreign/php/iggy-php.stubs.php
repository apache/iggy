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
         * Creates and initializes a consumer group consumer.
         *
         * `$partition_id` is ignored for a consumer group: the member reads the partitions
         * the server assigns to it.
         *
         * @param string $name
         * @param string $stream
         * @param string $topic
         * @param mixed $partition_id
         * @param \Iggy\PollingStrategy|null $polling_strategy
         * @param mixed $batch_length
         * @param \Iggy\AutoCommit|null $auto_commit
         * @param bool $create_consumer_group_if_not_exists
         * @param bool $auto_join_consumer_group
         * @param mixed $poll_interval_micros
         * @param mixed $polling_retry_interval_micros
         * @param mixed $init_retries
         * @param mixed $init_retry_interval_micros
         * @param bool $allow_replay
         * @return \Iggy\Consumer
         */
        public function consumerGroup(string $name, string $stream, string $topic, mixed $partition_id = null, ?\Iggy\PollingStrategy $polling_strategy = null, mixed $batch_length = null, ?\Iggy\AutoCommit $auto_commit = null, bool $create_consumer_group_if_not_exists = true, bool $auto_join_consumer_group = true, mixed $poll_interval_micros = null, mixed $polling_retry_interval_micros = null, mixed $init_retries = null, mixed $init_retry_interval_micros = null, bool $allow_replay = false): \Iggy\Consumer {}

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
         * Replays an exact retained operation. A committed rejection raises its original error
         * and releases its lane; uncertainty keeps it retained. HTTP is unsupported.
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

        public function __construct() {}
    }

    class TransientException extends \Iggy\Exception\IggyException {
        public function __construct() {}
    }
}
