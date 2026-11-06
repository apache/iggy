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

declare(strict_types=1);

namespace Iggy\Conformance;

use Iggy\Client;
use Iggy\Consumer;
use Iggy\OptionsScope;
use Iggy\PartitionReader;
use Iggy\PendingPoll;
use Iggy\PollOptions;

/** The methods of a client: each reads its arguments, calls the SDK and returns the canonical result. */
final class Calls
{
    /** The argument names of each method that the SDK has. */
    private const ARGUMENTS = [
        'connect' => [],
        'disconnect' => [],
        'ping' => [],
        'login_user' => ['username', 'password'],
        'get_stream' => ['stream_id'],
        'create_stream' => ['name', 'encryption_key_hex'],
        'update_stream' => ['stream_id', 'name', 'reset'],
        'delete_stream' => ['stream_id'],
        'get_topic' => ['stream_id', 'topic_id'],
        'create_topic' => ['stream_id', 'name', 'partitions_count', 'message_expiry', 'max_topic_size'],
        'update_topic' => ['stream_id', 'topic_id', 'name', 'message_expiry', 'max_topic_size', 'reset'],
        'delete_topic' => ['stream_id', 'topic_id'],
        'send_messages' => ['stream_id', 'topic_id', 'partitioning', 'messages'],
        'poll_messages' => [
            'stream_id', 'topic_id', 'partition_id', 'consumer', 'strategy', 'count', 'commit', 'max_wait_ms', 'min_count',
        ],
        'build_reader' => ['stream_id', 'topic_id', 'consumer', 'partitions', 'start', 'max_wait_ms', 'commit'],
        'build_consumer' => ['stream_id', 'topic_id', 'group', 'max_wait_ms', 'count', 'commit'],
        'resolve_operation' => ['operation_id', 'deadline_ms'],
        'abandon_operation' => ['operation_id'],
        'send_binary_request' => ['code', 'payload_hex'],
        'send_binary_request_to_replica' => ['code', 'payload_hex', 'replica'],
        'describe_options' => ['scope'],
        'get_metadata_changes' => ['start', 'groups', 'limit'],
    ];

    /** The methods of the contract that the SDK lacks. */
    private const MISSING = [
        'logout_user', 'get_streams', 'get_topics', 'create_consumer_group', 'get_consumer_group',
        'delete_consumer_group', 'create_user', 'get_user', 'update_user', 'delete_user', 'get_connection_info',
    ];

    public function __construct(private readonly Client $client)
    {
    }

    /** The arguments of a method that the SDK has, checked against its argument names. */
    public static function arguments(string $method, array $args): array
    {
        if (in_array($method, self::MISSING, true)) {
            throw new Unsupported("the SDK has no {$method}");
        }

        return fields($args, self::ARGUMENTS[$method] ?? throw new Unsupported("the method {$method}"));
    }

    public function call(string $method, array $args): mixed
    {
        $args = self::arguments($method, $args);

        return match ($method) {
            'connect' => $this->client->connect(),
            'disconnect' => $this->client->disconnect(),
            'ping' => $this->client->ping(),
            // loginUser returns no identity, so the result is null.
            'login_user' => $this->client->loginUser($args['username'], $args['password']),
            'get_stream' => $this->getStream($args),
            'create_stream' => $this->createStream($args),
            'update_stream' => $this->client->updateStream($args['stream_id'], name: $args['name'], raw_reset: $args['reset']),
            'delete_stream' => $this->client->deleteStream($args['stream_id']),
            'get_topic' => $this->getTopic($args),
            'create_topic' => $this->createTopic($args),
            'update_topic' => $this->updateTopic($args),
            'delete_topic' => $this->client->deleteTopic($args['stream_id'], $args['topic_id']),
            'send_messages' => $this->sendMessages($args),
            'poll_messages' => polled_messages($this->client->pollMessages(...self::pollArguments($args))),
            'resolve_operation' => $this->client->resolveOperation(
                (string) $args['operation_id'],
                micros($args['deadline_ms']),
            ),
            'abandon_operation' => $this->client->abandonOperation((string) $args['operation_id']),
            'send_binary_request' => [
                'payload_hex' => bin2hex($this->client->sendBinaryRequest($args['code'], hex_bytes($args['payload_hex'] ?? ''))),
            ],
            'send_binary_request_to_replica' => [
                'payload_hex' => bin2hex($this->client->sendBinaryRequestToReplica(
                    $args['code'],
                    hex_bytes($args['payload_hex'] ?? ''),
                    $args['replica'],
                )),
            ],
            'describe_options' => array_map(
                option_spec(...),
                $this->client->describeOptions(OptionsScope::from($args['scope'])),
            ),
            'get_metadata_changes' => metadata_changes($this->client->getMetadataChanges(
                metadata_start($args['start']),
                metadata_groups($args['groups']),
                limit: $args['limit'],
            )),
            default => throw new Failure("{$method} creates an entity"),
        };
    }

    /** Starts a poll that runs on the SDK's runtime while the agent reads on. */
    public function startPoll(array $args): PendingPoll
    {
        return $this->client->startPollMessages(...self::pollArguments(self::arguments('poll_messages', $args)));
    }

    public function buildReader(array $args): PartitionReader
    {
        $args = self::arguments('build_reader', $args);
        [$consumer, $group] = consumer($args['consumer']);
        $start = $args['start'] === null ? null : strategy($args['start']);
        $options = new PollOptions(max_wait_micros: micros($args['max_wait_ms']), commit: commit_policy($args['commit']));
        if ($group) {
            return $args['partitions'] === null
                ? $this->client->groupReader($args['stream_id'], $args['topic_id'], $consumer, $start, $options)
                : throw new Unsupported('the SDK reads the assignment of a group reader, not chosen partitions');
        }

        return $args['partitions'] !== null
            ? $this->client->reader($args['stream_id'], $args['topic_id'], $consumer, $args['partitions'], $start, $options)
            : throw new Unsupported('the SDK reads the partitions that a single consumer names, not every partition');
    }

    public function buildConsumer(array $args): Consumer
    {
        $args = self::arguments('build_consumer', $args);
        $options = new PollOptions(
            count: $args['count'],
            max_wait_micros: micros($args['max_wait_ms']),
            commit: commit_policy($args['commit']),
        );

        return $this->client->consumerGroup(
            $args['group'],
            (string) $args['stream_id'],
            (string) $args['topic_id'],
            poll_options: $options,
        );
    }

    /** The arguments of pollMessages() and startPollMessages(), in their order. */
    private static function pollArguments(array $args): array
    {
        [$consumer, $group] = consumer($args['consumer']);
        $options = new PollOptions(
            count: $args['count'],
            max_wait_micros: micros($args['max_wait_ms']),
            min_count: $args['min_count'],
            commit: commit_policy($args['commit']),
        );

        return [
            $args['stream_id'],
            $args['topic_id'],
            $args['partition_id'],
            $consumer,
            strategy($args['strategy']),
            $options,
            $group,
        ];
    }

    private function getStream(array $args): ?array
    {
        $stream = $this->client->getStream($args['stream_id']);

        return $stream === null ? null : stream_details($stream);
    }

    /** createStream returns no stream, so the result is null. */
    private function createStream(array $args): null
    {
        $key = $args['encryption_key_hex'] === null ? null : base64_encode(hex_bytes($args['encryption_key_hex']));
        $this->client->createStream($args['name'], $key);

        return null;
    }

    private function getTopic(array $args): ?array
    {
        $topic = $this->client->getTopic($args['stream_id'], $args['topic_id']);

        return $topic === null ? null : topic_details($topic);
    }

    /** createTopic returns no topic, so the result is null. */
    private function createTopic(array $args): null
    {
        $this->client->createTopic(
            $args['stream_id'],
            $args['name'],
            $args['partitions_count'] ?? throw new Unsupported('the SDK creates a topic only with a partitions count'),
            message_expiry_micros: topic_limit($args['message_expiry'], 'never'),
            max_topic_size: topic_limit($args['max_topic_size'], 'unlimited'),
        );

        return null;
    }

    private function updateTopic(array $args): null
    {
        $this->client->updateTopic(
            $args['stream_id'],
            $args['topic_id'],
            name: $args['name'],
            message_expiry_micros: updated_topic_limit($args['message_expiry'], 'never', 'message_expiry'),
            max_topic_size: updated_topic_limit($args['max_topic_size'], 'unlimited', 'max_topic_size'),
            raw_reset: $args['reset'],
        );

        return null;
    }

    private function sendMessages(array $args): null
    {
        $partition = partition_id($args['partitioning']);
        $messages = array_map(send_message(...), $args['messages']);
        $this->client->sendMessages($args['stream_id'], $args['topic_id'], $partition, $messages);

        return null;
    }
}
