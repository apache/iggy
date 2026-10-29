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

use Iggy\Client as IggyClient;
use Iggy\ClientCommitPolicy;
use Iggy\CommitEvent;
use Iggy\Exception\NotFoundException;
use Iggy\PolledMessages;
use Iggy\PollingStrategy;
use Iggy\PollOptions;
use Iggy\ReceiveMessage;
use Iggy\SendMessage;
use Iggy\SendMessagesResponse;
use Iggy\ServerCommitPolicy;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class IggySdkTest extends TestCase
{
    private const CREATE_USER_CODE = 33;
    private const DELETE_USER_CODE = 34;
    private const GET_ME_CODE = 20;
    private const ACTIVE_USER_STATUS = 1;
    private const STRING_IDENTIFIER_KIND = 2;
    private const CONSUMER_ID = 1;
    private const INVALID_COMMAND_CODE = 3;
    private const HISTORY_UNAVAILABLE_CODE = 87;
    private const EMPTY_READ_TIMEOUT_MICROS = 100_000;
    private const DEADLINE_SECONDS = 10;
    private const UNCOUNTED_WAIT_SECONDS = 0.5;
    private const CONSUMER_BUSY = 'cannot run while consumeMessages() runs';

    #[TestDox('A connected client can ping the server')]
    public function testPing(): void
    {
        new_client()->ping();
        assert_true(true, 'ping completed without throwing');
    }

    #[TestDox('The TCP constructor returns an IggyClient instance')]
    public function testClientNotNull(): void
    {
        assert_true(new_client() instanceof IggyClient);
    }

    #[TestDox('The TCP constructor treats an empty address as the default server address')]
    public function testEmptyClientAddressUsesDefault(): void
    {
        if (!in_array(server_host(), ['127.0.0.1', 'localhost'], true)) {
            $this->markTestSkipped('empty address defaults to localhost, but this run uses a remote test server');
        }

        $client = new IggyClient('');
        $client->connect();
        $client->loginUser(env_or_default('IGGY_ROOT_USERNAME', 'iggy'), env_or_default('IGGY_ROOT_PASSWORD', 'iggy'));
        $client->ping();

        assert_true($client instanceof IggyClient);
    }

    #[TestDox('A client created from a connection string can connect and ping')]
    public function testClientFromConnectionString(): void
    {
        $client = new_connection_string_client();
        $client->ping();

        assert_true($client instanceof IggyClient);
    }

    #[TestDox('Examples authenticate using literal environment credentials')]
    public function testExamplesAuthenticateWithLiteralEnvironmentCredentials(): void
    {
        require_once __DIR__ . '/../../../examples/php/src/common.php';

        $admin = new_client();
        $username = unique_name('php-example-user');
        $password = 'example % @:+= password';
        $request = chr(strlen($username)) . $username
            . chr(strlen($password)) . $password
            . chr(self::ACTIVE_USER_STATUS) . chr(0);
        $created = $admin->sendBinaryRequest(self::CREATE_USER_CODE, $request);
        $userId = unpack('VuserId', $created)['userId'];
        $keys = ['IGGY_CONNECTION_STRING', 'IGGY_HOST', 'IGGY_PORT', 'IGGY_USERNAME', 'IGGY_PASSWORD'];
        $previous = array_combine($keys, array_map('getenv', $keys));

        try {
            putenv('IGGY_CONNECTION_STRING');
            putenv('IGGY_HOST=' . server_host());
            putenv('IGGY_PORT=' . server_port());
            putenv('IGGY_USERNAME=' . $username);
            putenv('IGGY_PASSWORD=' . $password);

            $client = iggy_client();
            $identity = $client->sendBinaryRequest(self::GET_ME_CODE, '');
            assert_same($userId, unpack('VclientId/VuserId', $identity)['userId']);

            putenv('IGGY_PASSWORD=incorrect-password');
            assert_instance_of(
                \Iggy\Exception\AuthenticationException::class,
                assert_throws(static fn () => iggy_client()),
            );
        } finally {
            foreach ($previous as $key => $value) {
                putenv($value === false ? $key : $key . '=' . $value);
            }
            $admin->sendBinaryRequest(
                self::DELETE_USER_CODE,
                chr(self::STRING_IDENTIFIER_KIND) . chr(strlen($username)) . $username,
            );
        }
    }

    #[TestDox('Connection strings construct clients for every transport before connecting')]
    public function testConnectionStringsConstructEveryTransport(): void
    {
        foreach ([
            'iggy+tcp://iggy:iggy@127.0.0.1:8090',
            'iggy+quic://iggy:iggy@127.0.0.1:8080',
            'iggy+http://iggy:iggy@127.0.0.1:3000',
            'iggy+ws://iggy:iggy@127.0.0.1:8092',
        ] as $connectionString) {
            assert_instance_of(IggyClient::class, IggyClient::fromConnectionString($connectionString));
        }
    }

    #[TestDox('A stream can be created and fetched by name')]
    public function testCreateAndGetStream(): void
    {
        $client = new_client();
        $streamName = unique_name('test-stream');

        try {
            $client->createStream($streamName);
            $stream = $client->getStream($streamName);

            assert_not_null($stream);
            assert_same($streamName, $stream->name);
            assert_true($stream->id >= 0, 'expected non-negative stream id');
        } finally {
            cleanup_stream($client, $streamName);
        }
    }

    #[TestDox('A newly created stream reports no topics')]
    public function testNewStreamHasNoTopics(): void
    {
        $client = new_client();
        $streamName = unique_name('test-stream');

        try {
            $client->createStream($streamName);
            $stream = $client->getStream($streamName);

            assert_not_null($stream);
            assert_same($streamName, $stream->name);
            assert_true($stream->id >= 0, 'expected non-negative stream id');
            assert_same(0, $stream->topics_count);
        } finally {
            cleanup_stream($client, $streamName);
        }
    }

    #[TestDox('A topic can be created and fetched by name')]
    public function testCreateAndGetTopic(): void
    {
        $client = new_client();
        $streamName = unique_name('test-stream');
        $topicName = unique_name('test-topic');

        try {
            $client->createStream($streamName);
            $client->createTopic($streamName, $topicName, 2, null, null, null, null);
            $topic = $client->getTopic($streamName, $topicName);

            assert_not_null($topic);
            assert_same($topicName, $topic->name);
            assert_true($topic->id >= 0, 'expected non-negative topic id');
            assert_same(2, $topic->partitions_count);
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('Streams and topics can be fetched with numeric PHP identifiers')]
    public function testNumericIdentifiers(): void
    {
        $client = new_client();
        $streamName = unique_name('numeric-stream');
        $topicName = unique_name('numeric-topic');

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $streamByName = $client->getStream($streamName);
            assert_not_null($streamByName);

            $streamById = $client->getStream($streamByName->id);
            assert_not_null($streamById);
            assert_same($streamName, $streamById->name);

            $topicByName = $client->getTopic($streamByName->id, $topicName);
            assert_not_null($topicByName);

            $topicById = $client->getTopic($streamByName->id, $topicByName->id);
            assert_not_null($topicById);
            assert_same($topicName, $topicById->name);
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('Digit-only PHP string identifiers are fetched by name')]
    public function testDigitOnlyStringIdentifiersAreNamed(): void
    {
        $client = new_client();
        $streamName = (string) random_int(2_000_000_000, 4_294_967_295);
        $topicName = (string) random_int(2_000_000_000, 4_294_967_295);

        try {
            create_stream_and_topic($client, $streamName, $topicName);

            $stream = $client->getStream($streamName);
            assert_not_null($stream);
            assert_same($streamName, $stream->name);

            $topic = $client->getTopic($streamName, $topicName);
            assert_not_null($topic);
            assert_same($topicName, $topic->name);
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A topic created through the helper can be fetched')]
    public function testListTopicsViaGetTopic(): void
    {
        $client = new_client();
        $streamName = unique_name('test-stream');
        $topicName = unique_name('test-topic');

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $topic = $client->getTopic($streamName, $topicName);

            assert_not_null($topic);
            assert_same($topicName, $topic->name);
            assert_true($topic->id >= 0, 'expected non-negative topic id');
            assert_same(1, $topic->partitions_count);
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('Binary payloads round-trip through sendMessages and pollMessages')]
    public function testSendAndPollBinaryMessages(): void
    {
        $client = new_client();
        $streamName = unique_name('msg-stream');
        $topicName = unique_name('msg-topic');
        $partitionId = 0;
        $messages = array_map(
            static fn (int $i): string => random_bytes(16) . pack('C*', 0, $i, 255),
            range(1, 3),
        );

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $polled = $client->pollMessages(
                $streamName,
                $topicName,
                $partitionId,
                self::CONSUMER_ID,
                PollingStrategy::first(),
                new PollOptions(count: 10, server_commit: ServerCommitPolicy::OnRead),
            )->messages;
            assert_count(count($messages), $polled);
            assert_same($messages, collect_payloads($polled));
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('sendMessages reports one commit confirmation per written partition')]
    public function testSendMessagesReportsCommitConfirmation(): void
    {
        $client = new_client();
        $streamName = unique_name('confirm-stream');
        $topicName = unique_name('confirm-topic');
        $partitionId = 0;

        try {
            create_stream_and_topic($client, $streamName, $topicName);

            $response = $client->sendMessages($streamName, $topicName, $partitionId, [new SendMessage('confirm-first')]);
            assert_instance_of(SendMessagesResponse::class, $response);
            assert_count(1, $response->confirmations, 'a single-partition send commits in exactly one partition');
            assert_same($partitionId, $response->confirmations[0]->partition_id);

            // Offsets are per partition and start at 0, so the second send of
            // the same size must be confirmed one message later.
            $second = $client->sendMessages($streamName, $topicName, $partitionId, [new SendMessage('confirm-second')]);
            assert_same(
                $response->confirmations[0]->base_offset + 1,
                $second->confirmations[0]->base_offset,
                'the confirmed offset must advance by the committed message count'
            );
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('Received messages expose payload and metadata')]
    public function testMessageProperties(): void
    {
        $client = new_client();
        $streamName = unique_name('msg-stream');
        $topicName = unique_name('msg-topic');
        $partitionId = 0;
        $payload = unique_name('Property test');

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages($streamName, $topicName, $partitionId, [new SendMessage($payload)]);

            $polled = $client->pollMessages(
                $streamName,
                $topicName,
                $partitionId,
                self::CONSUMER_ID,
                PollingStrategy::last(),
                new PollOptions(count: 1, server_commit: ServerCommitPolicy::OnRead),
            )->messages;
            assert_count(1, $polled);

            $message = $polled[0];
            assert_same($payload, $message->payload());
            assert_true($message->offset() >= 0, 'expected non-negative offset');
            assert_true($message->id() !== '', 'expected message id');
            assert_true($message->timestamp() > 0, 'expected positive timestamp');
            assert_true(ctype_digit($message->checksum()), 'expected numeric checksum');
            assert_true($message->length() > 0, 'expected positive length');
            assert_same($partitionId, $message->partitionId());
            assert_same($partitionId, $message->checkpoint()->partition_id);
            assert_same($message->offset(), $message->checkpoint()->offset);
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('Polling strategies return the expected payloads and next advances stored offsets')]
    public function testPollingStrategies(): void
    {
        $client = new_client();
        $streamName = unique_name('poll-stream');
        $topicName = unique_name('poll-topic');
        $partitionId = 0;
        $messages = array_map(
            static fn (int $i): string => "Polling test {$i} - {$streamName}",
            range(0, 4),
        );

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $one = new PollOptions(count: 1);
            $firstMessages = $client->pollMessages($streamName, $topicName, $partitionId, self::CONSUMER_ID, PollingStrategy::first(), $one)->messages;
            assert_count(1, $firstMessages);
            assert_same([$messages[0]], collect_payloads($firstMessages));

            $lastMessages = $client->pollMessages($streamName, $topicName, $partitionId, self::CONSUMER_ID, PollingStrategy::last(), $one)->messages;
            assert_count(1, $lastMessages);
            assert_same([$messages[4]], collect_payloads($lastMessages));

            $offset = $firstMessages[0]->offset() + 2;
            $offsetMessages = $client->pollMessages(
                $streamName,
                $topicName,
                $partitionId,
                self::CONSUMER_ID,
                PollingStrategy::offset($offset),
                new PollOptions(count: 2),
            )->messages;
            assert_count(2, $offsetMessages);
            assert_same([$messages[2], $messages[3]], collect_payloads($offsetMessages));
            assert_same([$offset, $offset + 1], collect_offsets($offsetMessages));

            $storing = new PollOptions(count: 2, server_commit: ServerCommitPolicy::OnRead);
            $nextFirstBatch = $client->pollMessages($streamName, $topicName, $partitionId, self::CONSUMER_ID, PollingStrategy::next(), $storing)->messages;
            assert_count(2, $nextFirstBatch);
            assert_same([$messages[0], $messages[1]], collect_payloads($nextFirstBatch));

            $nextSecondBatch = $client->pollMessages($streamName, $topicName, $partitionId, self::CONSUMER_ID, PollingStrategy::next(), $storing)->messages;
            assert_count(2, $nextSecondBatch);
            assert_same([$messages[2], $messages[3]], collect_payloads($nextSecondBatch));

            $nextThirdBatch = $client->pollMessages($streamName, $topicName, $partitionId, self::CONSUMER_ID, PollingStrategy::next(), $storing)->messages;
            assert_count(1, $nextThirdBatch);
            assert_same([$messages[4]], collect_payloads($nextThirdBatch));
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A continuation reads after its batch, only in its partition and partition history')]
    public function testPollContinuesAfterTheBatch(): void
    {
        $client = new_client();
        $streamName = unique_name('continuation-stream');
        $topicName = unique_name('continuation-topic');
        $partitionId = 0;
        $messages = ['continuation-first', 'continuation-second'];
        $one = new PollOptions(count: 1);

        try {
            create_stream_and_topic($client, $streamName, $topicName, 2);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $first = $client->pollMessages($streamName, $topicName, $partitionId, self::CONSUMER_ID, PollingStrategy::first(), $one);
            assert_instance_of(PolledMessages::class, $first);
            assert_same($partitionId, $first->partition_id);
            assert_same(1, $first->count);
            assert_same(count($messages) - 1, $first->current_offset);
            assert_same($first->messages[0]->offset(), $first->checkpoint()->offset);
            assert_null($first->checkpointAt(1));

            $continued = $client->pollMessages($streamName, $topicName, $partitionId, self::CONSUMER_ID, $first->continuation(), $one);
            assert_same([$messages[1]], collect_payloads($continued->messages));

            $otherPartition = assert_throws(
                static fn () => $client->pollMessages($streamName, $topicName, $partitionId + 1, self::CONSUMER_ID, $first->continuation(), $one),
            );
            assert_same(self::INVALID_COMMAND_CODE, $otherPartition->getCode());

            $client->deleteTopic($streamName, $topicName);
            $client->createTopic($streamName, $topicName, 2, null, null, null, null);
            $recreated = assert_throws(
                static fn () => $client->pollMessages($streamName, $topicName, $partitionId, self::CONSUMER_ID, $first->continuation(), $one),
                'partition history is no longer available',
            );
            assert_same(self::HISTORY_UNAVAILABLE_CODE, $recreated->getCode());
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('Creating the same stream twice raises an error')]
    public function testDuplicateStreamCreation(): void
    {
        $client = new_client();
        $streamName = unique_name('duplicate-test');

        try {
            $client->createStream($streamName);
            assert_throws(static fn () => $client->createStream($streamName), 'already exists');
        } finally {
            cleanup_stream($client, $streamName);
        }
    }

    #[TestDox('Fetching a missing stream returns null')]
    public function testGetNonexistentStream(): void
    {
        $stream = new_client()->getStream(unique_name('nonexistent'));

        assert_null($stream);
    }

    #[TestDox('Creating a topic in a missing stream raises an error')]
    public function testCreateTopicInNonexistentStream(): void
    {
        $client = new_client();

        assert_throws(
            static fn () => $client->createTopic(unique_name('nonexistent'), 'test-topic', 1, null, null, null, null),
        );
    }

    #[TestDox('Consumer group metadata exposes name, stream, topic, partition, and empty offsets')]
    public function testConsumerGroupMeta(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-consumer');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');
        $partitionId = 0;

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $consumer = $client->consumerGroup(
                $consumerName,
                $streamName,
                $topicName,
                PollingStrategy::next(),
                client_commit: new ClientCommitPolicy(micros(5)),
            );

            assert_same($consumerName, $consumer->name());
            assert_same($streamName, $consumer->stream());
            assert_same($topicName, $consumer->topic());
            assert_same(0, $consumer->partitionId());
            assert_null($consumer->getLastConsumedOffset($partitionId));
            assert_null($consumer->getLastStoredOffset($partitionId));
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('Consumer group rejects zero duration options before initialization')]
    public function testConsumerGroupRejectsZeroDurationOptions(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-consumer');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');

        try {
            create_stream_and_topic($client, $streamName, $topicName);

            assert_throws(
                static fn () => new ClientCommitPolicy(0),
                'interval_micros',
            );

            assert_throws(
                static fn () => new ClientCommitPolicy(0, CommitEvent::eachBatch()),
                'interval_micros',
            );

            assert_throws(
                static fn () => $client->consumerGroup(
                    $consumerName,
                    $streamName,
                    $topicName,
                    PollingStrategy::next(),
                    polling_retry_interval_micros: 0,
                ),
                'polling_retry_interval_micros',
            );

            assert_throws(
                static fn () => $client->consumerGroup(
                    $consumerName,
                    $streamName,
                    $topicName,
                    PollingStrategy::next(),
                    init_retries: 0,
                    init_retry_interval_micros: 0,
                ),
                'init_retry_interval_micros',
            );
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('Consumer offsets require an explicit partition before any message is polled')]
    public function testConsumerOffsetCurrentPartitionRequiresPriorPoll(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-consumer');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $consumer = $client->consumerGroup($consumerName, $streamName, $topicName, PollingStrategy::next());

            assert_throws(static fn () => $consumer->storeOffset(0, null), 'partition_id');
            assert_throws(static fn () => $consumer->deleteOffset(null), 'partition_id');
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A consumer group callback receives messages in order')]
    public function testConsumeMessages(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-consumer');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');
        $partitionId = 0;
        $messages = array_map(
            static fn (int $i): string => "Consumer group test {$i} - {$streamName}",
            range(0, 4),
        );

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $consumer = $client->consumerGroup(
                $consumerName,
                $streamName,
                $topicName,
                PollingStrategy::next(),
                new PollOptions(count: 10, max_wait_micros: micros(1)),
                new ClientCommitPolicy(micros(5)),
            );
            $received = [];
            $count = $consumer->consumeMessages(
                static function (ReceiveMessage $message) use (&$received): void {
                    $received[] = $message->payload();
                },
                count($messages),
            );

            assert_same(count($messages), $count);
            assert_same($messages, $received);
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A consumer group iterator yields messages in order')]
    public function testIterMessages(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-iterator');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');
        $partitionId = 0;
        $messages = array_map(
            static fn (int $i): string => "Consumer iterator test {$i} - {$streamName}",
            range(0, 4),
        );

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $consumer = $client->consumerGroup(
                $consumerName,
                $streamName,
                $topicName,
                PollingStrategy::next(),
                client_commit: new ClientCommitPolicy(micros(5)),
            );
            $iterator = $consumer->iterMessages();
            $received = [];
            $keys = [];

            assert_instance_of(Iterator::class, $iterator);

            foreach ($iterator as $key => $message) {
                $keys[] = $key;
                $received[] = $message->payload();

                if (count($received) === count($messages)) {
                    break;
                }
            }

            assert_same(range(0, count($messages) - 1), $keys);
            assert_same($messages, $received);
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A consumer commits the checkpoint of a message after it consumed later messages')]
    public function testConsumerCommitsTheDeliveredCheckpoint(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-position');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');
        $partitionId = 0;
        $messages = ['position-first', 'position-second'];

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $consumer = $client->consumerGroup(
                $consumerName,
                $streamName,
                $topicName,
                PollingStrategy::next(),
                polling_retry_interval_micros: micros(1),
            );
            $received = [];
            $consumer->consumeMessages(
                static function (ReceiveMessage $message) use (&$received): void {
                    $received[] = $message;
                },
                count($messages),
            );
            assert_count(count($messages), $received);

            $first = $received[0];
            $checkpoint = $first->checkpoint();
            assert_same($first->partitionId(), $checkpoint->partition_id);
            assert_same($first->offset(), $checkpoint->offset);
            $consumer->commit($checkpoint);

            assert_same($first->offset(), $consumer->getLastStoredOffset($first->partitionId()));
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('Consumer group consumeMessages requires a finite limit')]
    public function testConsumeMessagesRequiresLimit(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-consumer');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $consumer = $client->consumerGroup($consumerName, $streamName, $topicName, PollingStrategy::next());

            assert_throws(static fn () => $consumer->consumeMessages(static fn (ReceiveMessage $message): null => null, null));
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A consumer reads again from a sought offset and delivers nothing after shutdown')]
    public function testConsumerSeeksAndShutsDown(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-seek');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');
        $partitionId = 0;
        $messages = ['seek-first', 'seek-second', 'seek-third'];

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $consumer = $client->consumerGroup($consumerName, $streamName, $topicName, PollingStrategy::next());
            $received = [];
            $collect = static function (ReceiveMessage $message) use (&$received): void {
                $received[] = $message->payload();
            };
            assert_same(count($messages), $consumer->consumeMessages($collect, count($messages)));

            $consumer->seek($partitionId, PollingStrategy::offset(1));
            assert_same(2, $consumer->consumeMessages($collect, 2));
            assert_same([...$messages, $messages[1], $messages[2]], $received);

            $consumer->shutdown();
            $consumer->shutdown();
            assert_same(0, $consumer->consumeMessages($collect, 1));
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('consumeMessages() counts a message after its callback returns and takes no message past its limit')]
    public function testConsumeMessagesCountsAfterTheCallbackAndStopsAtItsLimit(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-count');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');
        $partitionId = 0;
        $messages = ['count-first', 'count-second', 'count-third'];

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $consumer = $client->consumerGroup(
                $consumerName,
                $streamName,
                $topicName,
                PollingStrategy::next(),
                client_commit: new ClientCommitPolicy(on: CommitEvent::eachMessage()),
            );
            $stored = static fn (): ?int => $consumer->getLastStoredOffset($partitionId);
            $received = [];
            $storedInCallback = [];
            $collect = static function (ReceiveMessage $message) use ($stored, &$received, &$storedInCallback): void {
                $received[] = $message->payload();
                // The wait gives a count at delivery the time to store this offset before the callback returns.
                $storedInCallback[$message->offset()] = read_until($stored, $message->offset(), self::UNCOUNTED_WAIT_SECONDS);
            };

            assert_same(2, $consumer->consumeMessages($collect, 2));
            assert_same(array_slice($messages, 0, 2), $received);
            foreach ($storedInCallback as $offset => $storedOffset) {
                assert_true($storedOffset !== $offset, "message {$offset} must count only after its callback returns");
            }
            assert_same(1, $consumer->getLastConsumedOffset($partitionId), 'the run must take no message past its limit');
            assert_same(1, read_until($stored, 1, self::DEADLINE_SECONDS), 'the last message must count after its callback');

            assert_same(1, $consumer->consumeMessages($collect, 1));
            assert_same($messages, $received);
            $consumer->shutdown();
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('An exception of a consumeMessages() callback ends the run, is thrown, and its message still counts')]
    public function testConsumeMessagesThrowsTheCallbackExceptionAndCountsItsMessage(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-throw');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');
        $partitionId = 0;
        $messages = ['throw-first', 'throw-second'];

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $consumer = $client->consumerGroup(
                $consumerName,
                $streamName,
                $topicName,
                PollingStrategy::next(),
                client_commit: new ClientCommitPolicy(on: CommitEvent::eachMessage()),
            );
            $failure = new RuntimeException('callback failed');
            $thrown = assert_throws(
                static fn () => $consumer->consumeMessages(
                    static function (ReceiveMessage $message) use ($failure): void {
                        throw $failure;
                    },
                    count($messages),
                ),
            );

            assert_same($failure, $thrown);
            assert_same(0, $consumer->getLastConsumedOffset($partitionId), 'the exception must end the run at its message');
            assert_same(
                0,
                read_until(static fn (): ?int => $consumer->getLastStoredOffset($partitionId), 0, self::DEADLINE_SECONDS),
                'the message of the exception must count',
            );

            $received = [];
            $collect = static function (ReceiveMessage $message) use (&$received): void {
                $received[] = $message->payload();
            };
            assert_same(1, $consumer->consumeMessages($collect, 1));
            assert_same([$messages[1]], $received);
            $consumer->shutdown();
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A consumeMessages() callback can commit, and its calls that need the consumer alone throw')]
    public function testConsumeMessagesCallbackCommitsButCannotSeek(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-callback');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');
        $partitionId = 0;
        $payload = 'callback-calls';

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages($streamName, $topicName, $partitionId, [new SendMessage($payload)]);

            $consumer = $client->consumerGroup($consumerName, $streamName, $topicName, PollingStrategy::next());
            $storedInCallback = null;
            $consumer->consumeMessages(
                static function (ReceiveMessage $message) use ($consumer, $partitionId, &$storedInCallback): void {
                    $consumer->commit($message->checkpoint());
                    $storedInCallback = $consumer->getLastStoredOffset($partitionId);

                    assert_throws(
                        static fn () => $consumer->seek($partitionId, PollingStrategy::first()),
                        'seek() ' . self::CONSUMER_BUSY,
                    );
                    assert_throws(static fn () => $consumer->shutdown(), 'shutdown() ' . self::CONSUMER_BUSY);
                    assert_throws(
                        static fn () => $consumer->consumeMessages(static fn (ReceiveMessage $message): null => null, 1),
                        'consumeMessages() ' . self::CONSUMER_BUSY,
                    );
                    assert_throws(
                        static fn () => iterator_to_array($consumer->iterMessages()),
                        'iterMessages() ' . self::CONSUMER_BUSY,
                    );
                },
                1,
            );
            assert_same(0, $storedInCallback);

            $consumer->seek($partitionId, PollingStrategy::offset(0));
            $received = [];
            $consumer->consumeMessages(
                static function (ReceiveMessage $message) use (&$received): void {
                    $received[] = $message->payload();
                },
                1,
            );
            assert_same([$payload], $received);
            $consumer->shutdown();
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A reader delivers a batch, commits its checkpoint, seeks and closes')]
    public function testReaderReadsCommitsSeeksAndCloses(): void
    {
        $client = new_client();
        $readerName = unique_name('reader');
        $streamName = unique_name('reader-stream');
        $topicName = unique_name('reader-topic');
        $partitionId = 0;
        $messages = ['reader-first', 'reader-second', 'reader-third'];

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $reader = $client->reader($streamName, $topicName, $readerName, [$partitionId], PollingStrategy::first());
            assert_same([$partitionId], $reader->partitions());

            $batch = $reader->next();
            assert_same($partitionId, $batch->partition_id);
            assert_same($messages, collect_payloads($batch->messages));

            $reader->commit($batch->checkpoint());
            $afterCommit = $client->pollMessages($streamName, $topicName, $partitionId, $readerName, PollingStrategy::next());
            assert_same(0, $afterCommit->count, 'the commit must store the offset of the last message');

            $reader->seek($partitionId, PollingStrategy::offset(1));
            assert_same(array_slice($messages, 1), collect_payloads($reader->next()->messages));

            $reader->close();
            $reader->close();
            assert_throws(static fn () => $reader->next(), 'the reader is closed');
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A reader returns null at the timeout of next() and delivers a later batch on the next call')]
    public function testReaderNextReturnsNullAtItsTimeout(): void
    {
        $client = new_client();
        $readerName = unique_name('reader');
        $streamName = unique_name('reader-stream');
        $topicName = unique_name('reader-topic');
        $partitionId = 0;

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $reader = $client->reader($streamName, $topicName, $readerName, [$partitionId], PollingStrategy::first());
            try {
                assert_null($reader->next(self::EMPTY_READ_TIMEOUT_MICROS), 'an empty partition must time out');
                assert_null($reader->next(0), 'a zero timeout must not wait');
                assert_throws(static fn () => $reader->next(-1), "'timeout_micros' must be non-negative");

                $client->sendMessages($streamName, $topicName, $partitionId, [new SendMessage('after-timeout')]);
                // A bound instead of no limit: a lost batch fails the test instead of hanging it.
                $batch = $reader->next(micros(self::DEADLINE_SECONDS));
                assert_not_null($batch, 'the batch sent after the timeouts must arrive');
                assert_same(['after-timeout'], collect_payloads($batch->messages));
            } finally {
                $reader->close();
            }
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    #[TestDox('A group reader joins an existing consumer group and reads its assigned partition')]
    public function testGroupReaderReadsTheAssignedPartition(): void
    {
        $client = new_client();
        $groupName = unique_name('reader-group');
        $streamName = unique_name('reader-stream');
        $topicName = unique_name('reader-topic');
        $partitionId = 0;
        $messages = ['group-reader-first', 'group-reader-second'];

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $missing = assert_throws(static fn () => $client->groupReader($streamName, $topicName, $groupName));
            assert_instance_of(NotFoundException::class, $missing);

            // The client has no call that only creates a group: a consumer creates it and leaves.
            $client->consumerGroup($groupName, $streamName, $topicName)->shutdown();

            $reader = $client->groupReader($streamName, $topicName, $groupName, PollingStrategy::first());
            try {
                assert_same($messages, collect_payloads($reader->next()->messages));
                assert_same([$partitionId], $reader->partitions());
            } finally {
                $reader->close();
            }
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }
}
