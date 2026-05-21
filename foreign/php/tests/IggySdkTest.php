<?php

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

declare(strict_types=1);

use Iggy\AutoCommit;
use Iggy\Client as IggyClient;
use Iggy\PollingStrategy;
use Iggy\ReceiveMessage;
use Iggy\SendMessage;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class IggySdkTest extends TestCase
{
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
        $client->loginUser(env_or_default('IGGY_USERNAME', 'iggy'), env_or_default('IGGY_PASSWORD', 'iggy'));
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

            $polled = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::first(), 10, true);
            assert_count(count($messages), $polled);
            assert_same($messages, collect_payloads($polled));
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

            $polled = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::last(), 1, true);
            assert_count(1, $polled);

            $message = $polled[0];
            assert_same($payload, $message->payload());
            assert_true($message->offset() >= 0, 'expected non-negative offset');
            assert_true($message->id() !== '', 'expected message id');
            assert_true($message->timestamp() > 0, 'expected positive timestamp');
            assert_true(ctype_digit($message->checksum()), 'expected numeric checksum');
            assert_true($message->length() > 0, 'expected positive length');
            assert_same($partitionId, $message->partitionId());
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

            $firstMessages = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::first(), 1, false);
            assert_count(1, $firstMessages);
            assert_same([$messages[0]], collect_payloads($firstMessages));

            $lastMessages = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::last(), 1, false);
            assert_count(1, $lastMessages);
            assert_same([$messages[4]], collect_payloads($lastMessages));

            $offset = $firstMessages[0]->offset() + 2;
            $offsetMessages = $client->pollMessages(
                $streamName,
                $topicName,
                $partitionId,
                PollingStrategy::offset($offset),
                2,
                false,
            );
            assert_count(2, $offsetMessages);
            assert_same([$messages[2], $messages[3]], collect_payloads($offsetMessages));
            assert_same([$offset, $offset + 1], collect_offsets($offsetMessages));

            $nextFirstBatch = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::next(), 2, true);
            assert_count(2, $nextFirstBatch);
            assert_same([$messages[0], $messages[1]], collect_payloads($nextFirstBatch));

            $nextSecondBatch = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::next(), 2, true);
            assert_count(2, $nextSecondBatch);
            assert_same([$messages[2], $messages[3]], collect_payloads($nextSecondBatch));

            $nextThirdBatch = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::next(), 2, true);
            assert_count(1, $nextThirdBatch);
            assert_same([$messages[4]], collect_payloads($nextThirdBatch));
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
                $partitionId,
                PollingStrategy::next(),
                10,
                AutoCommit::interval(micros(5)),
                true,
                true,
                micros(1),
                null,
                null,
                null,
                false,
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
                static fn () => $client->consumerGroup(
                    $consumerName,
                    $streamName,
                    $topicName,
                    0,
                    PollingStrategy::next(),
                    10,
                    AutoCommit::disabled(),
                    true,
                    true,
                    0,
                    null,
                    null,
                    null,
                    false,
                ),
                'poll_interval_micros',
            );

            assert_throws(
                static fn () => $client->consumerGroup(
                    $consumerName,
                    $streamName,
                    $topicName,
                    0,
                    PollingStrategy::next(),
                    10,
                    AutoCommit::disabled(),
                    true,
                    true,
                    micros(1),
                    0,
                    null,
                    null,
                    false,
                ),
                'polling_retry_interval_micros',
            );

            assert_throws(
                static fn () => $client->consumerGroup(
                    $consumerName,
                    $streamName,
                    $topicName,
                    0,
                    PollingStrategy::next(),
                    10,
                    AutoCommit::disabled(),
                    true,
                    true,
                    micros(1),
                    null,
                    0,
                    0,
                    false,
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
            $consumer = $client->consumerGroup(
                $consumerName,
                $streamName,
                $topicName,
                0,
                PollingStrategy::next(),
                10,
                AutoCommit::disabled(),
                true,
                true,
                micros(1),
                null,
                null,
                null,
                false,
            );

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
                $partitionId,
                PollingStrategy::next(),
                10,
                AutoCommit::interval(micros(5)),
                true,
                true,
                micros(1),
                null,
                null,
                null,
                false,
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

    #[TestDox('Consumer group consumeMessages requires a finite limit')]
    public function testConsumeMessagesRequiresLimit(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-consumer');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $consumer = $client->consumerGroup(
                $consumerName,
                $streamName,
                $topicName,
                0,
                PollingStrategy::next(),
                10,
                AutoCommit::disabled(),
                true,
                true,
                micros(1),
                null,
                null,
                null,
                false,
            );

            assert_throws(static fn () => $consumer->consumeMessages(static fn (ReceiveMessage $message): null => null, null));
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }
}
