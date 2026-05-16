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

use PHPUnit\Framework\TestCase;

final class IggySdkTest extends TestCase
{
    public function testPing(): void
    {
        new_client()->ping();
    }

    public function testClientNotNull(): void
    {
        assert_true(new_client() instanceof IggyClient);
    }

    public function testClientFromConnectionString(): void
    {
        $client = new_connection_string_client();
        $client->ping();
    }

    public function testCreateAndGetStream(): void
    {
        $client = new_client();
        $streamName = unique_name('test-stream');

        $client->createStream($streamName);
        $stream = $client->getStream($streamName);

        assert_not_null($stream);
        assert_same($streamName, $stream->name);
        assert_true($stream->id >= 0, 'expected non-negative stream id');
    }

    public function testNewStreamHasNoTopics(): void
    {
        $client = new_client();
        $streamName = unique_name('test-stream');

        $client->createStream($streamName);
        $stream = $client->getStream($streamName);

        assert_not_null($stream);
        assert_same($streamName, $stream->name);
        assert_true($stream->id > 0, 'expected positive stream id');
        assert_same(0, $stream->topics_count);
    }

    public function testCreateAndGetTopic(): void
    {
        $client = new_client();
        $streamName = unique_name('test-stream');
        $topicName = unique_name('test-topic');

        $client->createStream($streamName);
        $client->createTopic($streamName, $topicName, 2, null, null, null, null);
        $topic = $client->getTopic($streamName, $topicName);

        assert_not_null($topic);
        assert_same($topicName, $topic->name);
        assert_true($topic->id >= 0, 'expected non-negative topic id');
        assert_same(2, $topic->partitions_count);
    }

    public function testListTopicsViaGetTopic(): void
    {
        $client = new_client();
        $streamName = unique_name('test-stream');
        $topicName = unique_name('test-topic');

        create_stream_and_topic($client, $streamName, $topicName);
        $topic = $client->getTopic($streamName, $topicName);

        assert_not_null($topic);
        assert_same($topicName, $topic->name);
        assert_true($topic->id >= 0, 'expected non-negative topic id');
        assert_same(1, $topic->partitions_count);
    }

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

        create_stream_and_topic($client, $streamName, $topicName);
        $client->sendMessages(
            $streamName,
            $topicName,
            $partitionId,
            array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
        );

        $polled = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::first(), 10, true);
        assert_true(count($polled) >= count($messages), 'expected at least the sent messages');
        assert_same($messages, array_slice(collect_payloads($polled), 0, count($messages)));
    }

    public function testMessageProperties(): void
    {
        $client = new_client();
        $streamName = unique_name('msg-stream');
        $topicName = unique_name('msg-topic');
        $partitionId = 0;
        $payload = unique_name('Property test');

        create_stream_and_topic($client, $streamName, $topicName);
        $client->sendMessages($streamName, $topicName, $partitionId, [new SendMessage($payload)]);

        $polled = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::last(), 1, true);
        assert_true(count($polled) >= 1, 'expected one message');

        $message = $polled[0];
        assert_same($payload, $message->payload());
        assert_true($message->offset() >= 0, 'expected non-negative offset');
        assert_true($message->id() !== '', 'expected message id');
        assert_true($message->timestamp() > 0, 'expected positive timestamp');
        assert_true(ctype_digit($message->checksum()), 'expected numeric checksum');
        assert_true($message->length() > 0, 'expected positive length');
        assert_same($partitionId, $message->partitionId());
    }

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

        create_stream_and_topic($client, $streamName, $topicName);
        $client->sendMessages(
            $streamName,
            $topicName,
            $partitionId,
            array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
        );

        $firstMessages = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::first(), 1, false);
        assert_true(count($firstMessages) >= 1, 'first strategy returned no messages');

        $lastMessages = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::last(), 1, false);
        assert_true(count($lastMessages) >= 1, 'last strategy returned no messages');

        $nextMessages = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::next(), 2, false);
        assert_true(count($nextMessages) >= 1, 'next strategy returned no messages');

        $offsetMessages = $client->pollMessages(
            $streamName,
            $topicName,
            $partitionId,
            PollingStrategy::offset($firstMessages[0]->offset()),
            1,
            false,
        );
        assert_true(count($offsetMessages) >= 1, 'offset strategy returned no messages');
    }

    public function testDuplicateStreamCreation(): void
    {
        $client = new_client();
        $streamName = unique_name('duplicate-test');

        $client->createStream($streamName);
        assert_throws(static fn () => $client->createStream($streamName), 'already exists');
    }

    public function testGetNonexistentStream(): void
    {
        $stream = new_client()->getStream(unique_name('nonexistent'));

        assert_null($stream);
    }

    public function testCreateTopicInNonexistentStream(): void
    {
        $client = new_client();

        assert_throws(
            static fn () => $client->createTopic(unique_name('nonexistent'), 'test-topic', 1, null, null, null, null),
        );
    }

    public function testConsumerGroupMeta(): void
    {
        $client = new_client();
        $consumerName = unique_name('consumer-group-consumer');
        $streamName = unique_name('consumer-group-stream');
        $topicName = unique_name('consumer-group-topic');
        $partitionId = 0;

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

        assert_same($streamName, $consumer->stream());
        assert_same($topicName, $consumer->topic());
        assert_same(0, $consumer->partitionId());
        assert_null($consumer->getLastConsumedOffset($partitionId));
        assert_null($consumer->getLastStoredOffset($partitionId));
    }

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
    }

}
