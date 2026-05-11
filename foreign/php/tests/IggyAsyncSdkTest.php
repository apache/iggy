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

final class IggyAsyncSdkTest
{
    public function testAsyncClientRegistered(): void
    {
        assert_true(class_exists(IggyAsyncClient::class), 'IggyAsyncClient is not registered');
    }

    public function testAsyncPing(): void
    {
        run_async(function (): void {
            $client = new IggyAsyncClient(server_host() . ':' . server_port());
            $client->connect();
            $client->loginUser(env_or_default('IGGY_USERNAME', 'iggy'), env_or_default('IGGY_PASSWORD', 'iggy'));
            $client->ping();
        });
    }

    public function testAsyncSendAndPollMessages(): void
    {
        run_async(function (): void {
            $client = new IggyAsyncClient(server_host() . ':' . server_port());
            $client->connect();
            $client->loginUser(env_or_default('IGGY_USERNAME', 'iggy'), env_or_default('IGGY_PASSWORD', 'iggy'));

            $streamName = unique_name('async-msg-stream');
            $topicName = unique_name('async-msg-topic');
            $partitionId = 0;
            $messages = array_map(
                static fn (int $i): string => "Async message {$i} - {$streamName}",
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
            assert_true(count($polled) >= count($messages), 'expected at least the sent async messages');
            assert_same($messages, array_slice(collect_payloads($polled), 0, count($messages)));
        });
    }

    public function testAsyncConsumerGroupMeta(): void
    {
        run_async(function (): void {
            $client = new IggyAsyncClient(server_host() . ':' . server_port());
            $client->connect();
            $client->loginUser(env_or_default('IGGY_USERNAME', 'iggy'), env_or_default('IGGY_PASSWORD', 'iggy'));

            $consumerName = unique_name('async-consumer-group-consumer');
            $streamName = unique_name('async-consumer-group-stream');
            $topicName = unique_name('async-consumer-group-topic');
            $partitionId = 0;

            create_stream_and_topic($client, $streamName, $topicName);
            $consumer = $this->consumerGroup($client, $consumerName, $streamName, $topicName, $partitionId);

            assert_true($consumer instanceof IggyAsyncConsumer);
            assert_same($streamName, $consumer->stream());
            assert_same($topicName, $consumer->topic());
            assert_same(0, $consumer->partitionId());
            assert_null($consumer->getLastConsumedOffset($partitionId));
            assert_null($consumer->getLastStoredOffset($partitionId));
        });
    }

    public function testAsyncConsumeMessages(): void
    {
        run_async(function (): void {
            $client = new IggyAsyncClient(server_host() . ':' . server_port());
            $client->connect();
            $client->loginUser(env_or_default('IGGY_USERNAME', 'iggy'), env_or_default('IGGY_PASSWORD', 'iggy'));

            $consumerName = unique_name('async-consumer-group-consumer');
            $streamName = unique_name('async-consumer-group-stream');
            $topicName = unique_name('async-consumer-group-topic');
            $partitionId = 0;
            $messages = array_map(
                static fn (int $i): string => "Async consumer group test {$i} - {$streamName}",
                range(0, 4),
            );

            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $consumer = $this->consumerGroup($client, $consumerName, $streamName, $topicName, $partitionId);
            $received = [];
            $count = $consumer->consumeMessages(
                static function (ReceiveMessage $message) use (&$received): void {
                    $received[] = $message->payload();
                },
                count($messages),
            );

            assert_same(count($messages), $count);
            assert_same($messages, $received);
        });
    }

    public function testAsyncIterMessages(): void
    {
        run_async(function (): void {
            $client = new IggyAsyncClient(server_host() . ':' . server_port());
            $client->connect();
            $client->loginUser(env_or_default('IGGY_USERNAME', 'iggy'), env_or_default('IGGY_PASSWORD', 'iggy'));

            $consumerName = unique_name('async-consumer-group-consumer');
            $streamName = unique_name('async-consumer-group-stream');
            $topicName = unique_name('async-consumer-group-topic');
            $partitionId = 0;
            $messages = array_map(
                static fn (int $i): string => "Async iterator test {$i} - {$streamName}",
                range(0, 4),
            );

            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $consumer = $this->consumerGroup($client, $consumerName, $streamName, $topicName, $partitionId);
            $iterator = $consumer->iterMessages();
            assert_true($iterator instanceof IggyAsyncReceiveMessageIterator);

            $received = [];
            while (count($received) < count($messages)) {
                $message = $iterator->next();
                assert_not_null($message);
                $received[] = $message->payload();
            }

            assert_same($messages, $received);
        });
    }

    private function consumerGroup(
        IggyAsyncClient $client,
        string $consumerName,
        string $streamName,
        string $topicName,
        int $partitionId,
    ): IggyAsyncConsumer {
        return $client->consumerGroup(
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
    }
}
