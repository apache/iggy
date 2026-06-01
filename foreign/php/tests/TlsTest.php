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
use Iggy\PollingStrategy;
use Iggy\SendMessage;
use PHPUnit\Framework\TestCase;

final class TlsTest extends TestCase
{
    public function testConnectionStringWithTlsParams(): void
    {
        $connectionString = getenv('IGGY_TLS_CONNECTION_STRING');
        if ($connectionString === false || $connectionString === '') {
            $this->markTestSkipped('set IGGY_TLS_CONNECTION_STRING to run TLS tests');
        }

        $client = IggyClient::fromConnectionString($connectionString);
        $client->connect();
        $client->ping();
    }

    public function testProduceAndConsumeOverTls(): void
    {
        $connectionString = getenv('IGGY_TLS_CONNECTION_STRING');
        if ($connectionString === false || $connectionString === '') {
            $this->markTestSkipped('set IGGY_TLS_CONNECTION_STRING to run TLS tests');
        }

        $client = IggyClient::fromConnectionString($connectionString);
        $client->connect();
        $client->ping();

        $streamName = unique_name('tls-msg-stream');
        $topicName = unique_name('tls-test-topic');
        $partitionId = 0;
        $messages = array_map(static fn (int $i): string => "tls-message-{$i}", range(0, 2));

        try {
            create_stream_and_topic($client, $streamName, $topicName);
            $client->sendMessages(
                $streamName,
                $topicName,
                $partitionId,
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $messages),
            );

            $polled = $client->pollMessages($streamName, $topicName, $partitionId, PollingStrategy::first(), 10, true);
            assert_count(count($messages), $polled, 'expected TLS messages');
            assert_same($messages, collect_payloads($polled));
        } finally {
            cleanup_stream_with_topics($client, $streamName, [$topicName]);
        }
    }

    public function testConnectWithoutTlsShouldFail(): void
    {
        $address = getenv('IGGY_TLS_PLAINTEXT_ADDRESS');
        if ($address === false || $address === '') {
            $this->markTestSkipped('set IGGY_TLS_PLAINTEXT_ADDRESS to run this TLS failure test');
        }

        $client = new IggyClient($address);
        assert_throws(static fn () => $client->connect());
    }
}
