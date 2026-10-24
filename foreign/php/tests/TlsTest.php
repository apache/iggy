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
use Iggy\MetadataChangeGroup;
use Iggy\MetadataChangeKind;
use Iggy\MetadataChangeStart;
use Iggy\MetadataChangeStatus;
use Iggy\MetadataStreamChange;
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

        assert_true($client instanceof IggyClient, 'TLS client connected and pinged');
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

    public function testMetadataChangesOverTls(): void
    {
        $connectionString = getenv('IGGY_TLS_CONNECTION_STRING');
        if ($connectionString === false || $connectionString === '') {
            $this->markTestSkipped('set IGGY_TLS_CONNECTION_STRING to run TLS tests');
        }

        $client = IggyClient::fromConnectionString($connectionString);
        $client->connect();
        $groups = [MetadataChangeGroup::Streams];
        $latest = $client->getMetadataChanges(MetadataChangeStart::latest(), $groups);
        assert_same(MetadataChangeStatus::Ok, $latest->status);
        $streamName = unique_name('tls-changes-stream');

        try {
            $client->createStream($streamName);
            $page = $client->getMetadataChanges(MetadataChangeStart::after($latest->next), $groups);
            assert_same(MetadataChangeStatus::Ok, $page->status);
            $created = array_values(array_filter(
                $page->records,
                static fn ($record): bool => $record->change instanceof MetadataStreamChange
                    && $record->change->name === $streamName,
            ));
            assert_count(1, $created);
            assert_same(MetadataChangeKind::StreamCreated, $created[0]->kind);
        } finally {
            cleanup_stream($client, $streamName);
        }
    }

    public function testConnectWithoutTlsShouldFail(): void
    {
        $address = getenv('IGGY_TLS_PLAINTEXT_ADDRESS');
        if ($address === false || $address === '') {
            $this->markTestSkipped('set IGGY_TLS_PLAINTEXT_ADDRESS to run this TLS failure test');
        }

        $client = new IggyClient($address);
        assert_throws(static function () use ($client): void {
            $client->connect();
            $client->ping();
        });
    }
}
