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

use Iggy\Client;
use Iggy\PollingStrategy;
use Iggy\SendMessage;

function iggy_connection_string(): string
{
    $configured = getenv('IGGY_CONNECTION_STRING');
    if ($configured !== false && $configured !== '') {
        return $configured;
    }

    $host = getenv('IGGY_HOST') ?: '127.0.0.1';
    $port = getenv('IGGY_PORT') ?: '8090';
    $username = rawurlencode(getenv('IGGY_USERNAME') ?: 'iggy');
    $password = rawurlencode(getenv('IGGY_PASSWORD') ?: 'iggy');

    return "iggy+tcp://{$username}:{$password}@{$host}:{$port}";
}

function iggy_client(): Client
{
    $client = Client::fromConnectionString(iggy_connection_string());
    $client->connect();

    return $client;
}

function ensure_stream_and_topic(Client $client, string $stream, string $topic): void
{
    if ($client->getStream($stream) === null) {
        $client->createStream($stream);
    }

    if ($client->getTopic($stream, $topic) === null) {
        $client->createTopic($stream, $topic, 1);
    }
}

function send_payloads(Client $client, string $stream, string $topic, array $payloads): void
{
    $messages = array_map(
        static fn (string $payload): SendMessage => new SendMessage($payload),
        $payloads,
    );

    $client->sendMessages($stream, $topic, 0, $messages);
}

function print_polled_messages(Client $client, string $stream, string $topic, int $count): void
{
    $messages = $client->pollMessages($stream, $topic, 0, PollingStrategy::first(), $count, true);

    if (count($messages) < $count) {
        throw new RuntimeException("Expected {$count} messages, received " . count($messages));
    }

    foreach ($messages as $message) {
        echo "Received message at offset {$message->offset()}: {$message->payload()}", PHP_EOL;
    }
}

function example_payloads(string $prefix, int $count): array
{
    return array_map(
        static fn (int $index): string => "{$prefix}-message-{$index}",
        range(1, $count),
    );
}
