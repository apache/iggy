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

require_once __DIR__ . '/../src/common.php';

use Iggy\CommitPolicy;
use Iggy\PollingStrategy;
use Iggy\PollOptions;

const ORDER_CREATED_TYPE = 'OrderCreated';
const ORDER_CONFIRMED_TYPE = 'OrderConfirmed';
const ORDER_REJECTED_TYPE = 'OrderRejected';

const MESSAGES_PER_POLL = 10;

function log_line(string $message): void
{
    echo $message, PHP_EOL;
}

/**
 * Decodes the inner payload, which the envelope carries as a JSON string.
 */
function decode_payload(string $payload): array
{
    $decoded = json_decode($payload, true, 512, JSON_THROW_ON_ERROR);
    if (!is_array($decoded)) {
        throw new UnexpectedValueException('Envelope payload is not a JSON object');
    }

    return $decoded;
}

function handle_message(string $raw): void
{
    try {
        // The message envelope lets one topic carry different message types.
        $envelope = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        if (
            !is_array($envelope)
            || !is_string($envelope['message_type'] ?? null)
            || !is_string($envelope['payload'] ?? null)
        ) {
            throw new UnexpectedValueException('Envelope must contain string "message_type" and "payload"');
        }

        $messageType = $envelope['message_type'];
        log_line("Handling message type: {$messageType}...");

        match ($messageType) {
            ORDER_CREATED_TYPE => log_line(
                'Order Created: ' . json_encode(decode_payload($envelope['payload'])),
            ),
            ORDER_CONFIRMED_TYPE => log_line(
                'Order Confirmed: ' . json_encode(decode_payload($envelope['payload'])),
            ),
            ORDER_REJECTED_TYPE => log_line(
                'Order Rejected: ' . json_encode(decode_payload($envelope['payload'])),
            ),
            default => log_line("Received unknown message type: {$messageType}, skipping"),
        };
    } catch (JsonException | UnexpectedValueException $e) {
        log_line('Skipping malformed message: ' . $e->getMessage());
    }
}

$stream = 'php-message-envelope-stream';
$topic = 'php-message-envelope-topic';
$client = iggy_client();

ensure_stream_and_topic($client, $stream, $topic);

$messages = $client->pollMessages(
    $stream,
    $topic,
    0,
    CONSUMER_ID,
    PollingStrategy::first(),
    new PollOptions(count: MESSAGES_PER_POLL, commit: CommitPolicy::onRead()),
)->messages;

if (count($messages) === 0) {
    log_line('No messages available. Run producer.php first.');
    exit(0);
}

foreach ($messages as $message) {
    handle_message($message->payload());
}

log_line('Consumed ' . count($messages) . ' message(s).');
