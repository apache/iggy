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

const ORDER_CREATED_TYPE = 'OrderCreated';
const ORDER_CONFIRMED_TYPE = 'OrderConfirmed';
const ORDER_REJECTED_TYPE = 'OrderRejected';

const MESSAGES_COUNT = 6;

/**
 * Wraps a payload in a message envelope. The envelope carries the message type
 * and the payload as a JSON string, so a single topic can hold several kinds
 * of messages.
 */
function to_json_envelope(string $messageType, array $payload): string
{
    return json_encode([
        'message_type' => $messageType,
        'payload' => json_encode($payload, JSON_THROW_ON_ERROR),
    ], JSON_THROW_ON_ERROR);
}

function generate_envelope(int $orderId): string
{
    return match ($orderId % 3) {
        0 => to_json_envelope(ORDER_CREATED_TYPE, [
            'orderId' => "order-{$orderId}",
            'customerId' => 'customer-' . random_int(0, 99),
            'amount' => random_int(0, 9999),
        ]),
        1 => to_json_envelope(ORDER_CONFIRMED_TYPE, [
            'orderId' => 'order-' . intdiv($orderId, 3),
            'timestamp' => (int) (microtime(true) * 1000),
        ]),
        default => to_json_envelope(ORDER_REJECTED_TYPE, [
            'orderId' => 'order-' . intdiv($orderId, 3),
            'reason' => 'Insufficient balance',
        ]),
    };
}

$stream = 'php-message-envelope-stream';
$topic = 'php-message-envelope-topic';
$client = iggy_client();

ensure_stream_and_topic($client, $stream, $topic);

$payloads = array_map(
    static fn (int $orderId): string => generate_envelope($orderId),
    range(1, MESSAGES_COUNT),
);

foreach ($payloads as $payload) {
    echo "Sending envelope: {$payload}", PHP_EOL;
}

send_payloads($client, $stream, $topic, $payloads);

echo 'Sent ' . MESSAGES_COUNT . ' PHP message envelopes', PHP_EOL;
