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

const SOURCES = ['browser', 'mobile', 'desktop', 'email', 'network', 'other'];
const STATES = ['active', 'inactive', 'blocked', 'deleted', 'unknown'];
const DOMAINS = [
    'gmail.com',
    'yahoo.com',
    'hotmail.com',
    'outlook.com',
    'aol.com',
];

const MAX_BATCHES = 100;
const MIN_RECORDS_PER_BATCH = 100;
const MAX_RECORDS_PER_BATCH = 499;

$streamName = getenv('IGGY_STREAM') ?: 'qw';
$topicName = getenv('IGGY_TOPIC') ?: 'records';

function random_alnum(int $length): string
{
    $alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    $maxIndex = strlen($alphabet) - 1;
    $result = '';

    for ($i = 0; $i < $length; $i++) {
        $result .= $alphabet[random_int(0, $maxIndex)];
    }

    return $result;
}

function random_record(): array
{
    $createdAt = new DateTimeImmutable();
    $createdAt = $createdAt->modify('-' . random_int(0, 999) . ' days');

    return [
        'user_id' => 'user_' . random_int(1, 99),
        'user_type' => random_int(1, 4),
        'email' => random_alnum(random_int(3, 19)) . '@' . DOMAINS[array_rand(DOMAINS)],
        'source' => SOURCES[array_rand(SOURCES)],
        'state' => STATES[array_rand(STATES)],
        'message' => random_alnum(random_int(10, 99)),
        'created_at' => $createdAt->format(DateTimeInterface::ATOM),
    ];
}

$client = iggy_client();

ensure_stream_and_topic($client, $streamName, $topicName);

$totalMessages = 0;

for ($batch = 1; $batch <= MAX_BATCHES; $batch++) {
    $recordsCount = random_int(MIN_RECORDS_PER_BATCH, MAX_RECORDS_PER_BATCH);
    $payloads = [];

    for ($i = 0; $i < $recordsCount; $i++) {
        $payloads[] = json_encode(random_record(), JSON_THROW_ON_ERROR);
    }

    send_payloads($client, $streamName, $topicName, $payloads);

    $totalMessages += $recordsCount;

    echo sprintf(
        "Batch %d/%d: sent %d messages (total: %d)%s",
        $batch,
        MAX_BATCHES,
        $recordsCount,
        $totalMessages,
        PHP_EOL
    );
}

echo sprintf("Successfully produced %d messages.%s", $totalMessages, PHP_EOL);
