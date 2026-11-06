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

namespace Iggy\Conformance;

use Iggy\AutoCommit;
use Iggy\CommitPolicy;
use Iggy\MetadataChangeCursor;
use Iggy\MetadataChangeGroup;
use Iggy\MetadataChangeStart;
use Iggy\MetadataCurrentPageToken;
use Iggy\PollingStrategy;
use Iggy\SendMessage;

/** The u64 maximum, which the SDK reads as never expire and as unlimited. */
const U64_MAX = '18446744073709551615';

/**
 * The arguments with every name present, null when absent. A name that the agent does not know
 * fails, so that a new argument shows up as a failure instead of being ignored.
 *
 * @param list<string> $names
 */
function fields(array $args, array $names): array
{
    $unknown = array_diff(array_keys($args), $names);
    if ($unknown !== []) {
        throw new Failure('unknown arguments: ' . implode(', ', $unknown));
    }

    return $args + array_fill_keys($names, null);
}

/** A keyword such as "first", or a one-key object such as {"offset": 3}, as its key and value. */
function keyed(mixed $value): array
{
    if (is_string($value)) {
        return [$value, null];
    }
    if (is_array($value) && count($value) === 1) {
        $key = array_key_first($value);

        return [$key, $value[$key]];
    }
    throw new Failure('expected a keyword or a one-key object: ' . json($value));
}

/** An identifier: a number for an id, text for a name. */
function identifier(mixed $value): int|string
{
    return is_int($value) || is_string($value)
        ? $value
        : throw new Failure('an identifier is a number or text: ' . json($value));
}

/**
 * The consumer of a call and whether it is a group. Without one the contract names consumer 0,
 * which the agent passes, because the PHP SDK has no default consumer.
 *
 * @return array{int|string, bool}
 */
function consumer(mixed $value): array
{
    if ($value === null) {
        return [0, false];
    }
    [$kind, $id] = keyed($value);

    return match ($kind) {
        'consumer' => [identifier($id), false],
        'group' => [identifier($id), true],
        default => throw new Failure("unknown consumer kind {$kind}"),
    };
}

function strategy(mixed $value): PollingStrategy
{
    [$kind, $argument] = keyed($value);

    return match ($kind) {
        'first' => PollingStrategy::first(),
        'last' => PollingStrategy::last(),
        'next' => PollingStrategy::next(),
        'offset' => PollingStrategy::offset($argument),
        'timestamp' => PollingStrategy::timestamp($argument),
        default => throw new Failure("unknown strategy {$kind}"),
    };
}

function commit_policy(?string $commit): CommitPolicy
{
    return match ($commit ?? 'manual') {
        'manual' => CommitPolicy::manual(),
        'on_read' => CommitPolicy::onRead(),
        // The default AutoCommit counts a message after processing.
        'after_processing' => CommitPolicy::auto(AutoCommit::default()),
        default => throw new Failure("unknown commit policy {$commit}"),
    };
}

/** The partition of a send. The SDK sends only to a partition that the caller names. */
function partition_id(mixed $partitioning): int
{
    [$kind, $value] = keyed($partitioning);

    return match ($kind) {
        'partition_id' => $value,
        'balanced', 'messages_key' => throw new Unsupported("the SDK sends to a partition id, not with {$kind} partitioning"),
        default => throw new Failure("unknown partitioning {$kind}"),
    };
}

function send_message(array $message): SendMessage
{
    $message = fields($message, ['id', 'payload', 'payload_hex', 'headers']);
    if ($message['id'] !== null) {
        throw new Unsupported('the SDK cannot set the id of a message');
    }
    if (($message['headers'] ?? []) !== []) {
        throw new Unsupported('the SDK cannot send user headers');
    }

    return new SendMessage(match (true) {
        $message['payload'] !== null && $message['payload_hex'] === null => $message['payload'],
        $message['payload'] === null && $message['payload_hex'] !== null => hex_bytes($message['payload_hex']),
        default => throw new Failure('a message needs exactly one of payload and payload_hex'),
    });
}

/** The message expiry or size cap of a new topic: null leaves it to the server default. */
function topic_limit(mixed $value, string $forever): int|string|null
{
    return match ($value) {
        null, 'server_default' => null,
        $forever => U64_MAX,
        default => $value,
    };
}

/** The message expiry or size cap of an update, which resets a key instead of naming a default. */
function updated_topic_limit(mixed $value, string $forever, string $key): int|string|null
{
    return $value === 'server_default'
        ? throw new Unsupported("the SDK resets {$key} to the server default with reset, not with server_default")
        : topic_limit($value, $forever);
}

function metadata_start(mixed $value): MetadataChangeStart
{
    [$kind, $argument] = keyed($value);

    return match ($kind) {
        'earliest' => MetadataChangeStart::earliest(),
        'latest' => MetadataChangeStart::latest(),
        'after' => MetadataChangeStart::after(MetadataChangeCursor::parse($argument)),
        'current' => MetadataChangeStart::current(
            $argument === null ? null : MetadataCurrentPageToken::parse($argument),
        ),
        default => throw new Failure("unknown metadata start {$kind}"),
    };
}

/** @return list<MetadataChangeGroup> */
function metadata_groups(?array $names): array
{
    return array_map(
        static fn (string $name): MetadataChangeGroup => MetadataChangeGroup::tryFrom($name)
            ?? throw new Failure("unknown metadata change group {$name}"),
        $names ?? [],
    );
}

function micros(?int $milliseconds): ?int
{
    return $milliseconds === null ? null : $milliseconds * 1000;
}

function hex_bytes(string $text): string
{
    return preg_match('/^(?:[0-9a-fA-F]{2})*$/', $text) === 1
        ? hex2bin($text)
        : throw new Failure("invalid hex: {$text}");
}
