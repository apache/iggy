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

// The canonical result forms of core/conformance/src/canonical.rs. The orchestrator decodes each
// result into its form and refuses a missing or unknown field. A form keeps only the fields that
// the PHP SDK gives, so a field that the SDK lacks fails the test instead of being made up here.

namespace Iggy\Conformance;

use Iggy\OptionSpec;
use Iggy\Partition;
use Iggy\PolledMessages;
use Iggy\ReceiveMessage;
use Iggy\StreamDetails;
use Iggy\TopicDetails;

/** The SDK's StreamDetails has no created_at, size_bytes or topics. */
function stream_details(StreamDetails $stream): array
{
    return [
        'id' => $stream->id,
        'name' => $stream->name,
        'messages_count' => $stream->messages_count,
        'topics_count' => $stream->topics_count,
        'options' => options($stream->options, $stream->derived_options),
    ];
}

/** The SDK's TopicDetails has no created_at or size_bytes. */
function topic_details(TopicDetails $topic): array
{
    return [
        'id' => $topic->id,
        'name' => $topic->name,
        'messages_count' => $topic->messages_count,
        'partitions_count' => $topic->partitions_count,
        'partitions' => array_map(partition(...), $topic->partitions),
        'options' => options($topic->options, $topic->derived_options),
    ];
}

/** The SDK's Partition has no created_at. */
function partition(Partition $partition): array
{
    return [
        'id' => $partition->id,
        'segments_count' => $partition->segments_count,
        'current_offset' => $partition->current_offset,
        'size_bytes' => $partition->size_bytes,
        'messages_count' => $partition->messages_count,
    ];
}

function polled_messages(PolledMessages $polled): array
{
    return [
        'partition_id' => $polled->partition_id,
        'current_offset' => $polled->current_offset,
        'count' => $polled->count,
        'messages' => array_map(message(...), $polled->messages),
    ];
}

/** The SDK's ReceiveMessage has no origin timestamp and no user headers. */
function message(ReceiveMessage $message): array
{
    $payload = $message->payload();
    $converted = [
        'offset' => $message->offset(),
        'id' => $message->id(),
        'timestamp' => $message->timestamp(),
        'checksum' => new Number($message->checksum()),
        'payload_hex' => bin2hex($payload),
    ];
    if (mb_check_encoding($payload, 'UTF-8')) {
        $converted['payload'] = $payload;
    }

    return $converted;
}

function received_message(ReceiveMessage $message): array
{
    return ['partition_id' => $message->partitionId(), 'message' => message($message)];
}

function option_spec(OptionSpec $spec): array
{
    return [
        'key' => $spec->key,
        'kind' => $spec->kind,
        'updatable' => $spec->updatable,
        'resettable' => $spec->resettable,
        'secret' => $spec->secret,
        'read_only' => $spec->read_only,
        'critical' => $spec->critical,
    ];
}

/**
 * The options that a client set and that the server derived, as one map. The SDK gives each
 * value as a PHP scalar without its kind, so an entry has no kind.
 */
function options(array $explicit, array $derived): \stdClass
{
    $entries = [];
    foreach ($derived as $key => $value) {
        $entries[$key] = ['value' => value_text($value), 'explicit' => false];
    }
    foreach ($explicit as $key => $value) {
        $entries[$key] = ['value' => value_text($value), 'explicit' => true];
    }

    return (object) $entries;
}

/**
 * The reversible text of a value as far as its PHP scalar holds it: true or false, base-10
 * integers, and 0x with the IEEE-754 bits of a float.
 */
function value_text(bool|int|float|string $value): string
{
    return match (true) {
        is_bool($value) => $value ? 'true' : 'false',
        is_float($value) => '0x' . bin2hex(pack('E', $value)),
        default => (string) $value,
    };
}
