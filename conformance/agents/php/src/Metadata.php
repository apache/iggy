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

// The canonical form of a metadata changes answer is the HTTP JSON that the fixture
// core/common/fixtures/metadata_changes.json names "expected". The SDK gives 128-bit values as
// decimal text, and the JSON has them as 32 lowercase hex characters.

namespace Iggy\Conformance;

use Iggy\MetadataChangeRecord;
use Iggy\MetadataChanges;
use Iggy\MetadataConsumerGroupCreated;
use Iggy\MetadataConsumerGroupDeleted;
use Iggy\MetadataConsumerGroupItem;
use Iggy\MetadataCurrentItem;
use Iggy\MetadataMemberItem;
use Iggy\MetadataMemberJoined;
use Iggy\MetadataMemberLeft;
use Iggy\MetadataPartitionHandedOver;
use Iggy\MetadataPartitionsChanged;
use Iggy\MetadataPartitionsItem;
use Iggy\MetadataSession;
use Iggy\MetadataSessionClosed;
use Iggy\MetadataStreamChange;
use Iggy\MetadataStreamDeleted;
use Iggy\MetadataTokenChange;
use Iggy\MetadataTokenDeleted;
use Iggy\MetadataTopicChange;
use Iggy\MetadataTopicDeleted;
use Iggy\MetadataTopicItem;
use Iggy\MetadataUnknownChange;
use Iggy\MetadataUnknownItem;
use Iggy\MetadataUserChange;
use Iggy\MetadataUserIdentity;
use Iggy\MetadataUserPermissionsUpdated;

function metadata_changes(MetadataChanges $changes): array
{
    return [
        'status' => $changes->status->value,
        'phase' => $changes->phase->value,
        'log_id' => $changes->log_id === null ? null : hex128($changes->log_id),
        'oldest_op' => u64($changes->oldest_op),
        'retained_since' => u64($changes->retained_since),
        'head_op' => u64($changes->head_op),
        'next' => $changes->next?->__toString(),
        'has_more' => $changes->has_more,
        'records' => array_map(change_record(...), $changes->records),
        'items' => array_map(current_item(...), $changes->items),
        'current_page_token' => $changes->current_page_token?->__toString(),
    ];
}

function change_record(MetadataChangeRecord $record): array
{
    $view = [
        'op' => u64($record->op),
        'ordinal' => $record->ordinal,
        'checksum' => hex128($record->checksum),
        'timestamp' => u64($record->timestamp),
    ];
    if ($record->actor_user_id !== null) {
        $view['actor_user_id'] = $record->actor_user_id;
    }
    $change = $record->change;
    if ($change instanceof MetadataUnknownChange) {
        return $view + ['group' => (string) $change->group, 'kind' => (string) $change->kind, 'body' => base64_encode($change->body)];
    }

    return $view + ['group' => $record->group->value, 'kind' => $record->kind->value] + body_view($change);
}

function current_item(MetadataCurrentItem $item): array
{
    $body = $item->body;
    if ($body instanceof MetadataUnknownItem) {
        return ['group' => (string) $body->group, 'body' => base64_encode($body->body)];
    }

    return ['group' => $item->group->value] + body_view($body);
}

function body_view(object $body): array
{
    return match (true) {
        $body instanceof MetadataStreamChange => stream_key($body)
            + ['created_at' => u64($body->created_at), 'name' => $body->name]
            + metadata_options($body),
        $body instanceof MetadataStreamDeleted => stream_key($body)
            + ['created_at' => u64($body->created_at), 'name' => $body->name]
            + optional('requested_op', $body->requested_op),
        $body instanceof MetadataTopicChange => topic_key($body)
            + ['created_at' => u64($body->created_at), 'name' => $body->name, 'partitions_count' => $body->partitions_count]
            + metadata_options($body),
        $body instanceof MetadataTopicItem => topic_key($body)
            + ['created_at' => u64($body->created_at), 'name' => $body->name]
            + metadata_options($body),
        $body instanceof MetadataTopicDeleted => topic_key($body)
            + ['created_at' => u64($body->created_at), 'name' => $body->name]
            + optional('requested_op', $body->requested_op),
        $body instanceof MetadataPartitionsChanged => topic_key($body)
            + ['partitions_count' => $body->partitions_count, 'removed_partition_ids' => $body->removed_partition_ids]
            + optional('requested_op', $body->requested_op),
        $body instanceof MetadataPartitionsItem => topic_key($body)
            + ['partitions_count' => $body->partitions_count],
        $body instanceof MetadataConsumerGroupCreated => topic_key($body)
            + ['group_id' => $body->group_id, 'name' => $body->name, 'consumer_groups_count' => $body->consumer_groups_count],
        $body instanceof MetadataConsumerGroupDeleted => topic_key($body)
            + ['group_id' => $body->group_id, 'name' => $body->name, 'consumer_groups_count' => $body->consumer_groups_count]
            + optional('requested_op', $body->requested_op),
        $body instanceof MetadataConsumerGroupItem => topic_key($body)
            + ['group_id' => $body->group_id, 'name' => $body->name],
        $body instanceof MetadataMemberJoined => topic_key($body) + [
            'group_id' => $body->group_id,
            'member_id' => $body->member_id,
            'client_id' => hex128($body->client_id),
            'members_count' => $body->members_count,
            'partition_ids' => $body->partition_ids,
        ],
        $body instanceof MetadataMemberLeft => topic_key($body) + [
            'group_id' => $body->group_id,
            'member_id' => $body->member_id,
            'client_id' => hex128($body->client_id),
            'members_count' => $body->members_count,
            'reason' => $body->reason->value,
        ],
        $body instanceof MetadataPartitionHandedOver => topic_key($body)
            + ['group_id' => $body->group_id, 'partition_id' => $body->partition_id]
            + optional('member_id', $body->member_id),
        $body instanceof MetadataMemberItem => topic_key($body) + [
            'group_id' => $body->group_id,
            'member_id' => $body->member_id,
            'client_id' => hex128($body->client_id),
            'partition_ids' => $body->partition_ids,
        ],
        $body instanceof MetadataUserChange => user_key($body) + [
            'username' => $body->username,
            'status' => $body->status,
            'permissions' => permissions_view($body->permissions),
        ] + metadata_options($body),
        $body instanceof MetadataUserIdentity => user_key($body) + ['username' => $body->username],
        $body instanceof MetadataUserPermissionsUpdated => user_key($body) + [
            'username' => $body->username,
            'permissions' => permissions_view($body->permissions),
            'reason' => $body->reason->value,
        ],
        $body instanceof MetadataTokenChange => user_key($body)
            + ['name' => $body->name]
            + optional('expiry', $body->expiry),
        $body instanceof MetadataTokenDeleted => user_key($body)
            + ['name' => $body->name, 'reason' => $body->reason->value],
        $body instanceof MetadataSession => user_key($body)
            + ['client_id' => hex128($body->client_id), 'session' => u64($body->session)],
        $body instanceof MetadataSessionClosed => user_key($body) + [
            'client_id' => hex128($body->client_id),
            'session' => u64($body->session),
            'reason' => $body->reason->value,
        ],
        default => throw new Failure('an unexpected metadata body ' . $body::class),
    };
}

function stream_key(object $body): array
{
    return ['stream_id' => $body->stream_id, 'stream_created_op' => u64($body->stream_created_op)];
}

function topic_key(object $body): array
{
    return stream_key($body) + ['topic_id' => $body->topic_id, 'topic_created_op' => u64($body->topic_created_op)];
}

function user_key(object $body): array
{
    return ['user_id' => $body->user_id, 'user_created_at' => u64($body->user_created_at)];
}

/** A field that the JSON leaves out when it has no value. */
function optional(string $name, int|string|null $value): array
{
    return $value === null ? [] : [$name => u64($value)];
}

function metadata_options(object $body): array
{
    return $body->options_unreadable
        ? ['options' => null, 'options_unreadable' => true]
        : ['options' => options($body->options, $body->derived_options)];
}

/** Permissions in the get_user JSON shape, whose stream and topic maps have id keys. */
function permissions_view(?array $permissions): ?array
{
    if ($permissions === null) {
        return null;
    }
    $streams = $permissions['streams'];

    return [
        'global' => $permissions['global'],
        'streams' => $streams === null ? null : (object) array_map(
            static fn (array $stream): array => ['topics' => $stream['topics'] === null ? null : (object) $stream['topics']] + $stream,
            $streams,
        ),
    ];
}

/** A u128 from its decimal text as 32 lowercase hex characters. */
function hex128(string $decimal): string
{
    return str_pad(gmp_strval(gmp_init($decimal, 10), 16), 32, '0', STR_PAD_LEFT);
}
