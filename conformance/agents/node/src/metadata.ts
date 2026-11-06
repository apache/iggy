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

// The canonical form of a metadata changes answer is the HTTP JSON that the
// fixture core/common/fixtures/metadata_changes.json names "expected". The
// views follow the fixture test of the SDK,
// foreign/node/src/wire/system/get-metadata-changes.command.test.ts, and keep
// 64-bit values exact where that test turns them into JS numbers.

import { globalView, optionsView } from './canonical.js';
import {
  UserStatus, type MetadataChangeRecord, type MetadataChanges, type MetadataCurrentItem,
  type MetadataOptions, type UserPermissions
} from './iggy.js';

/** The optional body fields that the HTTP form leaves out when they are null. */
const OMITTED_WHEN_NULL = new Set(['requestedOp', 'memberId', 'expiry']);

export const changesView = (changes: MetadataChanges) => ({
  status: changes.status,
  phase: changes.phase,
  log_id: changes.logId === null ? null : hex128(changes.logId),
  oldest_op: changes.oldestOp,
  retained_since: changes.retainedSince,
  head_op: changes.headOp,
  next: changes.next?.toString() ?? null,
  has_more: changes.hasMore,
  records: changes.records.map(recordView),
  items: changes.items.map(itemView),
  current_page_token: changes.currentPageToken?.toString() ?? null
});

const recordView = (record: MetadataChangeRecord) => {
  const envelope = {
    op: record.op,
    ordinal: record.ordinal,
    checksum: hex128(record.checksum),
    timestamp: record.timestamp,
    ...(record.actorUserId === null ? {} : { actor_user_id: record.actorUserId })
  };
  if (record.kind === 'unknown')
    return {
      ...envelope, group: String(record.groupCode), kind: String(record.kindCode),
      body: record.body.toString('base64')
    };
  return { ...envelope, group: record.group, kind: record.kind, ...bodyView(record.body) };
};

const itemView = (item: MetadataCurrentItem) => item.group === 'unknown'
  ? { group: String(item.groupCode), body: item.body.toString('base64') }
  : { group: item.group, ...bodyView(item.body) };

/** A body with snake_case keys, the options merged with their provenance and the client id as hex. */
const bodyView = (body: object): Record<string, unknown> => {
  const view: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(body)) {
    if (key === 'options')
      Object.assign(view, metadataOptionsView(value as MetadataOptions));
    else if (key === 'permissions')
      view.permissions = permissionsView(value as UserPermissions | null);
    else if (key === 'status')
      view.status = value === UserStatus.Active ? 'active' : 'inactive';
    else if (key === 'clientId')
      view.client_id = hex128(value as bigint);
    else if (!(value === null && OMITTED_WHEN_NULL.has(key)))
      view[key.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`)] = value;
  }
  return view;
};

const metadataOptionsView = (options: MetadataOptions) => options.unreadable
  ? { options: null, options_unreadable: true }
  : { options: optionsView(options.options, options.derivedOptions) };

/** Permissions in the get_user JSON form of the change log, with stream permissions. */
const permissionsView = (permissions: UserPermissions | null) => permissions === null ? null : {
  global: globalView(permissions.global),
  streams: permissions.streams.length === 0 ? null : Object.fromEntries(permissions.streams.map((stream) => [
    String(stream.streamId),
    {
      manage_stream: stream.permissions.manageStream,
      read_stream: stream.permissions.readStream,
      manage_topics: stream.permissions.manageTopics,
      read_topics: stream.permissions.readTopics,
      poll_messages: stream.permissions.pollMessages,
      send_messages: stream.permissions.sendMessages,
      topics: stream.topics.length === 0 ? null : Object.fromEntries(stream.topics.map((topic) => [
        String(topic.topicId),
        {
          manage_topic: topic.permissions.manage,
          read_topic: topic.permissions.read,
          poll_messages: topic.permissions.pollMessages,
          send_messages: topic.permissions.sendMessages
        }
      ]))
    }
  ]))
};

const hex128 = (value: bigint): string => value.toString(16).padStart(32, '0');
