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

import type { Stream } from '#lib/domain/Stream.js';
import type { Topic } from '#lib/domain/Topic.js';
import type { User } from '#lib/domain/User.js';

export const UINT64_MAX = 18_446_744_073_709_551_615n;
export const UNLIMITED = 'unlimited';
export const INVALID_STREAM_KEY_MESSAGE =
  'Encryption key must be standard padded base64 of exactly 32 bytes';

const STREAM_KEY_BYTES = 32;
const STANDARD_BASE64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/;

export type OptionEntry = { value: string; kind: string; explicit: boolean };
export type ResourceOptions = Record<string, OptionEntry>;
export type OptionInputValue = string | number | boolean;
export type CompressionAlgorithm = 'none' | 'gzip';
export type TopicResettableKey = 'message_expiry' | 'max_topic_size';

export type TopicCreateBody = {
  name: string;
  partitions_count: number;
  options: {
    compression_algorithm?: CompressionAlgorithm;
    message_expiry?: string;
    max_topic_size?: string;
  };
};

export type TopicUpdateBody = {
  name?: string;
  message_expiry?: string;
  max_topic_size?: string;
  reset?: TopicResettableKey[];
};

export type StreamCreateBody = { name: string; options: { encryption_key?: string } };
export type StreamUpdateBody = { name?: string };
export type UserUpdateBody = { username?: string; status?: User['status'] };

type Change<T> = { current: T; next: T };
type TopicUpdatable = Pick<Topic, 'name' | 'messageExpiry' | 'maxTopicSize'>;

export type TopicCreateInput = TopicUpdatable & {
  partitionsCount: number;
  compressionAlgorithm: CompressionAlgorithm;
};

export function optionText(options: ResourceOptions | undefined, key: string): string | undefined {
  return options && Object.prototype.hasOwnProperty.call(options, key)
    ? options[key].value
    : undefined;
}

export function optionUint64(
  options: ResourceOptions | undefined,
  key: string
): bigint | undefined {
  const text = optionText(options, key);
  return text !== undefined && /^\d+$/.test(text) ? BigInt(text) : undefined;
}

export function isOptionExplicit(options: ResourceOptions | undefined, key: string): boolean {
  return (
    options !== undefined &&
    Object.prototype.hasOwnProperty.call(options, key) &&
    options[key].explicit === true
  );
}

export function isStreamEncryptionKey(text: string): boolean {
  if (!STANDARD_BASE64.test(text)) return false;
  const bytes = atob(text);
  // Re-encoding refuses nonzero trailing bits, which the server also refuses.
  return bytes.length === STREAM_KEY_BYTES && btoa(bytes) === text;
}

export function streamCreate(name: string, encryptionKey = ''): StreamCreateBody {
  if (encryptionKey === '') return { name, options: {} };
  if (!isStreamEncryptionKey(encryptionKey)) throw new TypeError(INVALID_STREAM_KEY_MESSAGE);
  return { name, options: { encryption_key: encryptionKey } };
}

// An omitted key resolves to the catalog default and stays reported as a default.
export function topicCreate(input: TopicCreateInput): TopicCreateBody {
  const { name, partitionsCount, compressionAlgorithm, messageExpiry, maxTopicSize } = input;
  const options: TopicCreateBody['options'] = {};
  const size = maxTopicSize.trim();
  if (compressionAlgorithm !== 'none') options.compression_algorithm = compressionAlgorithm;
  if (messageExpiry > 0) options.message_expiry = messageExpiryText(messageExpiry);
  if (size !== '') options.max_topic_size = size;
  return { name, partitions_count: partitionsCount, options };
}

export function topicUpdate({
  current,
  next,
  reset = []
}: Change<TopicUpdatable> & { reset?: TopicResettableKey[] }): TopicUpdateBody {
  const body: TopicUpdateBody = {};
  const maxTopicSize = next.maxTopicSize.trim();
  if (next.name !== current.name) body.name = next.name;
  if (!reset.includes('message_expiry') && next.messageExpiry !== current.messageExpiry) {
    body.message_expiry = messageExpiryText(next.messageExpiry);
  }
  if (!reset.includes('max_topic_size') && maxTopicSize !== current.maxTopicSize) {
    body.max_topic_size = maxTopicSize;
  }
  if (reset.length > 0) body.reset = [...new Set(reset)];
  return body;
}

export function streamUpdate({ current, next }: Change<Pick<Stream, 'name'>>): StreamUpdateBody {
  return next.name !== current.name ? { name: next.name } : {};
}

export function userUpdate({
  current,
  next
}: Change<Pick<User, 'username' | 'status'>>): UserUpdateBody {
  const body: UserUpdateBody = {};
  if (next.username !== current.username) body.username = next.username;
  if (next.status !== current.status) body.status = next.status;
  return body;
}

// Zero means never in the form, but the server reads zero as its default.
function messageExpiryText(micros: number): string {
  return micros > 0 ? String(micros) : UNLIMITED;
}
