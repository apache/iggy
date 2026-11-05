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

import type { Id } from '../identifier.utils.js';
import type { OptionEntry, TopicOptionKey } from '../options.utils.js';
import { HeaderValue } from '../message/header.utils.js';
import { wrapCommand } from '../command.utils.js';
import { OptionsScope } from '../system/describe-options.command.js';
import { UPDATE_RESOURCE, columnOption } from '../system/update-resource.command.js';


/**
 * Parameters for the update topic command. Keys left out keep their values.
 * Settings fixed at create, compression among them, have no field here.
 */
export type UpdateTopic = {
  /** Stream identifier (ID or name) */
  streamId: Id,
  /** Topic identifier (ID or name) */
  topicId: Id,
  /** New topic name (1-255 bytes) */
  name?: string,
  /**
   * Message expiry time in microseconds. `0n` is not "unlimited": it omits the
   * key, so the topic keeps whatever expiry it currently has. Reset the key to
   * return to the server default.
   */
  messageExpiry?: bigint,
  /**
   * Maximum topic size in bytes. `0n` is not "unlimited": it omits the key, so
   * the topic keeps whatever cap it currently has. Reset the key to return to
   * the server default.
   */
  maxTopicSize?: bigint,
  /** Keys to reset to their defaults: `message_expiry` or `max_topic_size` */
  reset?: TopicOptionKey[],
  /**
   * Reset names with no typed member, for a resettable key the server catalog
   * gained after this build shipped. The server decides on them.
   */
  rawReset?: readonly string[],
  /**
   * Option keys with no field of their own, for an updatable key the server
   * catalog gained after this build shipped. A field above wins on collision.
   * The server decides on them, so a create-only or unknown key comes back as
   * a server error.
   */
  options?: OptionEntry[]
};

/**
 * Update topic command definition.
 * Changes only the keys it names, through the generic update command.
 */
export const UPDATE_TOPIC = {
  code: UPDATE_RESOURCE.code,

  serialize: ({
    streamId,
    topicId,
    name,
    messageExpiry = 0n,
    maxTopicSize = 0n,
    reset,
    rawReset,
    options: rawOptions
  }: UpdateTopic) => {
    const options: OptionEntry[] = [];
    if (name !== undefined)
      options.push(columnOption('name', name));
    if (messageExpiry !== 0n)
      options.push({
        key: 'message_expiry',
        value: HeaderValue.Uint64(messageExpiry)
      });
    if (maxTopicSize !== 0n)
      options.push({
        key: 'max_topic_size',
        value: HeaderValue.Uint64(maxTopicSize)
      });

    return UPDATE_RESOURCE.serialize({
      target: { scope: OptionsScope.Topic, streamId, topicId }, options, reset, rawOptions, rawReset
    });
  },

  deserialize: UPDATE_RESOURCE.deserialize
};


/**
 * Executable update topic command function.
 */
export const updateTopic = wrapCommand<UpdateTopic, boolean>(UPDATE_TOPIC);
