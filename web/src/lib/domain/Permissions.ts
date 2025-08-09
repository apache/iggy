/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


export type GlobalPermissions = {
  manageServers: boolean;
  readServers: boolean;
  manageUsers: boolean;
  readUsers: boolean;
  manageStreams: boolean;
  readStreams: boolean;
  manageTopics: boolean;
  readTopics: boolean;
  pollMessages: boolean;
  sendMessages: boolean;
};

export type StreamPermissions = {
  manageStream: boolean;
  readStream: boolean;
  manageTopics: boolean;
  readTopics: boolean;
  pollMessages: boolean;
  sendMessages: boolean;
  topics: Record<number, TopicPermissions>;
};

export type TopicPermissions = {
  manageTopic: boolean;
  readTopic: boolean;
  pollMessages: boolean;
  sendMessages: boolean;
};

export type Permissions = {
  global: GlobalPermissions;
  streams: Record<number, StreamPermissions> | null;
};

export const permissionsMapper = (item: any): Permissions => {
  const global = item.global;
  const streams = item.streams;
  return {
    global: {
      manageServers: global.manage_servers,
      readServers: global.read_servers,
      manageUsers: global.manage_users,
      readUsers: global.read_users,
      manageStreams: global.manage_streams,
      readStreams: global.read_streams,
      manageTopics: global.manage_topics,
      readTopics: global.read_topics,
      pollMessages: global.poll_messages,
      sendMessages: global.send_messages
    },
    streams: streamsPermissionsMapper(streams)
  };
};

const streamsPermissionsMapper = (streams: any): Record<number, StreamPermissions> | null => {
  if (!streams) return null;

  const streamsObject = {};

  Object.keys(streams).forEach((streamId) => {
    const s = streams[streamId];

    streamsObject[streamId] = {
      manageStream: s.manage_stream,
      readStream: s.read_stream,
      manageTopics: s.manage_topics,
      readTopics: s.read_topics,
      pollMessages: s.poll_messages,
      sendMessages: s.send_messages
    };
  });

  return streamsObject as Record<string, StreamPermissions>;
};
