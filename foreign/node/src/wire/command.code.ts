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


import { reverseRecord } from '../type.utils.js';

export const COMMAND_CODE = {
  Ping: 1,
  GetStats: 10,
  GetSnapshot: 11,                    // @TODO GET_SNAPSHOT_FILE_CODE: u32 = 11
  GetMe: 20,
  GetClient: 21,
  GetClients: 22,
  GetUser: 31,
  GetUsers: 32,
  CreateUser: 33,
  DeleteUser: 34,
  UpdateUser: 35,
  UpdatePermissions: 36,
  ChangePassword: 37,
  LoginUser: 38,
  LogoutUser: 39,
  GetAccessTokens: 41,
  CreateAccessToken: 42,
  DeleteAccessToken: 43,
  LoginWithAccessToken: 44,
  PollMessages: 100,
  SendMessages: 101,
  FlushUnsavedBuffers: 102,
  GetOffset: 120,
  StoreOffset: 121,
  DeleteConsumerOffset: 122,
  GetStream: 200,
  GetStreams: 201,
  CreateStream: 202,
  DeleteStream: 203,
  UpdateStream: 204,
  PurgeStream: 205,
  GetTopic: 300,
  GetTopics: 301,
  CreateTopic: 302,
  DeleteTopic: 303,
  UpdateTopic: 304,
  PurgeTopic: 305,
  CreatePartitions: 402,
  DeletePartitions: 403,
  DeleteSegments: 503,
  GetGroup: 600,
  GetGroups: 601,
  CreateGroup: 602,
  DeleteGroup: 603,
  JoinGroup: 604,
  LeaveGroup: 605,
};

const reverseCommandCodeMap = reverseRecord(COMMAND_CODE);

export const translateCommandCode = (code: number): string => {
  return reverseCommandCodeMap[code] || `unknow_command_code_${code}`
};
