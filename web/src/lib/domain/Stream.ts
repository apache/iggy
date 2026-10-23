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

import { optionText } from '#lib/api/resourceOptions.js';

export type Stream = {
  id: number;
  name: string;
  sizeBytes: number;
  sizeFormatted: string;
  messagesCount: number;
  topicsCount: number;
  createdAt: number;
  encryptionKeySource: string | undefined;
  encryptionKeyFingerprint: string | undefined;
  encryptionKekFingerprints: string[];
};

export function streamMapper(item: any): Stream {
  const kekFingerprints = optionText(item.options, 'encryption_kek_fingerprint');
  return {
    id: item.id,
    name: item.name,
    sizeBytes: item.size,
    sizeFormatted: item.size,
    messagesCount: item.messages_count,
    topicsCount: item.topics_count,
    createdAt: item.created_at,
    encryptionKeySource: optionText(item.options, 'encryption_key_source'),
    encryptionKeyFingerprint: optionText(item.options, 'encryption_key_fingerprint'),
    encryptionKekFingerprints: kekFingerprints ? kekFingerprints.split(',') : []
  };
}

export function streamListMapper(data: any): Stream[] {
  const streams = data.map(streamMapper) as Stream[];
  const sortedStream = streams.sort((a, b) => b.createdAt - a.createdAt);

  return sortedStream;
}
