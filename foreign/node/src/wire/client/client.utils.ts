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


export type Client = {
  clientId: number,
  userId: number,
  transport: string,
  address: string,
  consumerGroupCount: number
};

type ClientDeserialized = {
  bytesRead: number,
  data: Client
}

const transportString = (t: number): string => {
  switch (t.toString()) {
    case '1': return 'tcp';
    case '2': return 'quic';
    default: return `unknown_transport_${t}`;
    // default: throw new Error(`unknown_transport_${t}`);
  }
}

export const deserializeClient = (r: Buffer, pos = 0): ClientDeserialized => {
  /**
   *  0 - 4   u32 - client_id
   *  4 - 8   u32 - user_id
   *  8 - 9   u8  - transport
   *  9 - 13  u32 - adress length x
   * 13 - x   string - adress
   *  x - x+4 u32 - consumerGroupCount
   */
  if(r.length < 17)
    throw new Error('Client does not exist');

  const addressLength = r.readUInt32LE(pos + 9);

  return {
    bytesRead: 13 + addressLength + 4,
    data: {
      clientId: r.readUInt32LE(pos),
      userId: r.readUInt32LE(pos + 4),
      transport: transportString(r.readUInt8(pos + 8)), // { 1: tcp, 2: quic }
      address: r.subarray(pos + 13, pos + 13 + addressLength).toString(),
      consumerGroupCount: r.readUInt32LE(pos + 13 + addressLength)
    }
  }
};
