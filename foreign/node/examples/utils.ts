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


import { Client } from '../src/index.js';
import { getIggyAddress } from '../src/tcp.sm.utils.js';


export const getClient = () => {
  const [host, port] = getIggyAddress();
  const credentials = { username: 'iggy', password: 'iggy' };

  const opt = {
    transport: 'TCP' as const,
    options: { host, port },
    credentials
  };

  return new Client(opt);
}

export const ensureStream = async (cli: Client, streamId: number) => {
  try {
    return !!await cli.stream.get({ streamId });
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
  } catch (err) {
    return cli.stream.create({ streamId, name: `ensure-stream-${streamId}` })
  }
}

export const ensureTopic = async (
  cli: Client,
  streamId: number,
  topicId: number,
  partitionCount = 1,
  compressionAlgorithm = 1
) => {
  try {
    return !!await cli.topic.get({ streamId, topicId });
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
  } catch (err) {
    return cli.topic.create({
      streamId,
      topicId,
      name: `ensure-topic-${streamId}-${topicId}`,
      partitionCount,
      compressionAlgorithm
    });
  }
}

