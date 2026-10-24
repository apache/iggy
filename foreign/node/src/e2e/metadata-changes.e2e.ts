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

// The keyed stream case needs a server key, so resource-options.e2e.ts and
// tls.system.e2e.ts register it, and the shipped-default lane skips both.

import { Client } from '../client/client.js';
import { getIggyAddress } from '../tcp.sm.utils.js';
import { describeMetadataChanges } from './metadata-changes.suite.js';

const [host, port] = getIggyAddress();

describeMetadataChanges('e2e -> metadata changes', 'e2e-mc', (credentials) => new Client({
  transport: 'TCP', options: { host, port }, credentials
}));
