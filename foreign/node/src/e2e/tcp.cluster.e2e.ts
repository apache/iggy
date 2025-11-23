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

import { after, describe, it } from "node:test";
import assert from "node:assert/strict";
import { getTestClient } from "./test-client.utils.js";

// cluster mode still in dev atm
// response is mocked from /core/configs/server.toml
const expectedMeta = {
  name: "iggy-cluster",
  id: 0,
  transport: "TCP",
  nodes: [
    {
      id: 0,
      name: "iggy-node-1",
      address: "127.0.0.1:8090",
      role: "Leader",
      status: "Healthy",
    },
    {
      id: 1,
      name: "iggy-node-2",
      address: "127.0.0.1:8091",
      role: "Follower",
      status: "Healthy",
    },
  ],
};

describe("e2e -> system", async () => {
  const c = getTestClient();

  it("e2e -> cluster::getClusterMetadata", async () => {
    const meta = await c.cluster.getClusterMetadata();
    assert.deepEqual(meta, expectedMeta);
  });

  after(() => {
    c.destroy();
  });
});
