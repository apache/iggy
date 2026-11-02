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

import { after, describe, it } from "node:test";
import assert from "node:assert/strict";
import { getIggyAddress } from "../tcp.sm.utils.js";
import { getTestClient } from "./test-client.utils.js";

// The suite runs against one node or a cluster on any ports, so it checks
// the shape of the roster and the node that the client dialed.
describe("e2e -> system", async () => {
  const c = getTestClient();

  it("e2e -> cluster::getClusterMetadata", async () => {
    const meta = await c.cluster.getClusterMetadata();
    const [host, port] = getIggyAddress();

    assert.deepEqual(meta.nodes.map(({ replicaId }) => replicaId), meta.nodes.map((_, index) => index));
    assert.deepEqual(meta.nodes.filter(({ role }) => role === "Leader").length, 1);
    assert.ok(meta.nodes.every(({ status }) => status === "Healthy"), JSON.stringify(meta.nodes));
    assert.ok(meta.nodes.some(({ ip, endpoints }) => ip === host && endpoints.tcp === port),
      `${host}:${port} is not in ${JSON.stringify(meta.nodes)}`);
  });

  after(() => {
    c.destroy();
  });
});
