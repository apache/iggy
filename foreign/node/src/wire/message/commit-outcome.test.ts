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

import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { CommitErrorScope, provesNothingStored } from './commit-outcome.js';

type Outcome = { status: number, error: string, certain: boolean };

const fixture = JSON.parse(readFileSync(new URL('../fixtures/deferred_polling.json', import.meta.url), 'utf8'));

describe('commit outcomes', () => {
  for (const scope of [CommitErrorScope.Request, CommitErrorScope.Section]) {
    it(`classifies every ${scope} status of the shared fixture as the Rust SDK does`, () => {
      const outcomes: Outcome[] = fixture.commit_outcomes[scope];
      assert.ok(outcomes.some(({ certain }) => certain) && outcomes.some(({ certain }) => !certain));
      const mismatched = outcomes.filter(({ status, certain }) => provesNothingStored(scope, status) !== certain)
        .map(({ status, error }) => `${status} ${error}`);
      assert.deepEqual(mismatched, []);
    });
  }
});
