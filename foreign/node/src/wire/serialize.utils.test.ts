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


import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { toDate } from './serialize.utils.js';

describe('serialize utils', () => {

  describe('toDate', () => {

    it('turns the zero microsecond timestamp into the Unix epoch', () => {
      assert.equal(toDate(0n).getTime(), 0);
    });

    it('divides microseconds by a thousand to reach milliseconds', () => {
      assert.equal(
        toDate(1_700_000_000_000_000n).toISOString(),
        '2023-11-14T22:13:20.000Z'
      );
    });

  });

});
