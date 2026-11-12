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

/** Where a committing deferred poll (106) or fetch (109) reported an error. */
export const CommitErrorScope = {
  /** The whole reply is the error. */
  Request: 'request',
  /** One section of a fetch reply carries the error status. */
  Section: 'section'
} as const;

export type CommitErrorScope = typeof CommitErrorScope[keyof typeof CommitErrorScope];

const REQUEST_PROVES_NOTHING_STORED = new Set([5, 6, 58, 80, 87, 89, 90, 1009, 1010, 2010, 2011, 3007, 3024, 4100,
  5009]);
// A section whose read may have stored never carries one of these.
const SECTION_PROVES_NOTHING_STORED = new Set([58, 80, 87, 89, 3007, 3024, 5009]);

/**
 * Whether `status`, reported in `scope`, proves that the server stored no
 * offset. Any other status, a lost reply and a transport failure after the
 * send leave the outcome unknown.
 */
export const provesNothingStored = (scope: CommitErrorScope, status: number): boolean =>
  (scope === CommitErrorScope.Request ? REQUEST_PROVES_NOTHING_STORED : SECTION_PROVES_NOTHING_STORED).has(status);
