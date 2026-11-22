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
import { registerHooks } from 'node:module';
import { before, test } from 'node:test';

const messages = Array.from({ length: 25 }, (_, offset) => ({ offset }));
const requests = [];
let load;

// Keep the real loader and replace its HTTP and presentation boundaries.
export async function clientApi({ path, queryParams }) {
  if (!path.endsWith('/messages')) return {};
  requests.push(queryParams);
  const offset = Number(queryParams.value);
  return {
    currentOffset: messages.length - 1,
    messages: messages.slice(offset, offset + Number(queryParams.count))
  };
}

export const partitionMessagesDetailsMapper = (value) => value;
export const topicDetailsMapper = (value) => value;

before(async () => {
  const boundaries = new Set([
    '#lib/api/clientApi.js',
    '#lib/domain/MessageDetails.js',
    '#lib/domain/TopicDetails.js'
  ]);
  const hooks = registerHooks({
    resolve(specifier, context, nextResolve) {
      if (boundaries.has(specifier)) {
        return { url: import.meta.url, shortCircuit: true };
      }
      return nextResolve(specifier, context);
    }
  });
  try {
    ({ load } = await import('./+page.ts'));
  } finally {
    hooks.deregister();
  }
});

async function loadMessages(query) {
  const result = await load({
    params: { streamId: '1', topicId: '1', partitionId: '1' },
    url: new URL(`http://localhost/messages?${query}`)
  });
  return result.partitionMessages.messages.map((message) => message.offset);
}

test('given_25_messages_when_loading_newest_first_should_return_latest_20', async () => {
  assert.deepEqual(
    await loadMessages('direction=desc'),
    messages.slice(5).map(({ offset }) => offset)
  );
  assert.equal(requests.at(-1).auto_commit, 'false');
});

test('given_25_messages_when_loading_oldest_first_should_return_first_20', async () => {
  assert.deepEqual(
    await loadMessages('direction=asc'),
    messages.slice(0, 20).map(({ offset }) => offset)
  );
});

test(
  'given_25_messages_when_loading_final_newest_first_page_should_not_repeat_offsets',
  { skip: !process.env.IGGY_RUN_KNOWN_FAILURES && 'Known bug: apache/iggy#4717' },
  async () => {
    const firstPage = await loadMessages('direction=desc');
    // The page component clamps page 2 to offset 0: max(0, 25 - 2 * 20).
    const finalPage = await loadMessages('direction=desc&offset=0');
    // TODO: Agree on how to carry the final page's upper bound before changing the loader.
    assert.deepEqual(finalPage, [0, 1, 2, 3, 4]);
    assert.deepEqual(
      finalPage.filter((offset) => firstPage.includes(offset)),
      []
    );
  }
);
