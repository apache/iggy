#!/usr/bin/env node
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

// Check that a PR description has a linked issue (or N/A), a description of
// the change, and test notes, the three things PULL_REQUEST_TEMPLATE.md asks
// for. It looks for the content anywhere in the body, so a free-form
// description passes when it covers all three. Plain pattern checks, no
// model. Used by .github/workflows/pr-description.yml and runnable by hand.
// Read-only: it never changes anything on GitHub.
//
// Exits 1 and names each missing part when the description has gaps. In
// Actions it also writes them to the job summary.
//
// Usage:
//   scripts/ci/pr-description-check.mjs --pr 1234
//   scripts/ci/pr-description-check.mjs --pr 1234 --repo owner/name
//
// Configuration (environment):
//   GITHUB_TOKEN   token to read the PR. Falls back to `gh auth token`.
//                  Reading a public PR needs none.

import { appendFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { parseArgs } from 'node:util';

const USAGE = 'Usage: pr-description-check.mjs --pr <number> [--repo owner/name]';

function problems(rawBody) {
  // Drop template comments and the template lines left unedited.
  const body = (rawBody || '')
    .replace(/\r\n/g, '\n')
    .replace(/<!--[\s\S]*?-->/g, '')
    .split('\n')
    .filter(l => !/^\s*(Closes|Relates to)\s+#\s*$/i.test(l))
    .filter(l => !/^\s*-?\s*(Passed \/ not passed|Pre-commit hooks ran \/ not ran)\s*$/i.test(l))
    .join('\n');
  const issueSection = (body.match(
    /(?:^|\n)##[ \t]+Which issue does this PR address\?[ \t]*\n([\s\S]*?)(?=\n##[ \t]|$)/,
  ) || [])[1] || '';
  const prose = body.replace(/^\s*#+.*$/gm, '').replace(/\s+/g, ' ').trim();

  const found = [];
  // An explicit "N/A" or "None" under the issue heading is an answer.
  if (!/#\d+|github\.com\/apache\/iggy\/issues\/\d+/.test(body)
      && !/^\s*(N\/A|None)\b/im.test(issueSection)) {
    found.push('Link the issue this PR addresses, for example `Closes #123` or `Relates to #123`, or write N/A under "Which issue does this PR address?".');
  }
  if (prose.length < 100) {
    found.push('Describe why the change is needed and what it changes.');
  }
  if (!/\b(tested|tests? pass|passed|passing|ran|run locally|pre-commit|prek|cargo (test|nextest|clippy)|go test|pytest|npm test|dotnet test|mvn|gradlew?|verified|verification|nothing to run)\b/i.test(body)) {
    found.push('Say how you tested the change locally, under "Local Execution": passed or not passed, and whether pre-commit hooks ran.');
  }
  return found;
}

function githubToken(env) {
  if (env.GITHUB_TOKEN) return env.GITHUB_TOKEN;
  try {
    return execFileSync('gh', ['auth', 'token'], { encoding: 'utf8' }).trim();
  } catch {
    return '';
  }
}

async function main() {
  const { values } = parseArgs({
    options: {
      pr: { type: 'string' },
      repo: { type: 'string', default: process.env.GITHUB_REPOSITORY || 'apache/iggy' },
      help: { type: 'boolean', short: 'h', default: false },
    },
  });
  if (values.help) {
    console.log(USAGE);
    return;
  }
  if (!/^\d+$/.test(values.pr || '')) {
    console.error(USAGE);
    process.exit(2);
  }

  const token = githubToken(process.env);
  const path = `/repos/${values.repo}/pulls/${values.pr}`;
  const res = await fetch(`https://api.github.com${path}`, {
    headers: {
      'Accept': 'application/vnd.github+json',
      'X-GitHub-Api-Version': '2022-11-28',
      ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
    },
  });
  if (!res.ok) {
    throw new Error(`GitHub GET ${path}: HTTP ${res.status} ${await res.text()}`);
  }
  const pr = await res.json();

  const found = problems(pr.body);
  if (found.length === 0) {
    console.log('PR description covers the issue, the change and testing');
    return;
  }
  console.log('PR description is missing parts of the template:');
  for (const p of found) console.log(`- ${p}`);
  if (process.env.GITHUB_STEP_SUMMARY) {
    appendFileSync(
      process.env.GITHUB_STEP_SUMMARY,
      '### PR description is missing parts of the template\n\n' +
        found.map(p => `- ${p}`).join('\n') +
        '\n\nEdit the PR description to fix these. The check runs again on each edit.\n',
    );
  }
  process.exit(1);
}

main().catch(e => {
  console.error(e.message);
  process.exit(1);
});
