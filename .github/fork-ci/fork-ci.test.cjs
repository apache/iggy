/*
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

// Run with: node --test .github/fork-ci/fork-ci.test.cjs
// The GitHub API is faked. Nothing here reaches the network.

'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');

const forkCi = require('./fork-ci.cjs');

const REPOSITORY = 'apache/iggy';
const FORK = 'contributor/iggy';
const OTHER_FORK = 'colleague/iggy';
const SHA = 'a'.repeat(40);
const BASE = 'b'.repeat(40);
const RUN_URL = `https://github.com/${FORK}/actions/runs/7`;
const OTHER_RUN_URL = `https://github.com/${OTHER_FORK}/actions/runs/7`;
const ACTIONS_URL = `https://github.com/${FORK}/actions`;
const PULL_URL = `https://github.com/${REPOSITORY}/pull/1`;
// A whole number of sweep intervals, so the rotating slice starts at 0.
const NOW = 2_000_000 * forkCi.SWEEP_INTERVAL_MS * 4;
const LONG_AGO = new Date(NOW - 24 * 60 * 60 * 1000).toISOString();

const CHANGES_CI = 'This pull request changes CI files. A maintainer must add ci:upstream';
const NO_WORKFLOW = 'The branch has no Fork CI workflow. Update it from master';
const NO_RUN = 'No Fork CI run for this commit. Push a new commit to start one';
const ACTIONS_OFF = 'Enable GitHub Actions in your fork, then push a new commit';
const NOT_HOSTED = 'A Fork CI job did not use a GitHub-hosted runner. A maintainer must add ci:upstream';
const REVOKED = 'The Fork CI result for this commit no longer holds';

const labelled = (...names) => names.map((name) => ({ name }));

function pullRequest({ number = 1, head = FORK, labels = [forkCi.LABEL_FORK], sha = SHA, updated_at = new Date(NOW).toISOString() } = {}) {
  const [owner, name] = (head ?? '/').split('/');
  return {
    number,
    updated_at,
    html_url: `https://github.com/${REPOSITORY}/pull/${number}`,
    labels: labelled(...labels),
    head: {
      sha,
      ref: 'feature',
      repo: head && { full_name: head, name, html_url: `https://github.com/${head}`, owner: { login: owner } },
    },
  };
}

const run = (overrides = {}) => ({
  id: 7, head_sha: SHA, event: 'push', status: 'completed', conclusion: 'success', html_url: RUN_URL, ...overrides,
});
const hostedJob = { conclusion: 'success', runner_group_id: 0, runner_group_name: 'GitHub Actions' };
const skippedJob = { conclusion: 'skipped', runner_group_id: null, runner_group_name: null };
const status = (context, state, description, target_url = RUN_URL) => ({ context, state, description, target_url });
const accepted = (target_url = RUN_URL) => [
  status(forkCi.REQUIRED_CONTEXT, 'success', 'Fork CI passed', target_url),
  status(forkCi.STATUS_CONTEXT, 'success', 'Fork CI passed', target_url),
];
const tree = (directories = { '.github': 'g', '.config': 'c' }) => ({
  tree: Object.entries({ ...directories, core: 'x' }).map(([path, sha]) => ({ path, sha })),
});
const notFound = () => { throw Object.assign(new Error('Not Found'), { status: 404 }); };

// Each field is the answer of one endpoint. A function receives the request parameters.
function fakeGithub(answers = {}) {
  const world = {
    pulls: [pullRequest()],
    live: pullRequest(),
    statuses: [],
    runs: [run()],
    workflow: { state: 'active' },
    workflowFile: { path: '.github/workflows/fork-ci.yml' },
    jobs: [hostedJob, skippedJob],
    comparison: { merge_base_commit: { sha: BASE } },
    headTree: tree(),
    checks: [],
    ...answers,
  };
  const calls = [];
  const posted = [];
  const endpoint = (name, key, wrap = (data) => data) => async (params) => {
    calls.push({ name, ...params });
    const answer = world[key];
    return { data: wrap(typeof answer === 'function' ? answer(params) : answer, params) };
  };
  const github = {
    rest: {
      pulls: { list: endpoint('pulls.list', 'pulls'), get: endpoint('pulls.get', 'live') },
      repos: {
        getCombinedStatusForRef: endpoint('getCombinedStatusForRef', 'statuses', (statuses) => ({ statuses })),
        createCommitStatus: async (params) => {
          if (world.rejectStatus?.(params)) throw new Error('status rejected');
          posted.push(params);
        },
        compareCommitsWithBasehead: endpoint('compare', 'comparison'),
        getContent: endpoint('getContent', 'workflowFile'),
      },
      git: { getTree: endpoint('getTree', 'headTree', (head, params) => (params.tree_sha === BASE ? tree() : head)) },
      checks: { listForRef: endpoint('checks.listForRef', 'checks', (check_runs) => ({ check_runs })) },
      actions: {
        listWorkflowRuns: endpoint('listWorkflowRuns', 'runs', (workflow_runs) => ({ workflow_runs })),
        getWorkflow: endpoint('getWorkflow', 'workflow'),
        listJobsForWorkflowRun: endpoint('listJobsForWorkflowRun', 'jobs'),
      },
    },
    paginate: async (method, params) => (await method(params)).data,
  };
  return { github, calls, posted, called: (name) => calls.filter((call) => call.name === name) };
}

function fakeCore() {
  const core = { outputs: {}, warnings: [], failure: null, info() {} };
  core.setOutput = (name, value) => { core.outputs[name] = value; };
  core.warning = (message) => { core.warnings.push(message); };
  core.setFailed = (message) => { core.failure = message; };
  return core;
}

const context = (eventName, payload = {}) => ({ eventName, payload, repo: { owner: 'apache', repo: 'iggy' } });

async function sync(answers, eventName = 'schedule', payload = {}) {
  const fake = fakeGithub(answers);
  const core = fakeCore();
  await forkCi.sync({ github: fake.github, context: context(eventName, payload), core, now: NOW });
  return {
    ...fake,
    core,
    contexts: fake.posted.map((item) => `${item.context}=${item.state}`),
    reported: fake.posted.map(({ context: name, state, description, target_url }) => ({ context: name, state, description, target_url })),
  };
}

describe('usesForkCi', () => {
  const cases = [
    // [title, head repository, labels, while the switch is off, once it is on]
    ['a branch of the upstream repository', REPOSITORY, [], false, false],
    ['an upstream branch labelled for fork CI', REPOSITORY, [forkCi.LABEL_FORK], false, false],
    ['an upstream branch under another letter case', 'Apache/Iggy', [forkCi.LABEL_FORK], false, false],
    ['a fork', FORK, [], false, true],
    ['a fork in the pilot', FORK, [forkCi.LABEL_FORK], true, true],
    ['a fork sent upstream by a maintainer', FORK, [forkCi.LABEL_UPSTREAM], false, false],
    ['a fork with both labels', FORK, [forkCi.LABEL_FORK, forkCi.LABEL_UPSTREAM], false, false],
    ['a deleted fork', null, [forkCi.LABEL_FORK], false, false],
  ];
  for (const [title, head, labels, whenOff, whenOn] of cases) {
    it(title, () => {
      const subject = pullRequest({ head, labels });
      assert.equal(forkCi.usesForkCi(subject, REPOSITORY, false), whenOff);
      assert.equal(forkCi.usesForkCi(subject, REPOSITORY, true), whenOn);
    });
  }

  it('ships switched off', () => {
    assert.equal(forkCi.ENABLED, false);
  });
});

describe('gate', () => {
  async function gate(eventName, { payloadLabels = [], live = pullRequest({ labels: [] }), head = FORK } = {}) {
    const fake = fakeGithub({ live });
    const core = fakeCore();
    const payload = { pull_request: pullRequest({ head, labels: payloadLabels }) };
    await forkCi.gate({ github: fake.github, context: context(eventName, payload), core });
    return { ...fake, core };
  }

  it('runs the pipeline for a push without asking the API', async () => {
    const { core, calls } = await gate('push');
    assert.deepEqual(core.outputs, { run_ci: 'true' });
    assert.deepEqual(calls, []);
  });

  it('runs the pipeline upstream for an unlabelled fork while the switch is off', async () => {
    assert.deepEqual((await gate('pull_request')).core.outputs, { run_ci: 'true' });
  });

  it('leaves a labelled fork pull request to the fork', async () => {
    const { core } = await gate('pull_request', { live: pullRequest() });
    assert.deepEqual(core.outputs, { run_ci: 'false' });
  });

  it('trusts the live labels over the labels of a replayed event', async () => {
    const released = await gate('pull_request', {
      payloadLabels: [forkCi.LABEL_FORK], live: pullRequest({ labels: [forkCi.LABEL_FORK, forkCi.LABEL_UPSTREAM] }),
    });
    assert.deepEqual(released.core.outputs, { run_ci: 'true' });
  });

  it('runs the pipeline for an upstream branch without asking the API', async () => {
    const { core, calls } = await gate('pull_request', { head: REPOSITORY, live: pullRequest() });
    assert.deepEqual(core.outputs, { run_ci: 'true' });
    assert.deepEqual(calls, []);
  });

  it('runs the pipeline upstream when the pull request cannot be read', async () => {
    const { core } = await gate('pull_request', { live: () => { throw new Error('rate limited'); } });
    assert.deepEqual(core.outputs, { run_ci: 'true' });
    assert.match(core.warnings[0], /rate limited/);
  });
});

describe('sync of one commit', () => {
  it('reports a trusted green run as the required check, then as the fork status', async () => {
    const { posted } = await sync();
    assert.deepEqual(posted, [
      { owner: 'apache', repo: 'iggy', sha: SHA, context: forkCi.REQUIRED_CONTEXT, state: 'success', description: 'Fork CI passed', target_url: RUN_URL },
      { owner: 'apache', repo: 'iggy', sha: SHA, context: forkCi.STATUS_CONTEXT, state: 'success', description: 'Fork CI passed', target_url: RUN_URL },
    ]);
  });

  it('asks the fork only for push runs of the fork workflow at the head commit', async () => {
    const { called } = await sync();
    assert.deepEqual(called('listWorkflowRuns'), [{
      name: 'listWorkflowRuns', owner: 'contributor', repo: 'iggy',
      workflow_id: forkCi.FORK_WORKFLOW, head_sha: SHA, event: 'push', per_page: 100,
    }]);
  });

  it('compares the CI directories of the head commit with those of its merge base', async () => {
    const { called } = await sync();
    assert.equal(called('compare')[0].basehead, `master...${SHA}`);
    assert.deepEqual(new Set(called('getTree').map((call) => call.tree_sha)), new Set([BASE, SHA]));
  });

  const refused = [
    ['a queued run', { runs: [run({ status: 'queued', conclusion: null })] }, 'pending', 'Fork CI is queued', RUN_URL],
    ['a running run', { runs: [run({ status: 'in_progress', conclusion: null })] }, 'pending', 'Fork CI is running', RUN_URL],
    ['a failed run', { runs: [run({ conclusion: 'failure' })] }, 'failure', 'Fork CI finished with: failure', RUN_URL],
    ['a cancelled run', { runs: [run({ conclusion: 'cancelled' })] }, 'failure', 'Fork CI finished with: cancelled', RUN_URL],
    ['no run yet', { runs: [] }, 'pending', NO_RUN, ACTIONS_URL],
    ['a run of another commit only', { runs: [run({ head_sha: 'c'.repeat(40) })] }, 'pending', NO_RUN, ACTIONS_URL],
    ['a pull request run inside the fork only', { runs: [run({ event: 'pull_request' })] }, 'pending', NO_RUN, ACTIONS_URL],
    ['a disabled workflow', { runs: [], workflow: { state: 'disabled_fork' } }, 'error', ACTIONS_OFF, ACTIONS_URL],
    ['a fork that never enabled the workflow', { runs: notFound, workflow: notFound }, 'error', ACTIONS_OFF, ACTIONS_URL],
    ['a branch without the workflow file', { runs: [], workflowFile: notFound }, 'error', NO_WORKFLOW, ACTIONS_URL],
    ['a job on a self-hosted runner', { jobs: [hostedJob, { conclusion: 'success', runner_group_id: 1, runner_group_name: 'Default' }] }, 'error', NOT_HOSTED, RUN_URL],
    ['a self-hosted group named like the hosted one', { jobs: [{ conclusion: 'success', runner_group_id: 4, runner_group_name: 'GitHub Actions' }] }, 'error', NOT_HOSTED, RUN_URL],
    ['a group that has only the id of the hosted one', { jobs: [{ conclusion: 'success', runner_group_id: 0, runner_group_name: 'Default' }] }, 'error', NOT_HOSTED, RUN_URL],
    ['a change under .github/', { headTree: tree({ '.github': 'changed', '.config': 'c' }) }, 'pending', CHANGES_CI, `${PULL_URL}/files`],
    ['a change under .config/', { headTree: tree({ '.github': 'g', '.config': 'changed' }) }, 'pending', CHANGES_CI, `${PULL_URL}/files`],
    ['a removed .config/', { headTree: tree({ '.github': 'g' }) }, 'pending', CHANGES_CI, `${PULL_URL}/files`],
  ];
  for (const [title, answers, state, description, target_url] of refused) {
    it(`does not report the required check for ${title}`, async () => {
      const { reported } = await sync(answers);
      assert.deepEqual(reported, [{ context: forkCi.STATUS_CONTEXT, state, description, target_url }]);
    });
  }

  it('reports a change of CI files before it looks for a run', async () => {
    const { called } = await sync({ headTree: tree({ '.github': 'changed', '.config': 'c' }) });
    assert.deepEqual(called('listWorkflowRuns'), []);
  });

  it('refuses a green run when the CI files differ, even if the first sync missed it', async () => {
    const { reported } = await sync({
      headTree: tree({ '.github': 'changed', '.config': 'c' }),
      statuses: [status(forkCi.STATUS_CONTEXT, 'pending', 'Fork CI is running')],
    });
    assert.deepEqual(reported, [{ context: forkCi.STATUS_CONTEXT, state: 'pending', description: CHANGES_CI, target_url: `${PULL_URL}/files` }]);
  });

  for (const [title, reported] of [
    ['changes CI files', status(forkCi.STATUS_CONTEXT, 'pending', CHANGES_CI, `${PULL_URL}/files`)],
    ['has no workflow file', status(forkCi.STATUS_CONTEXT, 'error', NO_WORKFLOW, ACTIONS_URL)],
  ]) {
    it(`does not ask again about a commit that ${title}`, async () => {
      const { calls, posted } = await sync({ statuses: [reported] });
      assert.deepEqual(calls.map((call) => call.name), ['pulls.list', 'getCombinedStatusForRef']);
      assert.deepEqual(posted, []);
    });
  }

  it('takes the newest of several runs of the commit', async () => {
    const { contexts } = await sync({ runs: [run({ id: 3 }), run({ id: 9, conclusion: 'failure' }), run({ id: 5 })] });
    assert.deepEqual(contexts, ['Fork CI=failure']);
  });

  it('revokes the required check before it reports a run that no longer passes', async () => {
    const { reported } = await sync({ runs: [run({ conclusion: 'failure' })], statuses: accepted() });
    assert.deepEqual(reported, [
      { context: forkCi.REQUIRED_CONTEXT, state: 'pending', description: REVOKED, target_url: RUN_URL },
      { context: forkCi.STATUS_CONTEXT, state: 'failure', description: 'Fork CI finished with: failure', target_url: RUN_URL },
    ]);
  });

  it('does not verify again a green run that it already accepted', async () => {
    const { calls, posted } = await sync({ statuses: accepted() });
    assert.deepEqual(calls.map((call) => call.name), ['pulls.list', 'getCombinedStatusForRef', 'listWorkflowRuns']);
    assert.deepEqual(posted, []);
  });

  it('verifies a green run again when the accepted run was another one', async () => {
    const { contexts } = await sync({ statuses: accepted(`${RUN_URL}0`) });
    assert.deepEqual(contexts, ['finalize_pr=success', 'Fork CI=success']);
  });

  it('restores a required check that an earlier sync could not post', async () => {
    const { contexts } = await sync({
      statuses: [status(forkCi.REQUIRED_CONTEXT, 'pending', REVOKED), status(forkCi.STATUS_CONTEXT, 'success', 'Fork CI passed')],
    });
    assert.deepEqual(contexts, ['finalize_pr=success']);
  });

  it('leaves the fork status unsettled when the required check cannot be posted', async () => {
    const { posted, core } = await sync({ rejectStatus: (params) => params.context === forkCi.REQUIRED_CONTEXT });
    assert.deepEqual(posted, []);
    assert.equal(core.failure, '#1: status rejected');
  });

  for (const [title, subject] of [
    ['an unlabelled fork', pullRequest({ labels: [] })],
    ['an upstream branch', pullRequest({ head: REPOSITORY })],
    ['an upstream branch labelled for the upstream pipeline', pullRequest({ head: REPOSITORY, labels: [forkCi.LABEL_UPSTREAM] })],
    ['a deleted fork', pullRequest({ head: null })],
  ]) {
    it(`does not read or write anything for ${title}`, async () => {
      const { calls, posted } = await sync({ pulls: [subject] });
      assert.deepEqual(calls.map((call) => call.name), ['pulls.list']);
      assert.deepEqual(posted, []);
    });
  }
});

describe('pull requests with the same head commit', () => {
  const withoutRun = (params) => (params.owner === 'colleague' ? [] : [run()]);
  const pair = [pullRequest({ number: 1, head: OTHER_FORK }), pullRequest({ number: 2 })];

  it('reads the statuses of the commit one time', async () => {
    const { called } = await sync({ pulls: pair, runs: withoutRun });
    assert.equal(called('getCombinedStatusForRef').length, 1);
  });

  it('accepts a green run of one fork when the other fork has no run', async () => {
    const { reported } = await sync({ pulls: pair, runs: withoutRun });
    assert.deepEqual(reported.map((item) => [item.context, item.state, item.target_url]), [
      [forkCi.REQUIRED_CONTEXT, 'success', RUN_URL], [forkCi.STATUS_CONTEXT, 'success', RUN_URL],
    ]);
  });

  it('keeps an accepted run when the other fork has no run', async () => {
    const { posted } = await sync({ pulls: pair, runs: withoutRun, statuses: accepted() });
    assert.deepEqual(posted, []);
  });

  it('keeps an accepted run when the other fork is green too', async () => {
    const both = (params) => [run({ html_url: params.owner === 'colleague' ? OTHER_RUN_URL : RUN_URL })];
    const { posted } = await sync({ pulls: pair, runs: both, statuses: accepted() });
    assert.deepEqual(posted, []);
  });

  it('reports the first pull request when no fork is green', async () => {
    const { reported } = await sync({ pulls: pair, runs: (params) => (params.owner === 'colleague' ? [] : [run({ conclusion: 'failure' })]) });
    assert.deepEqual(reported, [{
      context: forkCi.STATUS_CONTEXT, state: 'pending', description: NO_RUN, target_url: `https://github.com/${OTHER_FORK}/actions`,
    }]);
  });
});

describe('pull request sent upstream', () => {
  const sent = pullRequest({ labels: [forkCi.LABEL_FORK, forkCi.LABEL_UPSTREAM] });
  const CHECKS_URL = `${PULL_URL}/checks`;
  const notUsed = { context: forkCi.STATUS_CONTEXT, state: 'success', description: 'Not used. ci:upstream runs the pipeline in apache/iggy', target_url: CHECKS_URL };
  const passed = { context: forkCi.REQUIRED_CONTEXT, state: 'success', description: 'The pipeline passed in apache/iggy', target_url: CHECKS_URL };
  const waiting = { context: forkCi.REQUIRED_CONTEXT, state: 'pending', description: 'The pipeline has not passed in apache/iggy', target_url: CHECKS_URL };
  const revoked = [status(forkCi.REQUIRED_CONTEXT, 'pending', REVOKED), status(forkCi.STATUS_CONTEXT, 'failure', 'Fork CI finished with: failure')];

  it('releases a revoked required check once the upstream pipeline passes', async () => {
    const { reported } = await sync({ pulls: [sent], statuses: revoked, checks: [{ id: 1, conclusion: 'success' }] });
    assert.deepEqual(reported, [passed, notUsed]);
  });

  it('takes back a fork success until the upstream pipeline passes', async () => {
    const { reported } = await sync({ pulls: [sent], statuses: accepted() });
    assert.deepEqual(reported, [waiting, notUsed]);
  });

  for (const [title, checks, expected] of [
    ['a newer success over an older cancelled check', [{ id: 3, conclusion: 'cancelled' }, { id: 5, conclusion: 'success' }], passed],
    ['a newer running check over an older success', [{ id: 5, conclusion: null }, { id: 3, conclusion: 'success' }], waiting],
    ['a newer failure over an older success', [{ id: 3, conclusion: 'success' }, { id: 5, conclusion: 'failure' }], waiting],
  ]) {
    it(`counts ${title}`, async () => {
      const { reported } = await sync({ pulls: [sent], statuses: accepted(), checks });
      assert.deepEqual(reported[0], expected);
    });
  }

  it('asks for the check named like the required status', async () => {
    const { called } = await sync({ pulls: [sent], statuses: accepted() });
    assert.deepEqual(called('checks.listForRef'), [{
      name: 'checks.listForRef', owner: 'apache', repo: 'iggy', ref: SHA, check_name: forkCi.REQUIRED_CONTEXT, filter: 'latest', per_page: 100,
    }]);
  });

  it('writes nothing on a commit that fork CI never reported', async () => {
    const { calls, posted } = await sync({ pulls: [sent] });
    assert.deepEqual(calls.map((call) => call.name), ['pulls.list', 'getCombinedStatusForRef']);
    assert.deepEqual(posted, []);
  });

  it('hands back a commit that another pull request still tests in its fork', async () => {
    const { reported, called } = await sync({ pulls: [pullRequest({ number: 1, head: OTHER_FORK }), { ...sent, number: 2 }], statuses: accepted() });
    assert.deepEqual(reported.map((item) => item.description), [waiting.description, notUsed.description]);
    assert.deepEqual(called('listWorkflowRuns'), []);
  });
});

describe('scheduled sweep', () => {
  const idle = Array.from({ length: 26 }, (_, index) => pullRequest({
    number: index + 1, sha: String(index).padStart(40, '0'), updated_at: LONG_AGO,
  }));
  const inspected = ({ called }) => called('getCombinedStatusForRef').map((call) => call.ref);

  it('reads one slice of the idle commits', async () => {
    assert.deepEqual(inspected(await sync({ pulls: idle })), idle.slice(0, 25).map((item) => item.head.sha));
  });

  it('reads every commit on a manual run', async () => {
    assert.equal(inspected(await sync({ pulls: idle }, 'workflow_dispatch')).length, 26);
  });

  it('follows a recently updated pull request outside the slice', async () => {
    const recent = pullRequest({ number: 26 });
    assert.equal(inspected(await sync({ pulls: [...idle.slice(0, 25), recent] })).at(-1), SHA);
  });

  it('continues after one commit fails and then fails the run', async () => {
    const failing = (params) => {
      if (params.owner === 'colleague') throw new Error('server error');
      return [run()];
    };
    const { contexts, core } = await sync({
      pulls: [pullRequest({ number: 1, head: OTHER_FORK, sha: 'd'.repeat(40) }), pullRequest({ number: 2 })], runs: failing,
    });
    assert.deepEqual(contexts, ['finalize_pr=success', 'Fork CI=success']);
    assert.equal(core.failure, '#1: server error');
  });
});

describe('sync after a Pre-merge run', () => {
  const event = (head_sha) => ({ workflow_run: { head_sha } });

  it('reports the commit that the run tested', async () => {
    const { contexts } = await sync({ runs: [run({ status: 'queued', conclusion: null })] }, 'workflow_run', event(SHA));
    assert.deepEqual(contexts, ['Fork CI=pending']);
  });

  it('reads no other commit', async () => {
    const other = pullRequest({ number: 2, sha: 'd'.repeat(40) });
    const { called } = await sync({ pulls: [other, pullRequest()] }, 'workflow_run', event(SHA));
    assert.deepEqual(called('getCombinedStatusForRef').map((call) => call.ref), [SHA]);
  });

  it('ignores a run of a commit that is no longer the head', async () => {
    const { calls } = await sync({}, 'workflow_run', event('d'.repeat(40)));
    assert.deepEqual(calls.map((call) => call.name), ['pulls.list']);
  });

  it('hands back a pull request that a maintainer sent upstream', async () => {
    const sent = pullRequest({ labels: [forkCi.LABEL_UPSTREAM] });
    const { contexts } = await sync({ pulls: [sent], statuses: accepted(), checks: [{ id: 1, conclusion: 'success' }] }, 'workflow_run', event(SHA));
    assert.deepEqual(contexts, ['finalize_pr=success', 'Fork CI=success']);
  });
});
