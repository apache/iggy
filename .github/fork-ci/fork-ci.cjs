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

// Fork CI: a pull request from a fork runs the Pre-merge pipeline on the
// contributor's runners instead of the runners all Apache projects share.
//
// `gate` runs first in _detect.yml and decides whether a Pre-merge run executes
// the pipeline. `sync` runs in fork-ci-status.yml from the default branch. It
// reads the fork's run and reports it on the head commit, including the check
// that branch protection requires.
//
// The contributor controls the fork, so `sync` accepts a green run only when
// GitHub hosted every runner and the commit changes no directory that decides
// what the run executes. Branch protection keeps `strict: true`, so a head that
// can merge already contains master, and with it master's workflows. The
// Actions caches of the fork are not verified.

'use strict';

// false: only pull requests labelled LABEL_FORK use fork CI.
// true: every pull request from a fork does, unless labelled LABEL_UPSTREAM.
const ENABLED = false;
const LABEL_FORK = 'ci:fork';
const LABEL_UPSTREAM = 'ci:upstream';

const BASE_BRANCH = 'master';
const FORK_WORKFLOW = 'fork-ci.yml';
const FORK_WORKFLOW_PATH = `.github/workflows/${FORK_WORKFLOW}`;
const STATUS_CONTEXT = 'Fork CI';
// Required by .asf.yaml. The pipeline of this repository reports it as a check
// run, `sync` reports it as a commit status. When both exist on a commit,
// GitHub requires both.
const REQUIRED_CONTEXT = 'finalize_pr';

const CI_DEFINING_DIRECTORIES = ['.github', '.config'];
// GitHub-hosted jobs report this group. Both fields are compared, because the
// owner of a self-hosted group chooses its name.
const HOSTED_RUNNER_GROUP = { id: 0, name: 'GitHub Actions' };

const RECENT_MS = 6 * 60 * 60 * 1000;
const SWEEP_INTERVAL_MS = 15 * 60 * 1000;
const SWEEP_SIZE = 25;

const CHANGES_CI = `This pull request changes CI files. A maintainer must add ${LABEL_UPSTREAM}`;
const NO_WORKFLOW = `The branch has no Fork CI workflow. Update it from ${BASE_BRANCH}`;

const sameRepository = (left, right) => left.toLowerCase() === right.toLowerCase();
const hasLabel = (pullRequest, name) => pullRequest.labels.some((label) => label.name === name);

// A branch of the upstream repository, or of a deleted fork, has no fork to run in.
function isFromFork(pullRequest, repository) {
  const head = pullRequest.head.repo?.full_name;
  return Boolean(head) && !sameRepository(head, repository);
}

function usesForkCi(pullRequest, repository, enabled = ENABLED) {
  return isFromFork(pullRequest, repository) && !hasLabel(pullRequest, LABEL_UPSTREAM) &&
    (enabled || hasLabel(pullRequest, LABEL_FORK));
}

async function gate({ github, context, core }) {
  const repository = `${context.repo.owner}/${context.repo.repo}`;
  const pullRequest = context.payload.pull_request;
  let runCi = true;
  if (context.eventName === 'pull_request' && isFromFork(pullRequest, repository)) {
    try {
      // A re-run replays the labels of the original event. Read the live ones,
      // so that a maintainer can add a label and re-run the jobs.
      const { data } = await github.rest.pulls.get({ ...context.repo, pull_number: pullRequest.number });
      runCi = !usesForkCi(data, repository);
    } catch (error) {
      // The pipeline of this repository is a valid result for every pull
      // request. A failed required check needs a maintainer to re-run it.
      core.warning(`Cannot read the labels, so the pipeline runs here: ${error.message}`);
    }
  }
  core.info(runCi ? 'The pipeline runs here' : 'The pipeline runs in the fork');
  core.setOutput('run_ci', String(runCi));
}

async function sync({ github, context, core, now = Date.now() }) {
  const upstream = context.repo;
  const repository = `${upstream.owner}/${upstream.repo}`;

  // A fork without the workflow, or with Actions disabled, answers 404. Any
  // other error must fail the sync instead of reporting missing CI.
  async function optional(request) {
    try {
      return (await request()).data;
    } catch (error) {
      if (error.status === 404) return null;
      throw error;
    }
  }

  async function statusesOf(sha) {
    const { data } = await github.rest.repos.getCombinedStatusForRef({ ...upstream, ref: sha, per_page: 100 });
    const current = (name) => data.statuses.find((status) => status.context === name);
    async function report(name, status) {
      const existing = current(name);
      if (existing?.state === status.state && existing.description === status.description &&
          existing.target_url === status.target_url) return;
      await github.rest.repos.createCommitStatus({ ...upstream, sha, context: name, ...status });
    }
    return { current, report };
  }

  // Compares the commit with the point where it left the base branch. The
  // answer depends on the commit alone, so a pull request whose head moves
  // during the sync cannot change it.
  async function changesCi(sha) {
    const { data: comparison } = await github.rest.repos.compareCommitsWithBasehead({
      ...upstream, basehead: `${BASE_BRANCH}...${sha}`, per_page: 1,
    });
    const [base, head] = await Promise.all([comparison.merge_base_commit.sha, sha].map(async (commit) => {
      const { data } = await github.rest.git.getTree({ ...upstream, tree_sha: commit });
      return CI_DEFINING_DIRECTORIES.map((path) => data.tree.find((entry) => entry.path === path)?.sha);
    }));
    return base.some((hash, index) => hash !== head[index]);
  }

  // Returns nothing when an earlier sync accepted the same run.
  async function inspectFork(pullRequest, sha, isAccepted) {
    const fork = { owner: pullRequest.head.repo.owner.login, repo: pullRequest.head.repo.name };
    const actionsUrl = `${pullRequest.head.repo.html_url}/actions`;

    // Only a push run executes the workflow of the head commit itself. A pull
    // request inside the fork runs a merge that the contributor controls.
    const runs = await optional(() => github.rest.actions.listWorkflowRuns({
      ...fork, workflow_id: FORK_WORKFLOW, head_sha: sha, event: 'push', per_page: 100,
    }));
    const run = (runs?.workflow_runs ?? [])
      .filter((item) => item.head_sha === sha && item.event === 'push')
      .sort((left, right) => right.id - left.id)[0];
    if (!run) {
      // GitHub runs the workflow file of the pushed commit, so a branch from
      // an older base starts no run, whatever the fork lists as active.
      const file = await optional(() => github.rest.repos.getContent({ ...fork, path: FORK_WORKFLOW_PATH, ref: sha }));
      if (!file) return { state: 'error', description: NO_WORKFLOW, target_url: actionsUrl };
      const workflow = await optional(() => github.rest.actions.getWorkflow({ ...fork, workflow_id: FORK_WORKFLOW }));
      return workflow?.state === 'active'
        ? { state: 'pending', description: 'No Fork CI run for this commit. Push a new commit to start one', target_url: actionsUrl }
        : { state: 'error', description: 'Enable GitHub Actions in your fork, then push a new commit', target_url: actionsUrl };
    }

    const target_url = run.html_url;
    if (run.status !== 'completed') {
      const phase = run.status === 'in_progress' ? 'running' : 'queued';
      return { state: 'pending', description: `Fork CI is ${phase}`, target_url };
    }
    if (run.conclusion !== 'success') {
      return { state: 'failure', description: `Fork CI finished with: ${run.conclusion}`, target_url };
    }
    if (isAccepted(target_url)) return null;

    const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun, {
      ...fork, run_id: run.id, filter: 'latest', per_page: 100,
    });
    const isHosted = (job) => job.runner_group_id === HOSTED_RUNNER_GROUP.id && job.runner_group_name === HOSTED_RUNNER_GROUP.name;
    if (jobs.some((job) => job.conclusion !== 'skipped' && !isHosted(job))) {
      return { state: 'error', description: `A Fork CI job did not use a GitHub-hosted runner. A maintainer must add ${LABEL_UPSTREAM}`, target_url };
    }
    return { state: 'success', description: 'Fork CI passed', target_url };
  }

  async function inspect(pullRequests, sha, current) {
    const reported = current(STATUS_CONTEXT);
    // The files of a commit never change, so these verdicts stay.
    if ([CHANGES_CI, NO_WORKFLOW].includes(reported?.description)) return null;
    const changesCiFiles = { state: 'pending', description: CHANGES_CI, target_url: `${pullRequests[0].html_url}/files` };
    // Say it at the first sync of a commit, before the contributor waits for
    // a run that cannot count.
    if (!reported && await changesCi(sha)) return changesCiFiles;

    const isAccepted = (url) => [STATUS_CONTEXT, REQUIRED_CONTEXT]
      .every((name) => current(name)?.state === 'success' && current(name).target_url === url);
    const results = [];
    for (const pullRequest of pullRequests) {
      const result = await inspectFork(pullRequest, sha, isAccepted);
      if (!result) return null;
      results.push(result);
    }
    const passed = results.find((result) => result.state === 'success');
    if (!passed) return results[0];
    return (await changesCi(sha)) ? changesCiFiles : passed;
  }

  // The statuses belong to the commit, so pull requests with the same head get
  // one verdict. A sync for each of them lets one fork overwrite the result of
  // another. A green run in any of the forks proves the commit.
  async function syncCommit(pullRequests) {
    const sha = pullRequests[0].head.sha;
    const { current, report } = await statusesOf(sha);
    const result = await inspect(pullRequests, sha, current);
    if (!result) return;

    // Only success is ever claimed: a commit without the required status cannot
    // merge. A success that no longer holds, for example after a failed re-run,
    // is revoked. The required status goes first, so a failure in between
    // leaves STATUS_CONTEXT unsettled and the next sync repeats both.
    if (result.state === 'success') {
      await report(REQUIRED_CONTEXT, result);
    } else if (current(REQUIRED_CONTEXT)?.state === 'success') {
      await report(REQUIRED_CONTEXT, {
        state: 'pending', description: 'The Fork CI result for this commit no longer holds', target_url: result.target_url,
      });
    }
    await report(STATUS_CONTEXT, result);
    core.info(`#${pullRequests[0].number}: ${result.state} (${result.description})`);
  }

  // LABEL_UPSTREAM moves a pull request to the pipeline of this repository, but
  // the statuses of an earlier sync stay on its head. A REQUIRED_CONTEXT status
  // left there would block the commit for good, or allow a merge before that
  // pipeline finished. From now on it follows the check run of the pipeline.
  async function handBack(pullRequests) {
    const sha = pullRequests[0].head.sha;
    const { current, report } = await statusesOf(sha);
    const target_url = `${pullRequests[0].html_url}/checks`;
    if (current(REQUIRED_CONTEXT)) {
      const { data } = await github.rest.checks.listForRef({
        ...upstream, ref: sha, check_name: REQUIRED_CONTEXT, filter: 'latest', per_page: 100,
      });
      // An older run of the pipeline can leave a failed check on the commit.
      const newest = data.check_runs.sort((left, right) => right.id - left.id)[0];
      await report(REQUIRED_CONTEXT, newest?.conclusion === 'success'
        ? { state: 'success', description: `The pipeline passed in ${repository}`, target_url }
        : { state: 'pending', description: `The pipeline has not passed in ${repository}`, target_url });
    }
    if (current(STATUS_CONTEXT)) {
      await report(STATUS_CONTEXT, {
        state: 'success', description: `Not used. ${LABEL_UPSTREAM} runs the pipeline in ${repository}`, target_url,
      });
    }
  }

  const open = (await github.paginate(github.rest.pulls.list, {
    ...upstream, state: 'open', base: BASE_BRANCH, per_page: 100,
  })).sort((left, right) => left.number - right.number);
  const isSentUpstream = (pullRequest) => isFromFork(pullRequest, repository) && hasLabel(pullRequest, LABEL_UPSTREAM);
  const commits = new Map();
  for (const pullRequest of open) {
    if (!usesForkCi(pullRequest, repository) && !isSentUpstream(pullRequest)) continue;
    commits.set(pullRequest.head.sha, [...(commits.get(pullRequest.head.sha) ?? []), pullRequest]);
  }
  const syncGroup = (group) => (group.some(isSentUpstream) ? handBack(group) : syncCommit(group));

  if (context.eventName === 'workflow_run') {
    // GitHub can leave workflow_run.pull_requests empty for a fork, so find the
    // pull requests by the commit. A run of an older commit matches none.
    const group = commits.get(context.payload.workflow_run.head_sha);
    if (group) await syncGroup(group);
    return;
  }

  // Each scheduled sync follows the recently updated pull requests and one
  // rotating slice of the rest, which finds re-runs in the fork. This bounds
  // the API calls. The slice follows the clock, so a late or replaced run
  // skips its slice until the next rotation. A manual run reads everything.
  const groups = [...commits.values()];
  const everything = context.eventName === 'workflow_dispatch';
  const slices = Math.ceil(groups.length / SWEEP_SIZE);
  const slice = Math.floor(now / SWEEP_INTERVAL_MS) % slices;
  const errors = [];
  for (const [index, group] of groups.entries()) {
    const swept = everything || Math.floor(index / SWEEP_SIZE) === slice;
    if (!swept && group.every((pullRequest) => now - Date.parse(pullRequest.updated_at) > RECENT_MS)) continue;
    try {
      await syncGroup(group);
    } catch (error) {
      // One failed commit must not stop the others.
      errors.push(`${group.map((pullRequest) => `#${pullRequest.number}`).join(', ')}: ${error.message}`);
    }
  }
  if (errors.length) core.setFailed(errors.join('\n'));
}

module.exports = {
  ENABLED, LABEL_FORK, LABEL_UPSTREAM, FORK_WORKFLOW, STATUS_CONTEXT, REQUIRED_CONTEXT, SWEEP_INTERVAL_MS,
  usesForkCi, gate, sync,
};
