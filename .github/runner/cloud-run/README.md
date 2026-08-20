# Cloud Run runner pilot

This directory contains a narrow experiment for one Apache Iggy GitHub Actions
job on a Cloud Run worker pool. It is not a replacement for the existing
pre-merge workflows.

## Scope

The pilot runs `cargo fmt --all -- --check` and `cargo fetch --locked` on two
runners for comparison:

- one ephemeral Cloud Run runner;
- one GitHub-hosted `ubuntu-latest` runner.

The workflow runs only on an upstream push to
`ci/self-hosted-gcp-runners`. It has read-only repository permissions and is not
triggered by pull requests. Both jobs also require the repository
variable `IGGY_CLOUD_RUNNER_PILOT_ENABLED` to equal `true`; an absent variable
keeps it skipped while the workflow and runner-group restrictions are being
reviewed. Do not make it a required check.

The Cloud Run image does not provide Docker, privileged mode, `sudo`, or the
GitHub-hosted runner tool cache. Do not route BDD, testcontainers, Buildx,
`kind`, publish, or release jobs to this runner.

## Security boundary

Apache Infra must create an organization runner group named
`iggy-cloud-run-pilot` and restrict it to:

- the `apache/iggy` repository;
- `apache/iggy/.github/workflows/cloud-run-runner-pilot.yml@refs/heads/ci/self-hosted-gcp-runners`.

The Cloud Run worker pool must be configured with one instance for the pilot.

The runner registers with the custom label `iggy-cloud-run-x64`. The workflow
requires both the group and label, so a generic `self-hosted` runner cannot pick
up the job accidentally.

The initial test uses a short-lived organization registration token issued by
Apache Infra. It is stored in a dedicated Secret Manager secret, removed from
the runner process environment before the job starts, and used to register an
ephemeral one-job runner. Never use a classic or personal PAT here.

Use a dedicated GCP sandbox project. The runtime service account must have no
project-level role, no VPC connector, no production access, and no service
account impersonation permission. Grant it access only to the single temporary
registration-token secret. Cloud Run captures container stdout and stderr
without granting the workload service account a project-level logging role.

The worker can use its metadata identity during a job, so unsetting the token
environment variable is not the complete security boundary. After the runner
appears online and before pushing the pilot branch, destroy the exact one-time
secret version and remove the runtime service account's direct access to the
secret. Do not proceed unless Secret Manager reports the version as
`DESTROYED`. This remains effective even if broader IAM accidentally grants the
service account read access to the secret container.

## Why the first test does not autoscale

GitHub registration tokens expire after about one hour. A token supplied once
by Infra is sufficient for a fixed, one-shot pilot, but not for a pool that
creates new instances continuously.

Apache Beam solves continuous registration with a GitHub App and a GCP Cloud
Function that exchanges an App installation token for a fresh organization
runner registration token. Its newer Actions Runner Controller deployment also
uses GitHub App credentials. Iggy should reuse an Infra-approved version of
that pattern or an Infra-managed token broker before enabling autoscaling.

CREMA is intentionally excluded from this pilot. Its GitHub runner scaler has
an open scale-down issue where queued work can become zero while a job is still
in progress, allowing the worker pool to terminate the active runner. A fixed
single instance keeps that variable out of the first experiment.

## Continuous registration design

The image also supports a renewable JIT broker mode for later scale-to-zero.
Set `RUNNER_JIT_CONFIG_BROKER_URL` and
`RUNNER_JIT_CONFIG_BROKER_AUDIENCE` instead of
`RUNNER_REGISTRATION_TOKEN`. At startup the container obtains a Google-signed
identity token from the metadata server, calls the HTTPS broker, and expects a
JSON response containing a non-empty `encoded_jit_config`. It then starts the
runner with `run.sh --jitconfig`.

The broker is a separate control-plane service. It must:

- accept invocations only from the dedicated worker service account;
- use an ASF-installed GitHub App with organization `Self-hosted runners: write`;
- call GitHub's `generate-jitconfig` endpoint with the fixed Iggy runner group,
  label, and `_work` directory;
- ignore any client-supplied organization, runner group, labels, or repository;
- return only `{ "encoded_jit_config": "..." }` and never return an App key,
  installation token, or registration token;
- atomically lease one allowlisted queued `workflow_job` for each JIT config,
  limit issuance to one outstanding config per lease, and reject calls when no
  eligible lease exists;
- rate-limit and audit issuance without logging credentials.

Do not grant the worker service account Secret Manager access in broker mode.
Its only additional permission is invocation of that one broker service. The
pilot remains trusted-push-only because job code inherits the worker identity
and could otherwise invoke the broker itself.

Broker mode is not an authorization boundary for untrusted jobs. Rate limiting
alone does not prevent job code from minting another runner. Public pull
requests must remain GitHub-hosted. Supporting untrusted jobs would require a
separate controller identity to pre-authorize each queued job and a worker that
cannot independently mint another JIT config.

The broker solves runner registration, not CREMA authentication. The scaler
needs a separate service account and renewable GitHub Actions read access. Do
not reuse the worker identity or the broker's GitHub App private key in CREMA.

## Image contents

The image is Linux x86-64 only. Its base images are pinned by platform digest:

- GitHub Actions runner `2.336.0`;
- Rust `1.97.1`, matching `rust-toolchain.toml`.

Runner auto-update and Rust channel synchronization are disabled. The workflow
fails before formatting if `rust-toolchain.toml` no longer matches the image.
Update both digests and `RUSTUP_TOOLCHAIN` deliberately when either version
changes.

## One-shot test procedure

The commands below are an operator runbook, not an automated deployment. Pass
`--project` and `--region` on every command instead of changing the active
`gcloud` context.

First keep `IGGY_CLOUD_RUNNER_PILOT_ENABLED` absent or set it to `false`, then
push this gated workflow. Both jobs must be skipped. This makes the exact
workflow path and branch available for Apache Infra to review and add to the
runner-group allowlist without queueing work for a runner that does not exist.

Set local identifiers:

```bash
PROJECT_ID="replace-with-pmc-sandbox-project"
REGION="us-central1"
AR_REPOSITORY="iggy-ci-pilot"
WORKER_POOL="iggy-cloud-run-pilot"
RUNTIME_SERVICE_ACCOUNT="iggy-runner-runtime@${PROJECT_ID}.iam.gserviceaccount.com"
TOKEN_SECRET="iggy-runner-registration-token"
IMAGE_URI="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPOSITORY}/runner"
```

Build the pinned image from the repository root:

```bash
gcloud builds submit .github/runner/cloud-run \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --tag "${IMAGE_URI}:pilot"

IMAGE_DIGEST="$(gcloud artifacts docker images describe "${IMAGE_URI}:pilot" \
  --project "$PROJECT_ID" \
  --format='value(image_summary.digest)')"
test -n "$IMAGE_DIGEST"
```

Create the dedicated secret and grant the runtime identity access only to that
secret:

```bash
gcloud secrets create "$TOKEN_SECRET" \
  --project "$PROJECT_ID" \
  --replication-policy automatic

gcloud secrets add-iam-policy-binding "$TOKEN_SECRET" \
  --project "$PROJECT_ID" \
  --member "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" \
  --role roles/secretmanager.secretAccessor
```

Add the token supplied by Infra without placing it in shell history:

```bash
read -rsp "Runner registration token: " RUNNER_REGISTRATION_TOKEN
printf '%s' "$RUNNER_REGISTRATION_TOKEN" | gcloud secrets versions add "$TOKEN_SECRET" \
  --project "$PROJECT_ID" \
  --data-file=-
unset RUNNER_REGISTRATION_TOKEN
```

Deploy exactly one instance by immutable image digest:

```bash
TOKEN_VERSION="replace-with-created-secret-version"
gcloud run worker-pools deploy "$WORKER_POOL" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --image "${IMAGE_URI}@${IMAGE_DIGEST}" \
  --instances 1 \
  --cpu 4 \
  --memory 8Gi \
  --service-account "$RUNTIME_SERVICE_ACCOUNT" \
  --set-secrets "RUNNER_REGISTRATION_TOKEN=${TOKEN_SECRET}:${TOKEN_VERSION}"
```

After the runner is online, destroy the one-time token version and revoke the
runtime identity's direct access before enabling or triggering the pilot:

```bash
gcloud secrets versions destroy "$TOKEN_VERSION" \
  --project "$PROJECT_ID" \
  --secret "$TOKEN_SECRET"

gcloud secrets remove-iam-policy-binding "$TOKEN_SECRET" \
  --project "$PROJECT_ID" \
  --member "serviceAccount:${RUNTIME_SERVICE_ACCOUNT}" \
  --role roles/secretmanager.secretAccessor

test "$(gcloud secrets versions describe "$TOKEN_VERSION" \
  --project "$PROJECT_ID" \
  --secret "$TOKEN_SECRET" \
  --format 'value(state)')" = "DESTROYED"
```

Set `IGGY_CLOUD_RUNNER_PILOT_ENABLED=true` only after Apache Infra confirms the
group restriction and the command above proves the one-time credential no
longer exists. Trigger the workflow with a new push to the pilot branch. Scale
the pool to zero as soon as the single job finishes:

```bash
gcloud run worker-pools update "$WORKER_POOL" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --instances 0
```

After the ephemeral runner exits, the entrypoint deliberately keeps the
container idle instead of registering another runner. The pool still incurs
cost until it is scaled to zero, so prompt scale-down is mandatory.

## Acceptance criteria

The first pilot passes only when all of the following are true:

1. Apache Infra confirms the runner group and workflow restriction.
2. The Cloud Run job and GitHub-hosted baseline check the same commit SHA.
3. The runner handles exactly one job and is automatically de-registered.
4. No pull request or other workflow can target the runner group.
5. The worker has no access to production, deployment credentials, or private
   networks.
6. The exact registration-token secret version is `DESTROYED` before the
   workflow is triggered.
7. Cloud Logging shows no restart, OOM, unexpected signal, or registration
   secret in logs.
8. The pool is scaled to zero after the experiment.

Compare queue time, startup time, execution time, result, peak memory, and cost.
Repeat at least 20 times before considering another non-Docker job.

## Autoscaling gate

Do not add CREMA or move required checks until all of these are complete:

- Infra approves a GitHub App or token-broker lifecycle for fresh registration
  or JIT configuration;
- no long-lived GitHub credential is available to job code;
- the scaler counts both queued and in-progress jobs and cannot terminate an
  active job;
- at least 100 synthetic jobs complete without `lost communication`, stale
  registrations, or cross-workflow scheduling;
- rollback to GitHub-hosted runners and scale-to-zero are exercised.

Relevant references:

- [ASF self-hosted runner policy](https://infra.apache.org/self-hosted-runners.html)
- [GitHub self-hosted runner security](https://docs.github.com/en/actions/reference/security/secure-use)
- [GitHub ephemeral runner guidance](https://docs.github.com/en/actions/reference/runners/self-hosted-runners)
- [Cloud Run GitHub runner tutorial](https://docs.cloud.google.com/run/docs/tutorials/github-runner)
- [Cloud Run runtime contract](https://docs.cloud.google.com/run/docs/container-contract)
- [Apache Beam token function](https://github.com/apache/beam/blob/master/.github/gh-actions-self-hosted-runners/helper-functions/cloud-functions/generateToken/index.js)
- [CREMA scale-down issue](https://github.com/GoogleCloudPlatform/cloud-run-external-metrics-autoscaling/issues/6)
