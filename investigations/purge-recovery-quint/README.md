# Purge and recovery model

These [Quint](https://quint.sh/docs/quint) models investigate how a persisted
purge cutoff interacts with operation history, crashes, and recovery.

The starting failure is a saved cutoff of 3 surviving the loss of the volatile
operation journal. If recovery authorizes a new empty history, fresh operation 1
can be mistaken for purged data. These are internal operation numbers, separate
from public message offsets.

## Models and limits

- [recovery.qnt](recovery.qnt) models three replicas, one completed purge, two
  possible numbering histories, and one fresh write per history. It compares
  retaining, clearing, raising, and clamping the boundary with adopting the
  boundary belonging to the selected history.
- [lifecycle.qnt](lifecycle.qnt) models bookmark deletion and directory sync,
  cleanup retries after fresh writes, and state transfer with rollback. Its
  transfer module distinguishes the partition cutoff from the journal poll floor.

Durable and volatile state are separate. The recovery model permits loss of
messages when every volatile copy disappears under `Replicated` durability.
The local cleanup model explicitly uses a fresh message already made durable.

Consensus history selection and fencing are assumptions. The candidate named
`certified` trusts an authoritative boundary; it does not construct or validate
a certificate. Journal replay and materialized snapshot installation are modeled
separately. Individual persistence helpers are atomic abstractions, so torn writes
and every filesystem failure boundary are outside the models.

Passing TLC checks cover the reachable states of these finite configurations.
Scenario tests demonstrate that recovery and fresh writes are possible; no
fairness or general liveness property is checked. The models do not prove that
the Rust implementation follows these rules or cover arbitrary replica counts,
multiple purges, mixed durability policies, or the complete replication protocol.

## Run

Install Quint 0.32.0 outside the repository and put a compatible Java runtime on
`PATH`. The models were checked with Java 23.0.1. From this directory:

```sh
quint_tools="$(mktemp -d)"
npm install --prefix "$quint_tools" --cache "$quint_tools/cache" @informalsystems/quint@0.32.0
export QUINT_HOME="$quint_tools/home"
QUINT="$quint_tools/node_modules/.bin/quint"
python3 check.py --quint="$QUINT" --suite=all
```

Use `--suite=quick` for type checking, scenario tests, and sampled simulation,
or `--suite=tlc` for exhaustive checking of the finite models. Initial TLC use
may download its dependencies and start a local Apalache compiler service.

The runner checks both expected counterexamples and passing candidates. A bug
scenario passes when it demonstrates the expected violation. Tool failures and
timeouts fail the run. There are 19 deterministic scenarios and 23 commands in
the complete suite, including simulations and TLC checks.

Logs and command summaries are written to the ignored `results/` directory.
Generated output stays local. The comments and named scenarios in the model
sources explain the failure sequences and the assumptions behind each candidate.
