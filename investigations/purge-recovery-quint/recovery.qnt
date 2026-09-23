// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Recovery must distinguish a fresh message from purged history even when both
// have the same internal operation number. This model explores that distinction.
//
// Worked example, executed by knownBugTest below:
//   Event                         Last operation   Saved purge cutoff   Fresh readable?
//   Purge operations 1 through 3  3                3                    No fresh write
//   Lose every journal; restart  0                3                    No fresh write
//   Authorize an empty history   0                3                    No fresh write
//   Commit a fresh operation 1   1                3                    NO: 1 > 3 is false
// The authorization in row 3 is ASSUMED here; this model does not run an election.
// These operation numbers belong to replication, not public message offsets.
//
// Read top to bottom: state -> starting policies -> events -> checks -> examples.
// The assumptions at establishEmpty and adopt define which recoveries are possible.
// The checks state what must remain true; the examples show concrete executions.
//
// Quint reading key:
//   action = a possible event; all = every guard must hold; any = choose an event.
//   state' = the resulting state; ...state = keep fields not explicitly changed.
//   nondet = explore a choice; forall = every member; exists = at least one member.
//   filter = keep matching members; subseteq = every member occurs in the other set.
//   run ... .then(...) = a concrete execution; .expect(...) = its required outcome.
//
// Scope: three replicas, one completed purge, two numbering histories, one fresh
// write per history. Repeated crashes are allowed. Journals are held in memory;
// this represents Replicated durability without a durable prepare journal.
// Elections, network traffic, multiple purges and persisted WAL recovery are outside
// this model. Full snapshot transfer has a separate model in lifecycle.qnt.
// Rust counterparts below refer to core/partitions/src/iggy_partition.rs.
module recovery {
  // 1. STATE: distinguish retained replication history from readable messages.

  // A bookmark is a consumer's saved position. wasPurged is a label for checking
  // the model; recovery cannot inspect it to decide whether an entry is fresh.
  type Entry = {
    operation: int,
    wasPurged: bool,
    isBookmark: bool
  }
  type Replica = {
    // Process state. A running replica may still lack permission to serve.
    running: bool,
    serving: bool,
    // Numbering history: 0 before total loss, 1 after empty recovery. The label
    // remains for bookkeeping while down; it grants no recovery authority.
    historyId: int,
    lastOperation: int,

    // Durable state: these two values survive crash. savedHistoryId is used only
    // by the candidate that assumes an authoritative boundary for its history.
    savedHistoryId: int,
    savedPurgeCutoff: int,

    // Volatile gates: applying and polling both require operation > cutoff.
    applyCutoff: int,
    pollCutoff: int,
    // Purge may retain journal entries for replication while hiding their data.
    journal: Set[Entry],
    readableMessages: Set[Entry],
    purgedBookmarkRestored: bool
  }
  type State = {
    recoveryPolicy: str,
    replicas: int -> Replica,
    activeReplicas: Set[int],

    // Model bookkeeping, not extra persisted cluster state. The first two fields
    // bound exploration; the last two record whether an event has ever occurred.
    newHistoryEstablished: bool,
    historiesWithFreshWrite: Set[int],
    replayOccurred: bool,
    newHistoryCrashed: bool
  }
  var state: State
  pure val replicaIds = Set(0, 1, 2)
  pure val quorums = Set(Set(0, 1), Set(0, 2), Set(1, 2))
  pure val purgedJournal = Set(
    { operation: 1, wasPurged: true, isBookmark: false },
    { operation: 2, wasPurged: true, isBookmark: true },
    { operation: 3, wasPurged: true, isBookmark: false })
  pure val initialReplica = {
    running: true,
    serving: true,
    historyId: 0,
    lastOperation: 3,
    savedHistoryId: 0,
    savedPurgeCutoff: 3,
    applyCutoff: 3,
    pollCutoff: 3,
    journal: purgedJournal,
    readableMessages: Set(),
    purgedBookmarkRestored: false
  }
  pure def initialState(recoveryPolicy: str): State = {
    recoveryPolicy: recoveryPolicy,
    replicas: replicaIds.mapBy(_ => initialReplica),
    activeReplicas: replicaIds,
    newHistoryEstablished: false,
    historiesWithFreshWrite: Set(),
    replayOccurred: false,
    newHistoryCrashed: false
  }

  // 2. STARTING POLICIES: same purged partition, different proposed recovery rules.
  // The initializer names remain stable for the existing verification commands.

  // Keep the saved cutoff, even if the numbering history disappears.
  action init = state' = initialState("current")
  // Erase the cutoff on every restart, including rolling restarts.
  action initClear = state' = initialState("clear")
  // Claim a journal head equal to the cutoff without journal evidence.
  action initRaise = state' = initialState("raise")
  // Persist min(local cutoff, adopted head) before replaying an adopted journal.
  action initClamp = state' = initialState("clamp")
  // Clear the cutoff for empty recovery in memory, leaving disk unchanged.
  action initVolatile = state' = initialState("volatile")
  // ASSUMPTION: adopt a trustworthy authority's boundary and persist it with its
  // history. Despite this command's name, no certificate is built or validated.
  action initCertified = state' = initialState("certified")

  // 3. EVENTS: each action has guards, then the state change they permit.

  // Lose the journal and readable data, but keep the saved cutoff. Losing every
  // volatile copy may lose fresh data; Replicated does not promise otherwise.
  action crash(node: int): bool = {
    val replica = state.replicas.get(node)
    all {
      replica.running,
      state' = {
        ...state,
        replicas: state.replicas.set(node, {
          ...replica,
          running: false,
          serving: false,
          lastOperation: 0,
          applyCutoff: 0,
          pollCutoff: 0,
          journal: Set(),
          readableMessages: Set(),
          purgedBookmarkRestored: false
        }),
        activeReplicas: state.activeReplicas.exclude(Set(node)),
        newHistoryCrashed: state.newHistoryCrashed or
          (state.newHistoryEstablished and replica.historyId == 1)
      }
    }
  }

  // Reload the cutoff, but do not serve until a history is selected. The clear
  // and raise proposals change this boot step; the other proposals act later.
  // Rust: hydrate_applied_purge_generation reloads the cutoff while
  // open_persistence_with_recovered may skip reopening an operation journal.
  action restart(node: int): bool = {
    val replica = state.replicas.get(node)
    val applyCutoff = if (state.recoveryPolicy == "clear") 0 else replica.savedPurgeCutoff
    all {
      not(replica.running),
      state' = {
        ...state,
        replicas: state.replicas.set(node, {
          ...replica,
          running: true,
          serving: false,
          applyCutoff: applyCutoff,
          savedPurgeCutoff: if (state.recoveryPolicy == "clear") 0 else replica.savedPurgeCutoff,
          lastOperation: if (state.recoveryPolicy == "raise") applyCutoff else 0
        })
      }
    }
  }

  // ASSUMPTION: two empty restarted replicas may establish history 1 and fence
  // the previous authority. This action stands in for that consensus decision.
  // The guard that no fresh journal entry survives is an environmental restriction,
  // not an election algorithm. savedHistoryId records an association; it does not
  // prove that the decision is authorized or that Rust can reach this transition.
  action establishEmpty(quorum: Set[int]): bool = all {
    not(state.newHistoryEstablished),
    replicaIds.forall(node => state.replicas.get(node).journal.forall(entry => entry.wasPurged)),
    quorum.forall(node => {
      val replica = state.replicas.get(node)
      replica.running and not(replica.serving) and replica.journal == Set()
    }),
    val replacements = replicaIds.mapBy(node => {
      val replica = state.replicas.get(node)
      if (quorum.contains(node)) {
        val applyCutoff =
          if (Set("clamp", "volatile", "certified").contains(state.recoveryPolicy)) 0
          else replica.applyCutoff
        {
          ...replica,
          serving: true,
          historyId: 1,
          lastOperation: 0,
          applyCutoff: applyCutoff,
          pollCutoff: 0,
          savedHistoryId: if (state.recoveryPolicy == "certified") 1 else replica.savedHistoryId,
          savedPurgeCutoff:
            if (Set("clamp", "certified").contains(state.recoveryPolicy)) 0
            else replica.savedPurgeCutoff
        }
      } else {
        ...replica,
        serving: false
      }
    })
    state' = {
      ...state,
      replicas: replacements,
      activeReplicas: quorum,
      newHistoryEstablished: true
    }
  }

  // Adopt a donor's journal, then replay it through the receiver's apply cutoff.
  // ASSUMPTION: the donor is authorized and has at least the head of every active
  // replica. Persisting a candidate boundary with its history is atomic here.
  //
  // This is JOURNAL REPLAY. Full snapshot installation copies materialized data;
  // it does not filter installed messages through this replay gate. The separate
  // lifecycle_transfer module examines its persistence and poll cutoff instead.
  // Rust: apply_repaired_prepare and append_repaired_send_messages replay entries;
  // the latter retains an entry but skips materialization when its number is fenced.
  action adopt(source: int, target: int): bool = {
    val donor = state.replicas.get(source)
    val receiver = state.replicas.get(target)
    val applyCutoff =
      if (state.recoveryPolicy == "certified") donor.applyCutoff
      else if (state.recoveryPolicy == "clamp")
        (if (receiver.applyCutoff < donor.lastOperation) receiver.applyCutoff else donor.lastOperation)
      else receiver.applyCutoff
    all {
      source != target,
      state.activeReplicas.contains(source),
      donor.serving,
      state.activeReplicas.forall(node => state.replicas.get(node).lastOperation <= donor.lastOperation),
      receiver.running,
      not(receiver.serving),
      state' = {
        ...state,
        replicas: state.replicas.set(target, {
          ...receiver,
          serving: true,
          historyId: donor.historyId,
          lastOperation: donor.lastOperation,
          journal: donor.journal,
          applyCutoff: applyCutoff,
          pollCutoff: applyCutoff,
          savedHistoryId:
            if (state.recoveryPolicy == "certified") donor.historyId else receiver.savedHistoryId,
          savedPurgeCutoff:
            if (Set("clamp", "certified").contains(state.recoveryPolicy)) applyCutoff
            else receiver.savedPurgeCutoff,
          readableMessages:
            donor.journal.filter(entry => not(entry.isBookmark) and entry.operation > applyCutoff),
          purgedBookmarkRestored:
            donor.journal.exists(entry =>
              entry.isBookmark and entry.wasPurged and entry.operation > applyCutoff)
        }),
        activeReplicas: state.activeReplicas.union(Set(target)),
        replayOccurred: true
      }
    }
  }

  // ASSUMPTION: admission, replication and commit succeed as one event on a quorum
  // sharing a history and head. One fresh write per history keeps the model finite.
  // The journal advances even when a stale cutoff hides the new message.
  action commitFresh(quorum: Set[int]): bool = {
    nondet source = quorum.oneOf()
    val donor = state.replicas.get(source)
    val operation = donor.lastOperation + 1
    val entry = { operation: operation, wasPurged: false, isBookmark: false }
    all {
      not(state.historiesWithFreshWrite.contains(donor.historyId)),
      quorum.subseteq(state.activeReplicas),
      quorum.forall(node => {
        val replica = state.replicas.get(node)
        replica.running and replica.serving and replica.historyId == donor.historyId and
          replica.lastOperation == donor.lastOperation
      }),
      state' = {
        ...state,
        historiesWithFreshWrite: state.historiesWithFreshWrite.union(Set(donor.historyId)),
        replicas: replicaIds.mapBy(node => {
          val replica = state.replicas.get(node)
          if (quorum.contains(node)) {
            ...replica,
            lastOperation: operation,
            journal: replica.journal.union(Set(entry)),
            readableMessages:
              if (operation > replica.applyCutoff and operation > replica.pollCutoff)
                replica.readableMessages.union(Set(entry))
              else replica.readableMessages
          } else replica
        })
      }
    }
  }

  // Explore enabled events in any order, including doing nothing. This permits
  // repeated failures; safety alone therefore cannot prove eventual recovery.
  action step = {
    nondet node = replicaIds.oneOf()
    nondet target = replicaIds.oneOf()
    nondet quorum = quorums.oneOf()
    any {
      crash(node),
      restart(node),
      establishEmpty(quorum),
      adopt(node, target),
      commitFresh(quorum),
      state' = state
    }
  }

  // 4. CHECKS: these are conclusions to test, unlike the assumptions above.

  // Purged messages and bookmarks must never become visible on a serving replica.
  // Sensitivity example: clearResurrectsTest violates this after rolling repair.
  val noResurrection = replicaIds.forall(node => {
    val replica = state.replicas.get(node)
    not(replica.serving) or
      (replica.readableMessages.forall(entry => not(entry.wasPurged)) and
        not(replica.purgedBookmarkRestored))
  })

  // A serving replica must expose fresh committed data that it STILL HOLDS.
  // No promise is made for data whose every volatile copy was lost in a crash.
  // Sensitivity examples: knownBugTest, clampDelayedReplicaTest, volatileRevertsTest.
  val freshNotHidden = replicaIds.forall(node => {
    val replica = state.replicas.get(node)
    not(replica.serving) or
      replica.journal.filter(entry => not(entry.wasPurged)).subseteq(replica.readableMessages)
  })

  // Every claimed head needs a journal entry behind it, even before serving.
  // There is no certified checkpoint in this model that could stand in for it.
  // Sensitivity example: raiseInventsHistoryTest claims operation 3 with no entry.
  val noInventedHistory = replicaIds.forall(node => {
    val replica = state.replicas.get(node)
    replica.lastOperation == 0 or
      replica.journal.exists(entry => entry.operation == replica.lastOperation)
  })

  // The candidate that adopts an authority must persist the association it serves with.
  // Equality checks that association; it does not establish the authority's validity.
  val boundaryMatchesHistory = state.recoveryPolicy != "certified" or replicaIds.forall(node => {
    val replica = state.replicas.get(node)
    not(replica.serving) or
      (replica.historyId == replica.savedHistoryId and replica.applyCutoff == replica.savedPurgeCutoff)
  })
  val safe = noResurrection and freshNotHidden and noInventedHistory and boundaryMatchesHistory

  // Coverage witnesses answer "can useful work happen?" They are not guarantees
  // that every run makes progress. The event flags are cumulative; the scenarios
  // below establish the intended order of events more precisely.
  val reusedOperation = state.newHistoryEstablished and state.historiesWithFreshWrite.contains(1) and
    replicaIds.exists(node => {
      val replica = state.replicas.get(node)
      replica.journal.exists(entry => not(entry.wasPurged) and entry.operation == 1)
    })
  val freshAfterRestart = reusedOperation and freshNotHidden
  val delayedJoin = state.newHistoryEstablished and state.historiesWithFreshWrite.contains(1) and
    state.replayOccurred and state.activeReplicas == replicaIds
  val rollingRepair = not(state.newHistoryEstablished) and state.replayOccurred
  val secondRestart = state.newHistoryCrashed and state.replayOccurred and state.activeReplicas == replicaIds
  val writesInBothEras = state.historiesWithFreshWrite == Set(0, 1)
  val recoveredQuorum = state.newHistoryEstablished and state.activeReplicas.size() >= 2

  // 5. SCENARIOS: read each chain as a timeline, one event per line.
  // A bug test PASSES when its final expect confirms the intended violation.

  // Replay the opening table: operation 1 commits, but cutoff 3 hides it.
  run knownBugTest = init
    .then(crash(0))
    .then(crash(1))
    .then(crash(2))
    .then(restart(0))
    .then(restart(1))
    .then(establishEmpty(Set(0, 1)))
    .then(commitFresh(Set(0, 1)))
    .expect(reusedOperation and not(freshNotHidden) and noResurrection)

  // Erasing cutoff 3 lets replica 0 replay a surviving peer's PURGED entries.
  run clearResurrectsTest = initClear
    .then(crash(0))
    .then(restart(0))
    .then(adopt(1, 0))
    .expect(not(noResurrection) and state.replicas.get(0).purgedBookmarkRestored)

  // Setting the head to 3 creates a claim with no supporting journal or checkpoint.
  run raiseInventsHistoryTest = initRaise
    .then(crash(0))
    .then(restart(0))
    .expect(not(noInventedHistory))

  // Replica 2 joins late: min(saved cutoff 3, donor head 1) = 1 still hides op1.
  // This counterexample concerns replay, not installation of a materialized snapshot.
  run clampDelayedReplicaTest = initClamp
    .then(crash(0))
    .then(crash(1))
    .then(restart(0))
    .then(restart(1))
    .then(establishEmpty(Set(0, 1)))
    .then(commitFresh(Set(0, 1)))
    .then(adopt(0, 2))
    .expect(delayedJoin and not(freshNotHidden) and state.replicas.get(2).applyCutoff == 1)

  // The first fresh write is visible with cutoff 0 in memory. Another restart
  // reloads the unchanged saved cutoff 3; replay then hides that same write.
  run volatileRevertsTest = initVolatile
    .then(crash(0))
    .then(crash(1))
    .then(restart(0))
    .then(restart(1))
    .then(establishEmpty(Set(0, 1)))
    .then(commitFresh(Set(0, 1)))
    .expect(freshNotHidden)
    .then(crash(0))
    .then(restart(0))
    .then(adopt(1, 0))
    .expect(not(freshNotHidden) and state.replicas.get(0).applyCutoff == 3)

  // Given an authoritative new history, save its cutoff 0 before serving it.
  // Both a delayed replica and a replica that restarts again can expose op1.
  run certifiedRecoveryTest = initCertified
    .then(crash(0))
    .then(crash(1))
    .then(restart(0))
    .then(restart(1))
    .then(establishEmpty(Set(0, 1)))
    .then(commitFresh(Set(0, 1)))
    .then(adopt(0, 2))
    .expect(safe and delayedJoin and freshAfterRestart)
    .then(crash(0))
    .then(restart(0))
    .then(adopt(1, 0))
    .expect(safe and secondRestart)

  // The finite write limit must not accidentally forbid writes after recovery.
  // Deliberately lose every copy of a write in history 0, then write in history 1.
  run writesBeforeAndAfterLossTest = initCertified
    .then(commitFresh(Set(0, 1)))
    .then(crash(0))
    .then(crash(1))
    .then(crash(2))
    .then(restart(0))
    .then(restart(1))
    .then(establishEmpty(Set(0, 1)))
    .then(commitFresh(Set(0, 1)))
    .expect(safe and state.historiesWithFreshWrite == Set(0, 1) and freshAfterRestart)

  // A rolling restart retains history 0 and cutoff 3; the next fresh op is 4.
  run certifiedRollingRepairTest = initCertified
    .then(crash(0))
    .then(restart(0))
    .then(adopt(1, 0))
    .then(commitFresh(Set(0, 1)))
    .expect(safe and rollingRepair and state.replicas.get(0).lastOperation == 4)
}
