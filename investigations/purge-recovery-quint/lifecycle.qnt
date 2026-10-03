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

// Read lifecycle first: purge the old message, allow a fresh durable append,
// finish bookmark cleanup, and crash between those stages. lifecycle_transfer
// below asks a separate question: can replacing history leave a stale cutoff?
//
// Quint reading key: all { ... } requires every condition and assignment;
// any { ... } chooses one enabled action. state is now; state' is the next state.
// { ...state, field: value } keeps all other fields. implies means "if ... then".
// A run chains actions with .then(...) and checks a state with .expect(...).

// Scope: one purge, old message 1, fresh message 2, and two bookmark directories.
// These are message identities, not operation numbers. Number reuse, elections,
// real filesystem calls, and fresh bookmark contents are outside this module.
// Each action is atomic here; crashes can occur between actions. Durability is
// represented explicitly, rather than inferred from an unlink in live state.
module lifecycle {
  type State = {
    // Live phase: normal -> resetting -> cleanup -> done. A crash enters down.
    phase: str,
    // Durable markers select the recovery phase, independently of live state.
    durableResetPublished: bool,
    durableCleanupComplete: bool,
    durableMessages: Set[int],
    liveMessages: Set[int],
    // A directory in either set means it still contains an old bookmark.
    // Unlink changes liveBookmarks; directory sync changes durableBookmarks.
    durableBookmarks: Set[str],
    liveBookmarks: Set[str],
    // Model memory: records that appendFresh succeeded, even across a crash,
    // so retryPreservesFresh can detect later loss of that durable message.
    freshDurableAppendAccepted: bool,
    // Live offset bookkeeping is lost on crash; no offset file is modeled.
    freshOffsetPending: bool,
    freshOffsetApplied: bool,
    // Model memory for reachability checks; this is not a production disk field.
    crashObserved: bool
  }
  var state: State
  pure val directories = Set("consumer", "group")

  action init = state' = {
    phase: "normal",
    durableResetPublished: false,
    durableCleanupComplete: false,
    durableMessages: Set(1),
    liveMessages: Set(1),
    durableBookmarks: directories,
    liveBookmarks: directories,
    freshDurableAppendAccepted: false,
    freshOffsetPending: false,
    freshOffsetApplied: false,
    crashObserved: false
  }

  // Assumption: the partition write lock excludes fresh writes while resetting.
  // Clearing live state alone does not mean the deletion survives a crash.
  action beginPurge = all {
    state.phase == "normal",
    state' = {
      ...state,
      phase: "resetting",
      liveMessages: Set()
    }
  }

  action syncMessageDirectory = all {
    state.phase == "resetting",
    state' = { ...state, durableMessages: Set() }
  }

  // Only durable message deletion permits publishing the reset. Fresh writes
  // become possible in cleanup, while the old bookmarks are still being removed.
  action publishReset = all {
    state.phase == "resetting",
    state.durableMessages == Set(),
    state' = {
      ...state,
      phase: "cleanup",
      durableResetPublished: true
    }
  }

  action unlinkBookmark(directory: str): bool = all {
    state.phase == "cleanup",
    state' = {
      ...state,
      liveBookmarks: state.liveBookmarks.exclude(Set(directory))
    }
  }

  // Sync makes an earlier unlink durable for this directory only.
  action syncBookmarkDirectory(directory: str): bool = all {
    state.phase == "cleanup",
    not(state.liveBookmarks.contains(directory)),
    state' = {
      ...state,
      durableBookmarks: state.durableBookmarks.exclude(Set(directory))
    }
  }

  // The completion marker promises both directories have been synced.
  action publishComplete = all {
    state.phase == "cleanup",
    state.durableBookmarks == Set(),
    state' = {
      ...state,
      phase: "done",
      durableCleanupComplete: true
    }
  }

  // Retrying cleanup must not repeat the destructive message reset.
  action retryCleanup = all {
    state.phase == "cleanup",
    state' = state
  }

  // This module deliberately grants the fresh append disk durability in one
  // action. The transfer module below uses a weaker, memory replication case.
  action appendFresh = all {
    Set("cleanup", "done").contains(state.phase),
    not(state.freshDurableAppendAccepted),
    state' = {
      ...state,
      liveMessages: state.liveMessages.union(Set(2)),
      durableMessages: state.durableMessages.union(Set(2)),
      freshDurableAppendAccepted: true
    }
  }

  action queueFreshOffset = all {
    state.phase == "cleanup",
    state.freshDurableAppendAccepted,
    state' = { ...state, freshOffsetPending: true }
  }

  // Offset progress waits until old bookmark cleanup can no longer erase it.
  action applyFreshOffset = all {
    state.phase == "done",
    state.freshOffsetPending,
    state' = {
      ...state,
      freshOffsetPending: false,
      freshOffsetApplied: true
    }
  }

  // Clear live state and retain durable fields plus the model's observations.
  action crash = all {
    state.phase != "down",
    state' = {
      ...state,
      phase: "down",
      liveMessages: Set(),
      liveBookmarks: Set(),
      freshOffsetPending: false,
      freshOffsetApplied: false,
      crashObserved: true
    }
  }

  // Completion wins over reset; without either marker a purge may start again.
  // Unsynced bookmark deletions disappear when live state is loaded from disk.
  action recover = all {
    state.phase == "down",
    state' = {
      ...state,
      phase: if (state.durableCleanupComplete) "done"
        else if (state.durableResetPublished) "cleanup"
        else "normal",
      liveMessages: state.durableMessages,
      liveBookmarks: state.durableBookmarks
    }
  }

  // The checker chooses any enabled action, including a crash or doing nothing.
  // There is no fairness condition: safe does not promise eventual completion.
  action step = any {
    beginPurge,
    syncMessageDirectory,
    publishReset,
    unlinkBookmark("consumer"),
    unlinkBookmark("group"),
    syncBookmarkDirectory("consumer"),
    syncBookmarkDirectory("group"),
    publishComplete,
    retryCleanup,
    appendFresh,
    queueFreshOffset,
    applyFreshOffset,
    crash,
    recover,
    state' = state
  }

  // Each property describes one promise. An implication constrains the state
  // only when its left side holds; it does not require reaching that state.
  val completionIsDurable = state.durableCleanupComplete
    implies state.durableBookmarks == Set()
  val purgedMessagesStayGone = state.durableResetPublished
    implies not(state.durableMessages.contains(1))
  val retryPreservesFresh = state.freshDurableAppendAccepted
    implies state.durableMessages.contains(2)
  val noOldBookmarkLoads = state.phase == "done"
    implies state.liveBookmarks == Set()
  val offsetWaitsForCleanup = state.freshOffsetApplied
    implies state.durableCleanupComplete
  val safe = completionIsDurable
    and purgedMessagesStayGone
    and retryPreservesFresh
    and noOldBookmarkLoads
    and offsetWaitsForCleanup

  // Reachability observations, not extra promises in safe.
  val partialCleanup = state.durableResetPublished
    and not(state.durableCleanupComplete)
    and state.durableBookmarks.size() == 1
  val completedAfterCrash = state.crashObserved and state.phase == "done"
  val freshDuringCleanup = state.freshDurableAppendAccepted and state.phase == "cleanup"
  val offsetProgress = state.freshOffsetApplied

  // Deliberate bugs, excluded from step. Their tests must observe a violated
  // property: a passing mutation test means the expected failure was detected.
  // First bug: publish completion without waiting for bookmark directory syncs.
  action prematureComplete = all {
    state.phase == "cleanup",
    state' = {
      ...state,
      phase: "done",
      durableCleanupComplete: true
    }
  }

  // Second bug: reset messages again while retrying bookmark cleanup.
  action destructiveRetry = all {
    state.phase == "cleanup",
    state' = {
      ...state,
      liveMessages: Set(),
      durableMessages: Set()
    }
  }
  action earlyCompletionStep = any { step, prematureComplete }
  action destructiveRetryStep = any { step, destructiveRetry }

  // Read this run as the successful lifecycle: append 2 during cleanup,
  // finish both directories, then apply the waiting offset.
  run retryPreservesFreshTest = init
    .then(beginPurge)
    .then(syncMessageDirectory)
    .then(publishReset)
    .then(appendFresh)
    .then(queueFreshOffset)
    .then(retryCleanup)
    .then(unlinkBookmark("consumer"))
    .then(syncBookmarkDirectory("consumer"))
    .then(unlinkBookmark("group"))
    .then(syncBookmarkDirectory("group"))
    .then(publishComplete)
    .then(applyFreshOffset)
    .expect(safe and state.liveMessages == Set(2) and offsetProgress)

  // Worked crash: after both unlinks, liveBookmarks = {}, but durableBookmarks
  // = {"group"} because only consumer was synced. Recovery must resume cleanup
  // with {"group"}; message 2 stays durable throughout. Completion then becomes
  // safe once group is unlinked and synced again.
  run crashBetweenDirectorySyncsTest = init
    .then(beginPurge)
    .then(syncMessageDirectory)
    .then(publishReset)
    .then(appendFresh)
    .then(unlinkBookmark("consumer"))
    .then(syncBookmarkDirectory("consumer"))
    .then(unlinkBookmark("group"))
    .then(crash)
    .then(recover)
    .expect(safe and partialCleanup and state.liveBookmarks == Set("group"))
    .then(unlinkBookmark("group"))
    .then(syncBookmarkDirectory("group"))
    .then(publishComplete)
    .expect(safe and completedAfterCrash)

  run crashBeforeResetTest = init
    .then(beginPurge)
    .then(crash)
    .then(recover)
    .expect(state.phase == "normal" and not(state.durableResetPublished) and safe)
    .then(beginPurge)
    .then(syncMessageDirectory)
    .then(publishReset)
    .expect(safe and state.durableResetPublished)

  // Both unlinks were visible, but neither was durable: the false completion
  // lets recovery enter done with both old bookmarks, violating safe.
  run earlyCompletionMutationTest = init
    .then(beginPurge)
    .then(syncMessageDirectory)
    .then(publishReset)
    .then(unlinkBookmark("consumer"))
    .then(unlinkBookmark("group"))
    .then(prematureComplete)
    .then(crash)
    .then(recover)
    .expect(not(safe) and state.liveBookmarks == directories)

  run destructiveRetryMutationTest = init
    .then(beginPurge)
    .then(syncMessageDirectory)
    .then(publishReset)
    .then(appendFresh)
    .then(destructiveRetry)
    .expect(not(retryPreservesFresh))
}

// Scope: one already accepted transfer replaces old history. The old purge
// cutoff is 3; the incoming history commits through operation 1. A fresh commit
// is operation 2. The local committed head is also 1: operations 2 and 3 are
// a purged, uncommitted suffix. Thus the offer does not rewind committed history.
// The election and the authority of that offer are assumed.
//
// The two questions checked here are whether a crash restores the old history
// with its original cutoff, and whether fresh operation 2 is pollable afterward.
// Each stage is atomic; this is not a model of every write in a snapshot install.
// Incoming materialized messages are omitted. The fresh operation 2 has not been
// flushed: the poll check concerns the resident journal, without a disk fallback.
// A Rust regression must still establish the complete production polling result.
module lifecycle_transfer {
  type Image = {
    // "old" and "installed" label histories, not election terms or certificates.
    historyLabel: str,
    purgeCutoff: int
  }
  type State = {
    // ready -> backed -> rebased -> replaced -> ready; crash enters down.
    phase: str,
    // Durable: history and its purge marker must be restored as one image.
    durableImage: Image,
    rollbackImage: Image,
    rollbackImagePresent: bool,
    // Live: cutoff N rejects operations <= N. Both cutoffs must admit a poll.
    historyLabel: str,
    purgeCutoff: int,
    journalPollCutoff: int,
    // Observations of the current live commit; both are cleared on crash.
    freshMemoryCommit: bool,
    freshCommitVisible: bool,
    // Model memory used to identify a path that recovered after a crash.
    crashObserved: bool
  }
  var state: State
  pure val original = { historyLabel: "old", purgeCutoff: 3 }
  pure val incoming = { historyLabel: "installed", purgeCutoff: 1 }

  action init = state' = {
    phase: "ready",
    durableImage: original,
    rollbackImage: original,
    rollbackImagePresent: false,
    historyLabel: "old",
    purgeCutoff: 3,
    journalPollCutoff: 3,
    freshMemoryCommit: false,
    freshCommitVisible: false,
    crashObserved: false
  }

  // A durable rollback image must exist before changing the purge marker.
  action beginBackup = all {
    state.phase == "ready",
    state.historyLabel == "old",
    state' = {
      ...state,
      phase: "backed",
      rollbackImage: state.durableImage,
      rollbackImagePresent: true
    }
  }

  // This intermediate state pairs old history with the new cutoff. It is not
  // ready to serve; a crash here must select the rollback image on recovery.
  action rebaseReset = all {
    state.phase == "backed",
    state' = {
      ...state,
      phase: "rebased",
      durableImage: { ...state.durableImage, purgeCutoff: 1 },
      purgeCutoff: 1
    }
  }

  // false models clear_all retaining poll_floor at 3; true models resetting it
  // to the installed cutoff 1. Both variants install the same durable history.
  // In the Rust snapshot linked in the README,
  // state_transfer.rs calls journal.clear_all(). journal.rs retains its
  // poll_floor, while rebase_purge_reset_for_install lowers the partition cutoff.
  action replaceHistory(resetJournalPollCutoff: bool): bool = all {
    state.phase == "rebased",
    state' = {
      ...state,
      phase: "replaced",
      durableImage: incoming,
      historyLabel: "installed",
      journalPollCutoff: if (resetJournalPollCutoff) 1 else state.journalPollCutoff
    }
  }

  // Durable retirement of the rollback image commits the installed recovery choice.
  action finish = all {
    state.phase == "replaced",
    state' = {
      ...state,
      phase: "ready",
      rollbackImagePresent: false
    }
  }

  // Concrete failure: 2 > purgeCutoff(1) passes, but 2 > journalPollCutoff(3)
  // fails. Successful replication alone therefore does not imply pollability.
  action commitFresh = all {
    state.phase == "ready",
    state.historyLabel == "installed",
    not(state.freshMemoryCommit),
    state' = {
      ...state,
      freshMemoryCommit: true,
      freshCommitVisible: 2 > state.purgeCutoff and 2 > state.journalPollCutoff
    }
  }

  // The fresh commit is replicated in memory here, unlike appendFresh above.
  // Its survival is not promised across a crash. Durable images remain intact.
  action crash = all {
    state.phase != "down",
    state' = {
      ...state,
      phase: "down",
      historyLabel: "none",
      purgeCutoff: 0,
      journalPollCutoff: 0,
      freshMemoryCommit: false,
      freshCommitVisible: false,
      crashObserved: true
    }
  }

  // Rollback restores history and purge marker together; the live journal
  // starts with no poll cutoff (0). This abstracts actual replay and I/O.
  action recover = all {
    state.phase == "down",
    val recoveredImage = if (state.rollbackImagePresent) state.rollbackImage else state.durableImage
    state' = {
      ...state,
      phase: "ready",
      durableImage: recoveredImage,
      rollbackImagePresent: false,
      historyLabel: recoveredImage.historyLabel,
      purgeCutoff: recoveredImage.purgeCutoff,
      journalPollCutoff: 0
    }
  }

  // Identical choices except the argument to replaceHistory. currentStep
  // exposes the stale cutoff; step checks the candidate that resets it.
  action currentStep = any {
    beginBackup,
    rebaseReset,
    replaceHistory(false),
    finish,
    commitFresh,
    crash,
    recover,
    state' = state
  }
  action step = any {
    beginBackup,
    rebaseReset,
    replaceHistory(true),
    finish,
    commitFresh,
    crash,
    recover,
    state' = state
  }

  // Compare a recovered old history with 3, not the incoming history's cutoff 1.
  val oldHistoryRemainsFenced = (state.phase == "ready" and state.historyLabel == "old")
    implies state.purgeCutoff == 3
  val freshCommitIsPollable = state.freshMemoryCommit implies state.freshCommitVisible
  val safe = oldHistoryRemainsFenced and freshCommitIsPollable

  // Reachability observations; safe does not assert that transfer must finish.
  val rollbackReached = state.crashObserved and state.phase == "ready" and state.historyLabel == "old"
  val installReached = state.phase == "ready" and state.historyLabel == "installed"
  val freshReached = state.freshMemoryCommit and state.freshCommitVisible

  // Deliberate bug: permit rebasing without retaining the original image.
  // Excluded from step; missingBackupMutationTest expects the fence to fail.
  action omitBackup = all {
    state.phase == "ready",
    state.historyLabel == "old",
    state' = {
      ...state,
      phase: "backed",
      rollbackImagePresent: false
    }
  }
  action omitBackupStep = any { step, omitBackup }

  // Worked transfer: (purge cutoff, journal cutoff) starts at (3, 3), becomes
  // (1, 3), and stays there after replaceHistory(false). Operation 2 is hidden.
  // The test passes when it reproduces that failure of freshCommitIsPollable.
  run currentPollFloorBugTest = init
    .then(beginBackup)
    .then(rebaseReset)
    .then(replaceHistory(false))
    .then(finish)
    .then(commitFresh)
    .expect(not(freshCommitIsPollable) and state.purgeCutoff == 1 and state.journalPollCutoff == 3)

  // Same transfer with replaceHistory(true) ends at (1, 1), admitting op 2.
  run resetPollFloorTest = init
    .then(beginBackup)
    .then(rebaseReset)
    .then(replaceHistory(true))
    .then(finish)
    .then(commitFresh)
    .expect(safe and freshReached)

  // Crashes before finish roll back, even if the replacement was already done.
  run crashAfterRebaseTest = init
    .then(beginBackup)
    .then(rebaseReset)
    .then(crash)
    .then(recover)
    .expect(safe and rollbackReached and state.durableImage == original)

  run crashAfterReplacementTest = init
    .then(beginBackup)
    .then(rebaseReset)
    .then(replaceHistory(true))
    .then(crash)
    .then(recover)
    .expect(safe and rollbackReached and state.durableImage == original)

  // Once finish removed the rollback marker, recovery keeps installed history.
  run crashAfterFinishTest = init
    .then(beginBackup)
    .then(rebaseReset)
    .then(replaceHistory(true))
    .then(finish)
    .then(crash)
    .then(recover)
    .then(commitFresh)
    .expect(safe and freshReached and state.durableImage == incoming)

  // No rollback image: recovery serves old history with cutoff 1 instead of 3.
  run missingBackupMutationTest = init
    .then(omitBackup)
    .then(rebaseReset)
    .then(crash)
    .then(recover)
    .expect(not(oldHistoryRemainsFenced) and state.durableImage.historyLabel == "old" and state.durableImage.purgeCutoff == 1)
}
