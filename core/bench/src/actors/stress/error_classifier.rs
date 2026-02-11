/* Licensed to the Apache Software Foundation (ASF) under one
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

use iggy::prelude::IggyError;
use std::sync::atomic::Ordering;

use super::stress_context::StressStats;

/// Three-tier error classification for stress test chaos tolerance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    /// Expected during chaos: races, stale references, already-exists conflicts.
    Expected,
    /// Not anticipated but non-fatal: server-side validation, capacity limits.
    Unexpected,
}

/// Classifies an `IggyError` into expected vs unexpected during a stress test.
///
/// During chaos (CRUD churn + concurrent data-plane), certain errors are normal:
/// - Resource-not-found errors when a churner deletes a topic another actor references
/// - Already-exists errors from concurrent create attempts
/// - Consumer group member-not-found during rebalance races
pub const fn classify(error: &IggyError) -> ErrorSeverity {
    match error {
        // Resource races, already-exists conflicts, user/PAT concurrency
        IggyError::StreamIdNotFound(_)
        | IggyError::TopicIdNotFound(_, _)
        | IggyError::PartitionNotFound(_, _, _)
        | IggyError::ConsumerGroupIdNotFound(_, _)
        | IggyError::ConsumerGroupNameNotFound(_, _)
        | IggyError::ConsumerGroupMemberNotFound(_, _, _)
        | IggyError::ResourceNotFound(_)
        | IggyError::StreamNameAlreadyExists(_)
        | IggyError::TopicNameAlreadyExists(_, _)
        | IggyError::ConsumerGroupNameAlreadyExists(_, _)
        | IggyError::UserAlreadyExists
        | IggyError::PersonalAccessTokenAlreadyExists(_, _)
        | IggyError::InvalidPersonalAccessToken
        | IggyError::SegmentNotFound
        | IggyError::SegmentClosed(_, _) => ErrorSeverity::Expected,

        _ => ErrorSeverity::Unexpected,
    }
}

/// Records an error in the stress stats based on its severity.
pub fn record_error(stats: &StressStats, error: &IggyError) {
    match classify(error) {
        ErrorSeverity::Expected => {
            stats.expected_errors.fetch_add(1, Ordering::Relaxed);
        }
        ErrorSeverity::Unexpected => {
            stats.unexpected_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}
