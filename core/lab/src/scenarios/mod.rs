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

pub mod concurrent_crud;
pub mod mixed_workload;
pub mod segment_rotation;
pub mod stale_consumers;

use crate::args::{ScenarioName, Transport};
use crate::invariants::{self, InvariantViolation};
use crate::ops::OpKind;
use crate::shadow::ShadowState;
use async_trait::async_trait;
use iggy::prelude::*;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[allow(dead_code)]
pub enum NamespaceScope {
    #[default]
    Any,
    /// Only target resources created during setup (skip creative ops).
    SetupOnly,
    /// Only target dynamically-created resources.
    DynamicOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Namespace {
    Stable,
    Churn,
}

/// Controls which namespace a lane's operations target.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LaneNamespace {
    Stable,
    Churn,
    #[default]
    Both,
}

/// Derived stable/churn prefixes from a single base prefix.
#[derive(Debug, Clone)]
pub struct NamespacePrefixes {
    pub base: String,
    pub stable: String,
    pub churn: String,
}

impl NamespacePrefixes {
    pub fn from_base(base: &str) -> Self {
        Self {
            base: base.to_owned(),
            stable: format!("{base}s-"),
            churn: format!("{base}c-"),
        }
    }

    pub fn matches(&self, name: &str) -> bool {
        name.starts_with(&self.stable) || name.starts_with(&self.churn)
    }

    pub fn namespace_of(&self, stream_name: &str) -> Option<Namespace> {
        if stream_name.starts_with(&self.stable) {
            Some(Namespace::Stable)
        } else if stream_name.starts_with(&self.churn) {
            Some(Namespace::Churn)
        } else {
            None
        }
    }

    /// Prefix for creating new streams in the given lane namespace.
    /// `Both` creates in churn — stable resources are only created during setup.
    pub fn create_prefix(&self, ns: LaneNamespace) -> &str {
        match ns {
            LaneNamespace::Stable => &self.stable,
            LaneNamespace::Churn | LaneNamespace::Both => &self.churn,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub enum RateModel {
    #[default]
    Unbounded,
    /// Target N ops/sec per worker; sleep between ops to approximate.
    TargetOpsPerSec(u32),
}

/// One lane of worker execution with its own op weight distribution.
pub struct LaneSpec {
    pub label: &'static str,
    pub op_weights: Vec<(OpKind, f64)>,
    /// `None` = use the global `--workers` CLI value.
    pub worker_count: Option<u32>,
    pub scope: NamespaceScope,
    pub namespace: LaneNamespace,
    pub rate: RateModel,
    pub destructive: bool,
}

#[derive(Debug, Clone, Copy, Default)]
#[allow(dead_code)]
pub enum CleanupPolicy {
    #[default]
    DeletePrefix,
    NoOp,
}

/// Everything the default `verify()` implementation needs.
pub struct VerifyContext<'a> {
    pub client: &'a IggyClient,
    pub shadow: &'a ShadowState,
    pub prefixes: &'a NamespacePrefixes,
    pub server_address: &'a str,
    pub transport: Transport,
}

#[async_trait]
pub trait Scenario: Send + Sync {
    fn name(&self) -> &'static str;
    fn describe(&self) -> &'static str;

    /// Create pre-existing resources. Default: no-op.
    async fn setup(
        &self,
        _client: &IggyClient,
        _prefixes: &NamespacePrefixes,
    ) -> Result<(), IggyError> {
        Ok(())
    }

    /// Declare worker lanes (replaces op_weights).
    fn lanes(&self) -> Vec<LaneSpec>;

    /// Post-run verification. Default: generic shadow + cross-client checks.
    async fn verify(&self, ctx: &VerifyContext<'_>) -> Vec<InvariantViolation> {
        let mut violations =
            invariants::post_run_verify(ctx.client, ctx.shadow, ctx.prefixes).await;
        violations.extend(
            invariants::cross_client_consistency(ctx.server_address, ctx.transport, ctx.prefixes)
                .await,
        );
        violations
    }

    /// Cleanup strategy. Default: delete all prefixed streams.
    fn cleanup_policy(&self) -> CleanupPolicy {
        CleanupPolicy::DeletePrefix
    }
}

pub fn create_scenario(name: ScenarioName) -> Box<dyn Scenario> {
    match name {
        ScenarioName::ConcurrentCrud => Box::new(concurrent_crud::ConcurrentCrud),
        ScenarioName::SegmentRotation => Box::new(segment_rotation::SegmentRotation),
        ScenarioName::MixedWorkload => Box::new(mixed_workload::MixedWorkload),
        ScenarioName::StaleConsumers => Box::new(stale_consumers::StaleConsumers),
    }
}

pub fn list_scenarios() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            concurrent_crud::ConcurrentCrud.name(),
            concurrent_crud::ConcurrentCrud.describe(),
        ),
        (
            segment_rotation::SegmentRotation.name(),
            segment_rotation::SegmentRotation.describe(),
        ),
        (
            mixed_workload::MixedWorkload.name(),
            mixed_workload::MixedWorkload.describe(),
        ),
        (
            stale_consumers::StaleConsumers.name(),
            stale_consumers::StaleConsumers.describe(),
        ),
    ]
}
