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

use super::{LaneSpec, OriginScope, RateModel, TargetNamespace};
use crate::ops::OpKind;

pub fn lane(label: &'static str) -> LaneBuilder {
    LaneBuilder {
        label,
        ops: Vec::new(),
        workers: None,
        scope: OriginScope::Any,
        namespace: TargetNamespace::Both,
        rate: RateModel::Unbounded,
        destructive: false,
    }
}

pub struct LaneBuilder {
    label: &'static str,
    ops: Vec<(OpKind, f64)>,
    workers: Option<u32>,
    scope: OriginScope,
    namespace: TargetNamespace,
    rate: RateModel,
    destructive: bool,
}

impl LaneBuilder {
    pub fn ops(mut self, slice: &[(OpKind, f64)]) -> Self {
        self.ops = slice.to_vec();
        self
    }

    pub fn op(mut self, kind: OpKind, weight: f64) -> Self {
        self.ops.push((kind, weight));
        self
    }

    pub fn workers(mut self, n: u32) -> Self {
        self.workers = Some(n);
        self
    }

    pub fn scope(mut self, s: OriginScope) -> Self {
        self.scope = s;
        self
    }

    pub fn namespace(mut self, ns: TargetNamespace) -> Self {
        self.namespace = ns;
        self
    }

    pub fn rate(mut self, r: RateModel) -> Self {
        self.rate = r;
        self
    }

    pub fn destructive(mut self) -> Self {
        self.destructive = true;
        self
    }

    pub fn build(self) -> LaneSpec {
        LaneSpec {
            label: self.label,
            op_weights: self.ops,
            worker_count: self.workers,
            scope: self.scope,
            namespace: self.namespace,
            rate: self.rate,
            allow_destructive: self.destructive,
        }
    }
}
