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

use crate::{plotting::chart_kind::ChartKind, report::BenchmarkReport};

/// Returns a title for a benchmark report
impl BenchmarkReport {
    pub fn title(&self, kind: ChartKind) -> String {
        if let Some(remark) = &self.params.remark {
            format!(
                "{} - {} Benchmark ({})",
                kind, self.params.benchmark_kind, remark
            )
        } else {
            format!("{} - {} Benchmark", kind, self.params.benchmark_kind)
        }
    }
}
