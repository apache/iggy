// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The allowed skips and the known divergences of each SDK, each with a
//! tracking issue.
//!
//! A skip that the list does not allow fails the run, and so does a listed
//! skip or divergence whose test passes, so the list can only shrink. A
//! divergence is a test that fails because the SDK differs from the contract.
//! Its failure does not fail the run.

use crate::runner::{RunReport, Verdict};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SkipList {
    #[serde(default)]
    pub skip: Vec<AllowedSkip>,
    #[serde(default)]
    pub divergence: Vec<Divergence>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Divergence {
    pub sdk: String,
    /// The test id, `<file>#<description>`.
    pub test: String,
    pub issue: String,
    /// How the SDK differs from the contract.
    pub reason: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllowedSkip {
    pub sdk: String,
    /// The test id, `<file>#<description>`.
    pub test: String,
    pub issue: String,
}

impl SkipList {
    /// # Errors
    ///
    /// Fails when the file cannot be read or parsed.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        toml::from_str(&text).map_err(|error| format!("{}: {error}", path.display()))
    }

    /// The failed tests of `report` that the list names as divergences.
    #[must_use]
    pub fn known_failures(&self, report: &RunReport) -> Vec<&Divergence> {
        report
            .tests
            .iter()
            .filter(|test| matches!(test.verdict, Verdict::Failed(_)))
            .filter_map(|test| {
                self.divergence
                    .iter()
                    .find(|divergence| divergence.sdk == report.sdk && divergence.test == test.id)
            })
            .collect()
    }

    /// Every way in which `report` breaks the list. A run with a test filter
    /// does not check for listed tests that it did not run.
    #[must_use]
    pub fn violations(&self, report: &RunReport, filtered: bool) -> Vec<String> {
        let listed: Vec<&AllowedSkip> = self
            .skip
            .iter()
            .filter(|skip| skip.sdk == report.sdk)
            .collect();
        let diverging: Vec<&Divergence> = self
            .divergence
            .iter()
            .filter(|divergence| divergence.sdk == report.sdk)
            .collect();
        let mut violations = Vec::new();
        for test in &report.tests {
            if matches!(test.verdict, Verdict::Passed)
                && diverging
                    .iter()
                    .any(|divergence| divergence.test == test.id)
            {
                violations.push(format!(
                    "{} passes {}; remove its divergence from conformance/skips.toml",
                    report.sdk, test.id
                ));
            }
            let allowed = listed.iter().any(|skip| skip.test == test.id);
            match (&test.verdict, allowed) {
                (Verdict::Skipped(reason), false) => violations.push(format!(
                    "{} skips {}, which conformance/skips.toml does not allow: {reason}",
                    report.sdk, test.id
                )),
                (Verdict::Passed, true) => violations.push(format!(
                    "{} passes {}; remove its entry from conformance/skips.toml",
                    report.sdk, test.id
                )),
                _ => {}
            }
        }
        if !filtered {
            let named = listed
                .iter()
                .map(|skip| skip.test.as_str())
                .chain(diverging.iter().map(|divergence| divergence.test.as_str()));
            for test_id in named {
                if !report.tests.iter().any(|test| test.id == test_id) {
                    violations.push(format!(
                        "conformance/skips.toml lists {test_id} for {}, which no test has",
                        report.sdk
                    ));
                }
            }
        }
        violations
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::Transport;
    use crate::runner::TestReport;

    fn run(verdict: Verdict) -> RunReport {
        RunReport {
            sdk: "swift".to_owned(),
            sdk_version: "1".to_owned(),
            transport: Transport::Tcp,
            tests: vec![TestReport {
                id: "f.yaml#t".to_owned(),
                file: "f.yaml".to_owned(),
                description: "t".to_owned(),
                verdict,
                duration_ms: 0,
            }],
        }
    }

    fn list() -> SkipList {
        toml::from_str("[[skip]]\nsdk = \"swift\"\ntest = \"f.yaml#t\"\nissue = \"#1\"\n").unwrap()
    }

    #[test]
    fn given_an_unlisted_skip_when_checking_should_report_it() {
        let violations =
            SkipList::default().violations(&run(Verdict::Skipped("x".to_owned())), false);
        assert_eq!(violations.len(), 1);
    }

    #[test]
    fn given_a_listed_skip_that_passes_when_checking_should_ask_to_remove_it() {
        let violations = list().violations(&run(Verdict::Passed), false);
        assert!(violations[0].contains("remove"), "{violations:?}");
    }

    #[test]
    fn given_a_listed_divergence_when_its_test_fails_should_count_it_as_known() {
        let list: SkipList = toml::from_str(
            "[[divergence]]\nsdk = \"swift\"\ntest = \"f.yaml#t\"\nissue = \"#1\"\nreason = \"r\"\n",
        )
        .unwrap();
        let failed = run(Verdict::Failed("x".to_owned()));
        assert_eq!(list.known_failures(&failed).len(), 1);
        assert!(list.violations(&failed, false).is_empty());
        assert_eq!(list.violations(&run(Verdict::Passed), false).len(), 1);
    }

    #[test]
    fn given_a_listed_skip_when_it_skips_should_pass() {
        assert!(
            list()
                .violations(&run(Verdict::Skipped("x".to_owned())), false)
                .is_empty()
        );
    }
}
