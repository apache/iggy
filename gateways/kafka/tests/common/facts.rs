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

//! What a scenario observed, and how the gateway's observations are judged against an oracle's.
//!
//! A scenario records **facts**: named, already-normalised observations such as
//! `produce.first.base_offset = 0`. Normalisation is the choice of what to record: node ids,
//! hosts, cluster ids, throttle times and broker-chosen producer ids are never recorded, so they
//! cannot differ. Every fact the gateway and an oracle disagree on is a difference, and every
//! difference must be declared in `tests/oracle/allowed_differences.toml` with the issue that owns
//! it. A declared difference that no longer occurs is reported too, so the file cannot go stale.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt::{self, Display, Write as _};

use super::oracle::TargetKind;

/// Observations of one scenario on one target, keyed by fact name.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Facts(BTreeMap<String, String>);

impl Facts {
    pub fn set(&mut self, fact: impl Into<String>, value: impl Display) {
        self.0.insert(fact.into(), value.to_string());
    }

    #[must_use]
    pub fn get(&self, fact: &str) -> Option<&str> {
        self.0.get(fact).map(String::as_str)
    }

    #[must_use]
    pub const fn inner(&self) -> &BTreeMap<String, String> {
        &self.0
    }
}

/// A fact the gateway and one oracle disagree on. `None` means the target recorded no such fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Difference {
    pub scenario: String,
    pub fact: String,
    pub oracle: TargetKind,
    pub gateway: Option<String>,
    pub expected: Option<String>,
}

impl Display for Difference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}: gateway {} but {} {}",
            self.scenario,
            self.fact,
            self.gateway.as_deref().unwrap_or("<absent>"),
            self.oracle,
            self.expected.as_deref().unwrap_or("<absent>")
        )
    }
}

/// Every fact on which `gateway` and `oracle` disagree.
#[must_use]
pub fn compare(
    scenario: &str,
    gateway: &Facts,
    oracle_kind: TargetKind,
    oracle: &Facts,
) -> Vec<Difference> {
    let mut names: Vec<&String> = gateway.0.keys().chain(oracle.0.keys()).collect();
    names.sort();
    names.dedup();
    names
        .into_iter()
        .filter_map(|fact| {
            let ours = gateway.0.get(fact);
            let theirs = oracle.0.get(fact);
            (ours != theirs).then(|| Difference {
                scenario: scenario.to_string(),
                fact: fact.clone(),
                oracle: oracle_kind,
                gateway: ours.cloned(),
                expected: theirs.cloned(),
            })
        })
        .collect()
}

/// One declared, intended difference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allowed {
    pub scenario: String,
    pub fact: String,
    /// Oracles it applies to; empty means every oracle.
    pub oracles: Vec<String>,
    pub issue: String,
    pub reason: String,
}

impl Allowed {
    fn matches(&self, difference: &Difference) -> bool {
        self.scenario == difference.scenario
            && self.fact == difference.fact
            && self.applies_to(difference.oracle)
    }

    fn applies_to(&self, oracle: TargetKind) -> bool {
        self.oracles.is_empty() || self.oracles.iter().any(|name| name == oracle.name())
    }
}

/// Parses `allowed_differences.toml`: an array of `[[difference]]` tables with `scenario`,
/// `fact`, `issue`, `reason` and an optional `oracles` list.
pub fn parse_allowed(text: &str) -> Result<Vec<Allowed>, String> {
    let table: toml::Table = text
        .parse()
        .map_err(|error| format!("allowed differences: {error}"))?;
    let Some(entries) = table.get("difference") else {
        return Ok(Vec::new());
    };
    let entries = entries
        .as_array()
        .ok_or("allowed differences: `difference` must be an array of tables")?;
    entries
        .iter()
        .enumerate()
        .map(|(index, entry)| {
            let field = |name: &str| {
                entry
                    .get(name)
                    .and_then(toml::Value::as_str)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .ok_or_else(|| {
                        format!("allowed difference #{}: `{name}` is required", index + 1)
                    })
            };
            let oracles = match entry.get("oracles") {
                None => Vec::new(),
                Some(value) => value
                    .as_array()
                    .ok_or_else(|| {
                        format!(
                            "allowed difference #{}: `oracles` must be a list",
                            index + 1
                        )
                    })?
                    .iter()
                    .map(|name| {
                        name.as_str().map(str::to_string).ok_or_else(|| {
                            format!("allowed difference #{}: `oracles` holds strings", index + 1)
                        })
                    })
                    .collect::<Result<_, _>>()?,
            };
            Ok(Allowed {
                scenario: field("scenario")?,
                fact: field("fact")?,
                oracles,
                issue: field("issue")?,
                reason: field("reason")?,
            })
        })
        .collect()
}

/// The judgement of one run.
#[derive(Debug, Default)]
pub struct Verdict {
    /// Differences nobody declared. Each fails the run.
    pub unexpected: Vec<Difference>,
    /// Declared differences, with the issue that owns each.
    pub declared: Vec<(Difference, String)>,
    /// Declared differences for a scenario and oracle that ran, which did not occur. Each fails
    /// the run: the gap closed and its entry should go.
    pub stale: Vec<Allowed>,
}

impl Verdict {
    #[must_use]
    pub const fn passed(&self) -> bool {
        self.unexpected.is_empty() && self.stale.is_empty()
    }

    /// A report for the test output and the CI job summary.
    #[must_use]
    pub fn report(&self) -> String {
        let mut out = String::new();
        for difference in &self.unexpected {
            let _ = writeln!(out, "UNEXPECTED {difference}");
        }
        for allowed in &self.stale {
            let _ = writeln!(
                out,
                "STALE      {}/{} ({}): declared but not observed; remove it from \
                 allowed_differences.toml",
                allowed.scenario, allowed.fact, allowed.issue
            );
        }
        for (difference, issue) in &self.declared {
            let _ = writeln!(out, "declared   {difference} [{issue}]");
        }
        out
    }
}

/// Judges `differences` from the `(scenario, oracle)` pairs in `ran` against `allowed`.
#[must_use]
pub fn judge(
    differences: &[Difference],
    allowed: &[Allowed],
    ran: &[(String, TargetKind)],
) -> Verdict {
    let mut verdict = Verdict::default();
    for difference in differences {
        match allowed.iter().find(|entry| entry.matches(difference)) {
            Some(entry) => verdict
                .declared
                .push((difference.clone(), entry.issue.clone())),
            None => verdict.unexpected.push(difference.clone()),
        }
    }
    for entry in allowed {
        let observed_somewhere = ran
            .iter()
            .filter(|(scenario, oracle)| *scenario == entry.scenario && entry.applies_to(*oracle))
            .any(|(_, oracle)| {
                differences
                    .iter()
                    .any(|difference| difference.oracle == *oracle && entry.matches(difference))
            });
        let ran_here = ran
            .iter()
            .any(|(scenario, oracle)| *scenario == entry.scenario && entry.applies_to(*oracle));
        if ran_here && !observed_somewhere {
            verdict.stale.push(entry.clone());
        }
    }
    verdict
}
