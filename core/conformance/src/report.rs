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

//! JUnit XML for CI, and the matrix of test, SDK and outcome.

use crate::runner::{RunReport, TestReport, Verdict};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

/// Counts of one run.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Totals {
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub errored: usize,
}

impl Totals {
    #[must_use]
    pub fn of(tests: &[TestReport]) -> Self {
        let mut totals = Self::default();
        for test in tests {
            match test.verdict {
                Verdict::Passed => totals.passed += 1,
                Verdict::Failed(_) => totals.failed += 1,
                Verdict::Skipped(_) => totals.skipped += 1,
                Verdict::Errored(_) => totals.errored += 1,
            }
        }
        totals
    }
}

/// One `testsuite` per test file.
#[must_use]
pub fn junit_xml(report: &RunReport) -> String {
    let mut files: BTreeMap<&str, Vec<&TestReport>> = BTreeMap::new();
    for test in &report.tests {
        files.entry(test.file.as_str()).or_default().push(test);
    }
    let totals = Totals::of(&report.tests);
    let mut xml = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    let _ = writeln!(
        xml,
        "<testsuites name=\"iggy-conformance {}\" tests=\"{}\" failures=\"{}\" errors=\"{}\" skipped=\"{}\">",
        escape(&report.sdk),
        report.tests.len(),
        totals.failed,
        totals.errored,
        totals.skipped
    );
    for (file, tests) in files {
        let owned: Vec<TestReport> = tests.iter().map(|test| (*test).clone()).collect();
        let file_totals = Totals::of(&owned);
        let _ = writeln!(
            xml,
            "  <testsuite name=\"{}/{}\" tests=\"{}\" failures=\"{}\" errors=\"{}\" skipped=\"{}\">",
            escape(&report.sdk),
            escape(file),
            tests.len(),
            file_totals.failed,
            file_totals.errored,
            file_totals.skipped
        );
        for test in tests {
            let seconds = test.duration_ms as f64 / 1000.0;
            let _ = write!(
                xml,
                "    <testcase classname=\"{}.{}\" name=\"{}\" time=\"{seconds:.3}\"",
                escape(&report.sdk),
                escape(file),
                escape(&test.description)
            );
            match &test.verdict {
                Verdict::Passed => xml.push_str("/>\n"),
                Verdict::Failed(reason) => {
                    let _ = writeln!(
                        xml,
                        ">\n      <failure message=\"{}\"/>\n    </testcase>",
                        escape(reason)
                    );
                }
                Verdict::Skipped(reason) => {
                    let _ = writeln!(
                        xml,
                        ">\n      <skipped message=\"{}\"/>\n    </testcase>",
                        escape(reason)
                    );
                }
                Verdict::Errored(reason) => {
                    let _ = writeln!(
                        xml,
                        ">\n      <error message=\"{}\"/>\n    </testcase>",
                        escape(reason)
                    );
                }
            }
        }
        xml.push_str("  </testsuite>\n");
    }
    xml.push_str("</testsuites>\n");
    xml
}

/// A Markdown table of every test against every SDK, then the reason of each
/// outcome that is not a pass.
#[must_use]
pub fn matrix_markdown(reports: &[RunReport]) -> String {
    let columns: Vec<String> = reports
        .iter()
        .map(|report| format!("{} ({:?})", report.sdk, report.transport).to_lowercase())
        .collect();
    let ids: BTreeSet<&str> = reports
        .iter()
        .flat_map(|report| report.tests.iter().map(|test| test.id.as_str()))
        .collect();
    let mut markdown = String::from("| Test |");
    for column in &columns {
        let _ = write!(markdown, " {column} |");
    }
    markdown.push_str("\n| --- |");
    for _ in &columns {
        markdown.push_str(" --- |");
    }
    markdown.push('\n');
    for id in &ids {
        let _ = write!(markdown, "| {} |", id.replace('|', "\\|"));
        for report in reports {
            let cell = report
                .tests
                .iter()
                .find(|test| test.id == *id)
                .map_or("not run", |test| verdict_word(&test.verdict));
            let _ = write!(markdown, " {cell} |");
        }
        markdown.push('\n');
    }
    let mut notes = String::new();
    for (column, report) in columns.iter().zip(reports) {
        for test in &report.tests {
            let reason = match &test.verdict {
                Verdict::Passed => continue,
                Verdict::Failed(reason) | Verdict::Skipped(reason) | Verdict::Errored(reason) => {
                    reason
                }
            };
            let _ = writeln!(
                notes,
                "- {column}, {}: {} ({reason})",
                test.id,
                verdict_word(&test.verdict)
            );
        }
    }
    if !notes.is_empty() {
        markdown.push('\n');
        markdown.push_str(&notes);
    }
    markdown
}

fn verdict_word(verdict: &Verdict) -> &'static str {
    match verdict {
        Verdict::Passed => "pass",
        Verdict::Failed(_) => "FAIL",
        Verdict::Skipped(_) => "skip",
        Verdict::Errored(_) => "ERROR",
    }
}

fn escape(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            '\n' => escaped.push_str("&#10;"),
            character if character.is_control() => {}
            character => escaped.push(character),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::Transport;

    fn report(sdk: &str, verdict: Verdict) -> RunReport {
        RunReport {
            sdk: sdk.to_owned(),
            sdk_version: "1".to_owned(),
            transport: Transport::Tcp,
            tests: vec![TestReport {
                id: "a.yaml#t".to_owned(),
                file: "a.yaml".to_owned(),
                description: "t".to_owned(),
                verdict,
                duration_ms: 1500,
            }],
        }
    }

    #[test]
    fn given_a_failure_when_writing_junit_should_escape_its_reason() {
        let xml = junit_xml(&report("go", Verdict::Failed("a <b> & \"c\"".to_owned())));
        assert!(xml.contains("failures=\"1\""), "{xml}");
        assert!(xml.contains("a &lt;b&gt; &amp; &quot;c&quot;"), "{xml}");
        assert!(xml.contains("time=\"1.500\""), "{xml}");
    }

    #[test]
    fn given_two_sdks_when_writing_the_matrix_should_add_a_column_each() {
        let markdown = matrix_markdown(&[
            report("rust", Verdict::Passed),
            report("go", Verdict::Skipped("no reader".to_owned())),
        ]);
        assert!(
            markdown.contains("| a.yaml#t | pass | skip |"),
            "{markdown}"
        );
        assert!(
            markdown.contains("go (tcp), a.yaml#t: skip (no reader)"),
            "{markdown}"
        );
    }
}
