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

//! The real OMB runs in `core/bench/dashboard/fixtures/omb`, one per directory, as the loader
//! reads them. That directory holds valid runs only, so the tests build the cases that the loader
//! must skip in temporary directories.

use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::setup::OMB_SETUP_FILE_NAME;
use std::fs;
use std::path::{Path, PathBuf};

pub(crate) const FIXTURE_OMB_RESULTS_DIR: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../fixtures/omb");

/// One run directory of the fixtures.
pub(crate) struct FixtureRun {
    pub(crate) dir: &'static str,
    pub(crate) file_name: &'static str,
    pub(crate) has_sidecar: bool,
}

/// 0.10.0-rc.1 on 3 nodes, 18 windows, publish P99 1.523 ms, with a sidecar.
pub(crate) const RC1_DEFAULT: FixtureRun = FixtureRun {
    dir: "rc1-12-topics-150mb",
    file_name: "12-topics-1-partition-1kb-150mb-Iggy-2026-11-06-13-26-21.json",
    has_sidecar: true,
};
/// The workload and setup of [`RC1_DEFAULT`] with the driver `Iggy-persisted`.
pub(crate) const RC1_PERSISTED: FixtureRun = FixtureRun {
    dir: "rc1-12-topics-150mb-persisted",
    file_name: "12-topics-1-partition-1kb-150mb-Iggy-persisted-2026-11-06-13-31-22.json",
    has_sidecar: true,
};
pub(crate) const EDGE8_DEFAULT: FixtureRun = FixtureRun {
    dir: "edge8-12-topics-150mb",
    file_name: "12-topics-1-partition-1kb-150mb-Iggy-2026-11-03-06-24-42.json",
    has_sidecar: true,
};
pub(crate) const EDGE8_100_PARTITIONS: FixtureRun = FixtureRun {
    dir: "edge8-i7i-100-partitions-125mb",
    file_name: "1-topic-100-partitions-1kb-125mb-Iggy-2026-11-02-14-55-18.json",
    has_sidecar: true,
};
/// No consumers and no sidecar. The run time comes from the file name.
pub(crate) const NO_CONSUMERS: FixtureRun = FixtureRun {
    dir: "edge-12-topics-75mb-no-consumers",
    file_name: "12-topics-1-partition-1kb-75mb-no-consumers-Iggy-2026-10-22-10-21-15.json",
    has_sidecar: false,
};
/// Publish errors in every window, no sidecar.
pub(crate) const PUBLISH_ERRORS: FixtureRun = FixtureRun {
    dir: "edge-12-topics-75mb-persisted-errors",
    file_name: "12-topics-1-partition-1kb-75mb-Iggy-persisted-2026-10-15-13-33-17.json",
    has_sidecar: false,
};

/// Every fixture run, newest first: the sidecar time where there is one, else the file name time.
pub(crate) const NEWEST_FIRST: [&FixtureRun; 6] = [
    &RC1_PERSISTED,
    &RC1_DEFAULT,
    &EDGE8_DEFAULT,
    &EDGE8_100_PARTITIONS,
    &NO_CONSUMERS,
    &PUBLISH_ERRORS,
];

impl FixtureRun {
    pub(crate) fn result_path(&self) -> PathBuf {
        self.dir_path().join(self.file_name)
    }

    pub(crate) fn sidecar_path(&self) -> PathBuf {
        self.dir_path().join(OMB_SETUP_FILE_NAME)
    }

    pub(crate) fn json(&self) -> Vec<u8> {
        fs::read(self.result_path()).expect("the fixture result is readable")
    }

    pub(crate) fn id(&self) -> OmbRunId {
        OmbRunId::of_file(&self.json())
    }

    /// Copies the run directory, with its sidecar if it has one, to `results_dir/run_dir`.
    pub(crate) fn copy_as(&self, results_dir: &Path, run_dir: &str) -> PathBuf {
        let target = results_dir.join(run_dir);
        fs::create_dir_all(&target).expect("the run directory is created");
        fs::copy(self.result_path(), target.join(self.file_name))
            .expect("the fixture result is copied");
        if self.has_sidecar {
            fs::copy(self.sidecar_path(), target.join(OMB_SETUP_FILE_NAME))
                .expect("the fixture sidecar is copied");
        }
        target
    }

    /// Copies the run directory to `results_dir` under its own name.
    pub(crate) fn copy_into(&self, results_dir: &Path) -> PathBuf {
        self.copy_as(results_dir, self.dir)
    }

    fn dir_path(&self) -> PathBuf {
        Path::new(FIXTURE_OMB_RESULTS_DIR).join(self.dir)
    }
}
