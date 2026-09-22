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

//! Canary so fixture-backed suites cannot go green-but-empty when CI requires fixtures.

#[path = "common/fixtures.rs"]
mod fixtures;

use fixtures::{FIXTURE_SKIP_HINT, any_wire_fixture_present, fixtures_dir};

#[test]
fn kafka_wire_fixtures_canary() {
    let fixtures_required =
        std::env::var("KAFKA_FIXTURES_REQUIRED").is_ok_and(|value| value == "1");
    let present = any_wire_fixture_present();

    if fixtures_required {
        assert!(
            present,
            "KAFKA_FIXTURES_REQUIRED=1 but no .bin fixtures under {:?}; {FIXTURE_SKIP_HINT}",
            fixtures_dir()
        );
        return;
    }

    if !present {
        eprintln!(
            "note: no wire fixtures under {:?}; fixture-backed suites will skip ({FIXTURE_SKIP_HINT})",
            fixtures_dir()
        );
    }
}
