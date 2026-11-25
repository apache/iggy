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

/// The releases that can share a cluster.
///
/// A release links with itself, with every release its row lists, and with
/// every release whose row lists it. Only the binary that holds a row knows
/// it, so a release needs no knowledge of the releases published after it.
/// A release absent from both sides of every pair links only with itself.
///
/// A row asserts that the listed release behaves as the row's release in
/// everything replicas share: the frames they exchange, the state transfer
/// formats (metadata snapshot, client table, state manifest, partition
/// transfer), the object store formats and the replicated logic. Replicas
/// that apply one operation differently diverge, and nothing at the handshake
/// can detect it. The handshake only refuses a listed release on another
/// client protocol, replica wire revision or storage format.
///
/// A release is matched by its exact version string. Nothing is inferred, so
/// every pair in a group of compatible releases must have an explicit entry.
/// A new table cannot revoke a pair already listed by a deployed binary.
///
/// A row reads `ReleaseCompatibility { release: "1.2.1", compatible_with: &["1.2.0"] }`.
pub const RELEASE_COMPATIBILITY: ReleaseCompatibilityTable = ReleaseCompatibilityTable(&[]);

pub struct ReleaseCompatibilityTable(&'static [ReleaseCompatibility]);

impl ReleaseCompatibilityTable {
    /// The releases that the row of `release` lists.
    pub fn compatible_with(&self, release: &str) -> &'static [&'static str] {
        self.0
            .iter()
            .find(|row| row.release == release)
            .map_or(&[], |row| row.compatible_with)
    }
}

pub struct ReleaseCompatibility {
    pub release: &'static str,
    pub compatible_with: &'static [&'static str],
}

#[cfg(test)]
mod tests {
    use super::*;
    use message_bus::replica::handshake::MAX_COMPATIBLE_RELEASES;
    use semver::Version;
    use std::collections::{BTreeMap, BTreeSet};

    const TABLE: ReleaseCompatibilityTable = ReleaseCompatibilityTable(&[
        ReleaseCompatibility {
            release: "1.2.1",
            compatible_with: &["1.2.0"],
        },
        ReleaseCompatibility {
            release: "1.2.2",
            compatible_with: &["1.2.0", "1.2.1"],
        },
    ]);

    #[test]
    fn given_a_release_with_a_row_when_looking_it_up_should_return_what_the_row_lists() {
        assert_eq!(TABLE.compatible_with("1.2.1"), ["1.2.0"]);
        assert_eq!(TABLE.compatible_with("1.2.2"), ["1.2.0", "1.2.1"]);
    }

    #[test]
    fn given_a_release_without_a_row_when_looking_it_up_should_return_nothing() {
        for release in ["1.2.0", "1.2.3", "1.2.1-fork.1", "1.2.1+build", "1.2"] {
            assert!(TABLE.compatible_with(release).is_empty(), "{release}");
        }
    }

    #[test]
    fn given_the_shipped_table_when_checking_its_rows_should_find_no_defect() {
        assert_eq!(first_defect(&RELEASE_COMPATIBILITY), None);
        assert_eq!(first_defect(&TABLE), None);
    }

    #[test]
    fn given_a_defective_row_when_checking_a_table_should_name_the_defect() {
        for (rows, defect) in [
            (
                &[
                    ReleaseCompatibility {
                        release: "1.2.1",
                        compatible_with: &["1.2.0"],
                    },
                    ReleaseCompatibility {
                        release: "1.2.1",
                        compatible_with: &[],
                    },
                ][..],
                "1.2.1 has two rows",
            ),
            (
                &[ReleaseCompatibility {
                    release: "1.2.1",
                    compatible_with: &["1.2.1"],
                }],
                "1.2.1 lists itself",
            ),
            (
                &[ReleaseCompatibility {
                    release: "1.2.1",
                    compatible_with: &["1.2.0", "1.2.0"],
                }],
                "1.2.1 lists 1.2.0 twice",
            ),
            (
                &[ReleaseCompatibility {
                    release: "1.2.1",
                    compatible_with: &["1.2"],
                }],
                "1.2 is not a version",
            ),
            (
                &[
                    ReleaseCompatibility {
                        release: "1.2.1",
                        compatible_with: &["1.2.0"],
                    },
                    ReleaseCompatibility {
                        release: "1.2.2",
                        compatible_with: &["1.2.0"],
                    },
                ],
                "1.2.1 and 1.2.2 both link with 1.2.0 but cannot link with each other",
            ),
            (
                &[
                    ReleaseCompatibility {
                        release: "1.2.1",
                        compatible_with: &["1.2.0"],
                    },
                    ReleaseCompatibility {
                        release: "1.2.2",
                        compatible_with: &["1.2.1"],
                    },
                ],
                "1.2.0 and 1.2.2 both link with 1.2.1 but cannot link with each other",
            ),
            (
                &[ReleaseCompatibility {
                    release: "1.2.2",
                    compatible_with: &["1.2.0", "1.2.1"],
                }],
                "1.2.0 and 1.2.1 both link with 1.2.2 but cannot link with each other",
            ),
            (
                &[ReleaseCompatibility {
                    release: "1.2.3.4",
                    compatible_with: &[],
                }],
                "1.2.3.4 is not a version",
            ),
            (
                &[ReleaseCompatibility {
                    release: "1.2.0",
                    compatible_with: &["1.0.0"; MAX_COMPATIBLE_RELEASES + 1],
                }],
                "1.2.0 exceeds the replica hello limit",
            ),
            (
                &[ReleaseCompatibility {
                    release: "latest",
                    compatible_with: &[],
                }],
                "latest is not a version",
            ),
        ] {
            assert_eq!(
                first_defect(&ReleaseCompatibilityTable(rows)).as_deref(),
                Some(defect)
            );
        }
    }

    fn first_defect(table: &ReleaseCompatibilityTable) -> Option<String> {
        let mut neighbors = BTreeMap::<&str, BTreeSet<&str>>::new();
        for (index, row) in table.0.iter().enumerate() {
            let release = row.release;
            if Version::parse(release).is_err() {
                return Some(format!("{release} is not a version"));
            }
            if table.0[..index]
                .iter()
                .any(|other| other.release == release)
            {
                return Some(format!("{release} has two rows"));
            }
            if row.compatible_with.len() > MAX_COMPATIBLE_RELEASES {
                return Some(format!("{release} exceeds the replica hello limit"));
            }
            for (position, &listed) in row.compatible_with.iter().enumerate() {
                if Version::parse(listed).is_err() {
                    return Some(format!("{listed} is not a version"));
                }
                if listed == release {
                    return Some(format!("{release} lists itself"));
                }
                if row.compatible_with[..position].contains(&listed) {
                    return Some(format!("{release} lists {listed} twice"));
                }
                neighbors.entry(release).or_default().insert(listed);
                neighbors.entry(listed).or_default().insert(release);
            }
        }
        for (release, peers) in &neighbors {
            for (index, first) in peers.iter().enumerate() {
                for second in peers.iter().skip(index + 1) {
                    if !neighbors[first].contains(second) {
                        return Some(format!(
                            "{first} and {second} both link with {release} but cannot link with each other"
                        ));
                    }
                }
            }
        }
        None
    }
}
