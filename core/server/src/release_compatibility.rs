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
/// Replicas with matching build identities can link. For two stable releases with different identities, either server's row must list the other release. Every pair in a compatible group needs a direct entry.
///
/// Before listing a pair, verify both directions of replica traffic, state transfer (metadata snapshots, client tables, manifests and partition transfers), object storage formats, replicated operations and the HTTP relay between nodes, including route headers, relay proofs and the metadata floor endpoint. Check defaults that affect peers, such as the metadata journal capacity. When defaults change, pin a common supported value across the cluster before the first replacement and change it only after the last node rejoins. For a larger metadata journal, increase the backups first and the metadata primary last. If the releases cannot interoperate under that configuration, omit the pair.
///
/// At each stable release, freeze the row (including an empty one), client protocol, replica wire revision, storage marker and metadata transfer formats in the test's published release records. Use the version in `core/server/Cargo.toml` at the release commit. Never add, edit or remove a row retroactively. Carry published records and rows from release branches into master and reconcile sibling branches before publishing another pair.
///
/// For example, `ReleaseCompatibility { release: "1.2.1", compatible_with: &["1.2.0"] }` lets the two releases link in either direction.
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

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Formats {
        client_protocol: u32,
        replica_wire_revision: u32,
        storage_marker: &'static [u8],
        min_readable_snapshot_version: u32,
        snapshot_version: u32,
        client_table_magic: [u8; 4],
        state_manifest_magic: [u8; 4],
    }

    const BUILD_FORMATS: Formats = Formats {
        client_protocol: iggy_binary_protocol::IGGY_PROTOCOL_VERSION,
        replica_wire_revision: iggy_binary_protocol::REPLICA_WIRE_REVISION,
        storage_marker: crate::boot::STORAGE_FORMAT,
        min_readable_snapshot_version:
            metadata::stm::snapshot::MIN_READABLE_SNAPSHOT_FORMAT_VERSION,
        snapshot_version: metadata::stm::snapshot::SNAPSHOT_FORMAT_VERSION,
        client_table_magic: consensus::client_table::CLIENT_TABLE_MAGIC,
        state_manifest_magic: consensus::state_manifest::STATE_MANIFEST_MAGIC,
    };

    #[derive(Clone, Copy)]
    struct PublishedRelease {
        version: &'static str,
        listed: &'static [&'static str],
        formats: Formats,
    }

    // Record literal values from each stable release starting with the first that supports compatibility rows.
    const PUBLISHED: &[PublishedRelease] = &[];

    const RELEASE_FORMATS: Formats = Formats {
        client_protocol: iggy_binary_protocol::version::pack_protocol_version(0, 12, 3),
        replica_wire_revision: 5,
        storage_marker: b"IGGY-TIERED-SEGMENT-CHAIN-2\n",
        min_readable_snapshot_version: 21,
        snapshot_version: 21,
        client_table_magic: *b"ICT6",
        state_manifest_magic: *b"ISM1",
    };
    const RELEASES: [PublishedRelease; 3] = [
        PublishedRelease {
            version: "1.2.0",
            listed: &[],
            formats: RELEASE_FORMATS,
        },
        PublishedRelease {
            version: "1.2.1",
            listed: &["1.2.0"],
            formats: RELEASE_FORMATS,
        },
        PublishedRelease {
            version: "1.2.2",
            listed: &["1.2.0", "1.2.1"],
            formats: RELEASE_FORMATS,
        },
    ];

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
        assert_eq!(first_defect(RELEASE_COMPATIBILITY.0), None);
        assert_eq!(first_defect(TABLE.0), None);
        assert_eq!(
            published_defect(
                &RELEASE_COMPATIBILITY,
                PUBLISHED,
                crate::VERSION,
                BUILD_FORMATS
            ),
            None
        );
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
                "1.2 is not a stable release",
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
                "1.2.3.4 is not a stable release",
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
                "latest is not a stable release",
            ),
        ] {
            assert_eq!(first_defect(rows).as_deref(), Some(defect));
        }
    }

    #[test]
    fn edge_and_build_versions_are_rejected_on_either_side_of_a_pair() {
        for (version, listed, invalid) in [
            ("1.2.1-edge.2", &["1.2.0"][..], "1.2.1-edge.2"),
            ("1.2.1+build", &["1.2.0"][..], "1.2.1+build"),
            ("1.2.0", &["1.2.1-edge.2"][..], "1.2.1-edge.2"),
            ("1.2.0", &["1.2.1+build"][..], "1.2.1+build"),
        ] {
            let rows = [ReleaseCompatibility {
                release: version,
                compatible_with: listed,
            }];
            assert_eq!(
                first_defect(&rows),
                Some(format!("{invalid} is not a stable release"))
            );
        }
    }

    #[test]
    fn metadata_transfer_formats_match_replica_wire_revision() {
        assert_eq!(
            (
                iggy_binary_protocol::REPLICA_WIRE_REVISION,
                metadata::stm::snapshot::MIN_READABLE_SNAPSHOT_FORMAT_VERSION,
                metadata::stm::snapshot::SNAPSHOT_FORMAT_VERSION,
                consensus::client_table::CLIENT_TABLE_MAGIC,
                consensus::state_manifest::STATE_MANIFEST_MAGIC
            ),
            (5, 21, 21, *b"ICT6", *b"ISM1")
        );
    }

    #[test]
    fn published_pairs_require_the_original_rows() {
        let mut records = RELEASES;
        let check = |records: &[PublishedRelease]| {
            published_defect(&TABLE, records, "1.2.2", RELEASE_FORMATS)
        };
        assert_eq!(check(&records), None);
        assert!(
            check(&records[1..])
                .unwrap()
                .contains("1.2.0 has no release record")
        );
        assert!(
            check(&records[..2])
                .unwrap()
                .contains("1.2.2 needs a release record")
        );
        assert_eq!(
            check(&[records[0], records[2]]).as_deref(),
            Some("1.2.1 has no release record")
        );
        assert_eq!(
            check(&[records[0], records[0]]).as_deref(),
            Some("invalid or duplicate published release 1.2.0")
        );
        assert_eq!(
            check(&[PublishedRelease {
                version: "1.2.0-edge.1",
                ..records[0]
            }])
            .as_deref(),
            Some("invalid or duplicate published release 1.2.0-edge.1")
        );
        records[1].listed = &[];
        assert!(
            check(&records)
                .unwrap()
                .contains("1.2.1 differs from its published row")
        );
        records[1].listed = &["1.2.0"];
        let removed = ReleaseCompatibilityTable(&[]);
        assert!(
            published_defect(&removed, &records, "1.2.3-edge.1", RELEASE_FORMATS)
                .unwrap()
                .contains("1.2.1 differs from its published row")
        );
    }

    #[test]
    fn published_pairs_require_matching_formats() {
        let mut records = RELEASES;
        for formats in [
            Formats {
                client_protocol: RELEASE_FORMATS.client_protocol + 1,
                ..RELEASE_FORMATS
            },
            Formats {
                replica_wire_revision: RELEASE_FORMATS.replica_wire_revision + 1,
                ..RELEASE_FORMATS
            },
            Formats {
                storage_marker: b"older format",
                ..RELEASE_FORMATS
            },
            Formats {
                min_readable_snapshot_version: 20,
                ..RELEASE_FORMATS
            },
            Formats {
                snapshot_version: 22,
                ..RELEASE_FORMATS
            },
            Formats {
                client_table_magic: *b"ICT5",
                ..RELEASE_FORMATS
            },
            Formats {
                state_manifest_magic: *b"ISM2",
                ..RELEASE_FORMATS
            },
        ] {
            records[0].formats = formats;
            assert!(
                published_defect(&TABLE, &records, "1.2.2", RELEASE_FORMATS)
                    .unwrap()
                    .contains("1.2.1 and 1.2.0 have different")
            );
        }
    }

    fn stable(version: &str) -> bool {
        Version::parse(version)
            .is_ok_and(|version| version.pre.is_empty() && version.build.is_empty())
    }

    fn published_defect(
        table: &ReleaseCompatibilityTable,
        published: &[PublishedRelease],
        current: &str,
        formats: Formats,
    ) -> Option<String> {
        let mut by_version = BTreeMap::new();
        for release in published {
            let version = release.version;
            if !stable(version) || by_version.insert(version, release).is_some() {
                return Some(format!("invalid or duplicate published release {version}"));
            }
            if table.compatible_with(version) != release.listed {
                return Some(format!("{version} differs from its published row"));
            }
        }
        if stable(current)
            && by_version.get(current).map(|release| release.formats) != Some(formats)
        {
            return Some(format!(
                "{current} needs a release record matching its build"
            ));
        }
        for row in table.0 {
            let Some(release) = by_version.get(row.release) else {
                return Some(format!("{} has no release record", row.release));
            };
            for &listed in row.compatible_with {
                let Some(peer) = by_version.get(listed) else {
                    return Some(format!("{listed} has no release record"));
                };
                if peer.formats != release.formats {
                    return Some(format!(
                        "{} and {listed} have different protocol, storage or state transfer formats",
                        row.release
                    ));
                }
            }
        }
        None
    }

    fn first_defect(rows: &[ReleaseCompatibility]) -> Option<String> {
        let mut neighbors = BTreeMap::<&str, BTreeSet<&str>>::new();
        for (index, row) in rows.iter().enumerate() {
            let release = row.release;
            if !stable(release) {
                return Some(format!("{release} is not a stable release"));
            }
            if rows[..index].iter().any(|other| other.release == release) {
                return Some(format!("{release} has two rows"));
            }
            if row.compatible_with.len() > MAX_COMPATIBLE_RELEASES {
                return Some(format!("{release} exceeds the replica hello limit"));
            }
            for (position, &listed) in row.compatible_with.iter().enumerate() {
                if !stable(listed) {
                    return Some(format!("{listed} is not a stable release"));
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
