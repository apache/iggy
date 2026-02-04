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

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::fmt;

#[derive(Debug)]
pub enum SnapshotError {
    /// A required section is missing from the snapshot.
    MissingSection(&'static str),
    /// Serialization failed.
    Serialize(rmp_serde::encode::Error),
    /// Deserialization failed.
    Deserialize(rmp_serde::decode::Error),
    /// Slab ID mismatch during snapshot restore.
    SlabIdMismatch {
        section: &'static str,
        expected: usize,
        actual: usize,
    },
    /// Duplicate section name detected in snapshot.
    DuplicateSection(String),
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SnapshotError::MissingSection(name) => {
                write!(f, "missing snapshot section: {}", name)
            }
            SnapshotError::Serialize(e) => write!(f, "snapshot serialization failed: {}", e),
            SnapshotError::Deserialize(e) => write!(f, "snapshot deserialization failed: {}", e),
            SnapshotError::SlabIdMismatch {
                section,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "slab ID mismatch in section '{}': expected {}, got {}",
                    section, expected, actual
                )
            }
            SnapshotError::DuplicateSection(name) => {
                write!(f, "duplicate snapshot section name: {}", name)
            }
        }
    }
}

impl std::error::Error for SnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SnapshotError::Serialize(e) => Some(e),
            SnapshotError::Deserialize(e) => Some(e),
            _ => None,
        }
    }
}

/// The outer envelope for a complete metadata snapshot.
///
/// Contains metadata about the snapshot and a collection of sections,
/// where each section corresponds to one state machine's serialized state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEnvelope {
    /// Timestamp when the snapshot was created (microseconds since epoch).
    pub created_at: u64,
    /// The VSR commit number this snapshot corresponds to.
    /// All state in this snapshot reflects operations up to and including this commit.
    pub commit_number: u64,
    /// The individual state machine sections.
    pub sections: Vec<SnapshotSection>,
}

impl SnapshotEnvelope {
    /// Create a new snapshot envelope with the given commit number.
    pub fn new(commit_number: u64) -> Self {
        Self {
            created_at: iggy_common::IggyTimestamp::now().as_micros(),
            commit_number,
            sections: Vec::new(),
        }
    }

    /// Find a section by name.
    pub fn find_section(&self, name: &str) -> Option<&SnapshotSection> {
        self.sections.iter().find(|s| s.name == name)
    }

    /// Validate that no two sections share the same name.
    pub fn validate_no_duplicate_sections(&self) -> Result<(), SnapshotError> {
        let mut seen = std::collections::HashSet::new();
        for section in &self.sections {
            if !seen.insert(&section.name) {
                return Err(SnapshotError::DuplicateSection(section.name.clone()));
            }
        }
        Ok(())
    }

    /// Encode the envelope to bytes.
    pub fn encode(&self) -> Result<Vec<u8>, SnapshotError> {
        rmp_serde::to_vec(self).map_err(SnapshotError::Serialize)
    }

    /// Decode an envelope from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        rmp_serde::from_slice(bytes).map_err(SnapshotError::Deserialize)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotSection {
    /// The unique name identifying this section (e.g., "streams", "users").
    pub name: String,
    /// Encoded state data for this section.
    pub data: Vec<u8>,
}

impl SnapshotSection {
    /// Create a new section with the given name and data (version defaults to 1).
    pub fn new(name: impl Into<String>, data: Vec<u8>) -> Self {
        Self {
            name: name.into(),
            data,
        }
    }
}

/// Trait implemented by each `{Name}Inner` state machine to support snapshotting.
/// Each state machine defines its own snapshot
/// type for serialization and provides conversion methods.
pub trait Snapshotable {
    /// A unique, stable section name for this state machine.
    /// This name is used to identify the section in the snapshot envelope.
    const SECTION_NAME: &'static str;

    /// The serde-serializable snapshot representation of this state.
    /// This should be a plain struct with only serializable types and no wrappers
    /// like `Arc`, `AtomicUsize`, or other non-serializable wrappers.
    type Snapshot: Serialize + DeserializeOwned;

    /// Convert the current in-memory state into a serializable snapshot.
    fn to_snapshot(&self) -> Self::Snapshot;

    /// Restore in-memory state from a snapshot representation.
    fn from_snapshot(snapshot: Self::Snapshot) -> Result<Self, SnapshotError>
    where
        Self: Sized;

    /// Serialize the current state to a snapshot section.
    fn take_snapshot(&self) -> Result<SnapshotSection, SnapshotError> {
        let snap = self.to_snapshot();
        let data = rmp_serde::to_vec(&snap).map_err(SnapshotError::Serialize)?;
        Ok(SnapshotSection {
            name: Self::SECTION_NAME.to_owned(),
            data,
        })
    }

    /// Restore state from a snapshot section.
    fn apply_snapshot(section: &SnapshotSection) -> Result<Self, SnapshotError>
    where
        Self: Sized,
    {
        let snap: Self::Snapshot =
            rmp_serde::from_slice(&section.data).map_err(SnapshotError::Deserialize)?;
        Self::from_snapshot(snap)
    }
}

/// Trait for types that can contribute snapshot sections.
///
/// This is the visitor-pattern trait that the variadic tuple recursion implements,
/// walking through the tuple of state machines and collecting/restoring snapshot data.
pub trait SnapshotContributor {
    /// Collect snapshot sections from all constituent state machines.
    ///
    /// Each state machine in the composite appends its serialized section
    /// to the provided vector.
    fn collect_sections(&self, sections: &mut Vec<SnapshotSection>) -> Result<(), SnapshotError>;

    /// Restore all constituent state machines from sections.
    ///
    /// Returns the restored composite state. Each state machine finds its
    /// section by name and deserializes its state.
    fn restore_from_sections(sections: &[SnapshotSection]) -> Result<Self, SnapshotError>
    where
        Self: Sized;

    /// Return the list of section names this contributor knows about.
    ///
    /// Used to detect unknown sections during restore.
    fn known_section_names() -> Vec<&'static str>
    where
        Self: Sized,
    {
        vec![]
    }
}

/// Base case for the recursive tuple pattern - unit type terminates the recursion.
impl SnapshotContributor for () {
    fn collect_sections(&self, _sections: &mut Vec<SnapshotSection>) -> Result<(), SnapshotError> {
        Ok(())
    }

    fn restore_from_sections(_sections: &[SnapshotSection]) -> Result<Self, SnapshotError> {
        Ok(())
    }
}

/// Generates a `SnapshotContributor` implementation for a wrapper type.
///
/// The wrapper type (e.g., `Streams`) must have a `snapshot_read` method that
/// provides read access to its inner state, and the inner type (e.g., `StreamsInner`)
/// must implement `Snapshotable`.
///
/// # Example
///
/// ```ignore
/// impl_snapshot_contributor!(Streams, StreamsInner);
/// ```
#[macro_export]
macro_rules! impl_snapshot_contributor {
    ($wrapper:ident, $inner:ident) => {
        impl $crate::stm::snapshot::SnapshotContributor for $wrapper {
            fn collect_sections(
                &self,
                sections: &mut Vec<$crate::stm::snapshot::SnapshotSection>,
            ) -> Result<(), $crate::stm::snapshot::SnapshotError> {
                use $crate::stm::snapshot::Snapshotable;
                let section = self.snapshot_read(|inner| inner.take_snapshot())?;
                sections.push(section);
                Ok(())
            }

            fn restore_from_sections(
                sections: &[$crate::stm::snapshot::SnapshotSection],
            ) -> Result<Self, $crate::stm::snapshot::SnapshotError> {
                use $crate::stm::snapshot::{SnapshotError, Snapshotable};
                let section = sections
                    .iter()
                    .find(|s| s.name == $inner::SECTION_NAME)
                    .ok_or(SnapshotError::MissingSection($inner::SECTION_NAME))?;
                let inner = $inner::apply_snapshot(section)?;
                Ok(inner.into())
            }

            fn known_section_names() -> Vec<&'static str> {
                use $crate::stm::snapshot::Snapshotable;
                vec![$inner::SECTION_NAME]
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_envelope_roundtrip() {
        let mut envelope = SnapshotEnvelope::new(42);
        envelope
            .sections
            .push(SnapshotSection::new("test_section", vec![1, 2, 3, 4]));

        let encoded = envelope.encode().unwrap();
        let decoded = SnapshotEnvelope::decode(&encoded).unwrap();

        assert_eq!(decoded.commit_number, 42);
        assert_eq!(decoded.sections.len(), 1);
        assert_eq!(decoded.sections[0].name, "test_section");
        assert_eq!(decoded.sections[0].data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_find_section() {
        let mut envelope = SnapshotEnvelope::new(0);
        envelope.sections.push(SnapshotSection::new("foo", vec![1]));
        envelope.sections.push(SnapshotSection::new("bar", vec![2]));

        assert!(envelope.find_section("foo").is_some());
        assert!(envelope.find_section("bar").is_some());
        assert!(envelope.find_section("baz").is_none());
    }

    #[test]
    fn duplicate_section_name_detected() {
        let mut envelope = SnapshotEnvelope::new(0);
        envelope
            .sections
            .push(SnapshotSection::new("same_name", vec![1]));
        envelope
            .sections
            .push(SnapshotSection::new("same_name", vec![2]));

        let result = envelope.validate_no_duplicate_sections();
        assert!(result.is_err());
        if let Err(SnapshotError::DuplicateSection(name)) = result {
            assert_eq!(name, "same_name");
        } else {
            panic!("expected DuplicateSection error");
        }
    }

    #[test]
    fn roundtrip() {
        let mut envelope = SnapshotEnvelope::new(100);
        envelope
            .sections
            .push(SnapshotSection::new("data", vec![42; 256]));

        let encoded = envelope.encode().unwrap();
        let decoded = SnapshotEnvelope::decode(&encoded).unwrap();

        assert_eq!(decoded.commit_number, 100);
        assert_eq!(decoded.sections[0].data, vec![42; 256]);
    }
}
