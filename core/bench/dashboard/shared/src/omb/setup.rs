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

//! The setup sidecar: an optional `setup.toml` in the directory of an OMB result. The result
//! doesn't record the hardware, the versions or the start of its run, so any tool can write them
//! into this file. The dashboard defines the format. Every reader of a run page sees the sidecar,
//! so it must hold no secrets or private addresses.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::error::Error;
use std::fmt;
use std::path::Path;
use std::str::FromStr;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use toml::{Table, Value};

/// The name of the sidecar in a run directory.
pub const OMB_SETUP_FILE_NAME: &str = "setup.toml";
/// The group of the keys directly under `[properties]`.
pub const OMB_OTHER_PROPERTY_GROUP: &str = "Other";
/// The properties after this many, in group and key order, are left out.
pub const OMB_MAX_PROPERTIES: usize = 100;
/// A property value with more characters than this is left out.
pub const OMB_MAX_PROPERTY_VALUE_CHARS: usize = 256;

/// A property key or group title that holds one of these words, in any case, is left out. A key
/// anywhere in the file that holds one keeps the server from sending the file.
const SECRET_WORDS: [&str; 3] = ["password", "secret", "token"];

/// The setup of one OMB run, from its sidecar. Every table and field is optional.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OmbSetup {
    /// Start of the run, RFC 3339 with a zone, as written. It replaces the time from the file
    /// name.
    pub timestamp: Option<String>,
    /// A free label for a set of runs, for example `cluster-3-nodes`.
    pub suite: Option<String>,
    /// `[environment]`
    pub environment: Option<OmbEnvironment>,
    /// `[server]`
    pub server: Option<OmbServerBuild>,
    /// `[omb]`
    pub omb: Option<OmbBuild>,
    /// `[properties]`: the groups in title order, with the properties of each group in key order.
    pub properties: Vec<OmbPropertyGroup>,
    /// The file has a key at any depth whose name looks like a secret. The setup leaves it out,
    /// but the file still holds it, so the server doesn't send the file.
    pub file_holds_secrets: bool,
}

/// The machines of a run. Two runs on the same machines have the same values, except the label.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OmbEnvironment {
    /// For display only, for example `AWS i4i.4xlarge, 3 nodes`.
    pub label: Option<String>,
    pub server_instance: Option<String>,
    pub server_nodes: Option<u32>,
    pub client_instance: Option<String>,
    /// The hosts that run the OMB workers.
    pub client_hosts: Option<u32>,
}

/// The Iggy server build, with the names and the meaning of the iggy-bench options `--gitref` and
/// `--gitref-date`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmbServerBuild {
    /// The version that the server reports, for example `0.10.0-rc.1`.
    pub version: Option<String>,
    /// A release tag such as `server-0.10.0`, a commit or another ref.
    pub gitref: Option<String>,
    /// The commit date, RFC 3339 with a zone.
    pub gitref_date: Option<String>,
}

/// The OMB build. It holds the driver code, so it is part of the setup.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmbBuild {
    pub gitref: Option<String>,
    /// The commit date, RFC 3339 with a zone.
    pub gitref_date: Option<String>,
}

/// One table under `[properties]`, or the keys directly under it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmbPropertyGroup {
    /// The table name, or [`OMB_OTHER_PROPERTY_GROUP`].
    pub title: String,
    pub properties: Vec<OmbProperty>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmbProperty {
    pub key: String,
    pub value: OmbPropertyValue,
}

/// A property value as the pages show it, with the TOML type it had. Two values are equal when
/// both their text and their type are.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OmbPropertyValue {
    pub text: String,
    pub kind: OmbScalarKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OmbScalarKind {
    String,
    Integer,
    Float,
    Boolean,
}

/// A TOML value that a property can't hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OmbNonScalar {
    Array,
    Table,
    Date,
}

/// A parsed sidecar: the setup, the values that the parser left out, and the fields that it
/// doesn't know.
#[derive(Debug, Clone, PartialEq)]
pub struct OmbSidecar {
    pub setup: OmbSetup,
    /// For a warning in the server log.
    pub warnings: Vec<OmbSetupWarning>,
    /// Paths of the fields that the parser doesn't know, such as `environment.region`, for a debug
    /// log. A newer writer can add fields, so they are not warnings.
    pub unknown_fields: Vec<String>,
}

/// A value of the sidecar that the parser left out. No variant holds a value, so a warning never
/// writes a secret into the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OmbSetupWarning {
    /// A known field holds a value of another type.
    WrongType {
        field: &'static str,
        expected: &'static str,
    },
    /// `timestamp` or a `gitref_date` that is not an RFC 3339 time with a zone.
    InvalidTime {
        field: &'static str,
    },
    /// A property group whose title holds a secret word. The whole group is left out.
    SecretGroup {
        group: String,
    },
    /// A property key that holds a secret word.
    SecretProperty {
        group: String,
        key: String,
    },
    NotScalar {
        group: String,
        key: String,
        found: OmbNonScalar,
    },
    TooLong {
        group: String,
        key: String,
        chars: usize,
    },
    /// A key directly under `[properties]` that is also in `[properties.Other]`. The parser keeps
    /// one of them.
    Duplicate {
        group: String,
        key: String,
    },
    TooManyProperties {
        dropped: usize,
    },
}

/// The sidecar is not valid TOML. The error names the line, but not the text of the file, so a
/// broken line with a secret doesn't go into the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OmbSetupError {
    pub message: String,
    /// From 1.
    pub line: Option<usize>,
}

/// Collects the warnings and unknown fields while it reads the TOML tables.
#[derive(Default)]
struct SidecarReader {
    warnings: Vec<OmbSetupWarning>,
    unknown_fields: Vec<String>,
}

impl FromStr for OmbSidecar {
    type Err = OmbSetupError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let table = text
            .parse::<Table>()
            .map_err(|error| OmbSetupError::new(&error, text))?;
        let file_holds_secrets = holds_secret_key(&table);
        let mut reader = SidecarReader::default();
        let mut setup = reader.setup(table);
        setup.file_holds_secrets = file_holds_secrets;
        Ok(Self {
            setup,
            warnings: reader.warnings,
            unknown_fields: reader.unknown_fields,
        })
    }
}

impl OmbSetup {
    /// The properties of every group, in group and key order.
    pub fn properties(&self) -> impl Iterator<Item = (&str, &OmbProperty)> {
        self.properties.iter().flat_map(|group| {
            group
                .properties
                .iter()
                .map(|property| (group.title.as_str(), property))
        })
    }
}

impl OmbEnvironment {
    /// Whether any value that tells machines apart is known. The label is for display only.
    pub const fn is_known(&self) -> bool {
        self.server_instance.is_some()
            || self.server_nodes.is_some()
            || self.client_instance.is_some()
            || self.client_hosts.is_some()
    }

    /// This environment without its label, for keys that must not change with the wording.
    pub fn without_label(&self) -> Self {
        Self {
            label: None,
            ..self.clone()
        }
    }
}

impl TryFrom<Value> for OmbPropertyValue {
    type Error = OmbNonScalar;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        let (text, kind) = match value {
            Value::String(text) => (text, OmbScalarKind::String),
            Value::Integer(integer) => (integer.to_string(), OmbScalarKind::Integer),
            // Debug keeps the decimal point of a whole float, so 2.0 doesn't read as the integer 2.
            Value::Float(float) => (format!("{float:?}"), OmbScalarKind::Float),
            Value::Boolean(boolean) => (boolean.to_string(), OmbScalarKind::Boolean),
            Value::Array(_) => return Err(OmbNonScalar::Array),
            Value::Table(_) => return Err(OmbNonScalar::Table),
            Value::Datetime(_) => return Err(OmbNonScalar::Date),
        };
        Ok(Self { text, kind })
    }
}

impl fmt::Display for OmbNonScalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Array => "an array",
            Self::Table => "a table",
            Self::Date => "a date",
        })
    }
}

impl fmt::Display for OmbSetupWarning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongType { field, expected } => {
                write!(f, "{field} must be {expected}, so it is left out")
            }
            Self::InvalidTime { field } => write!(
                f,
                "{field} is not an RFC 3339 time with a zone, so it is left out"
            ),
            Self::SecretGroup { group } => write!(
                f,
                "the property group \"{group}\" looks like it holds secrets, so it is left out"
            ),
            Self::SecretProperty { group, key } => write!(
                f,
                "the property \"{key}\" in the group \"{group}\" looks like a secret, so it is \
                 left out"
            ),
            Self::NotScalar { group, key, found } => write!(
                f,
                "the property \"{key}\" in the group \"{group}\" is {found}, but a property is a \
                 string, a number or a boolean, so it is left out"
            ),
            Self::TooLong { group, key, chars } => write!(
                f,
                "the property \"{key}\" in the group \"{group}\" has {chars} characters, more \
                 than {OMB_MAX_PROPERTY_VALUE_CHARS}, so it is left out"
            ),
            Self::Duplicate { group, key } => write!(
                f,
                "the property \"{key}\" is in the group \"{group}\" twice, so one of them is left \
                 out"
            ),
            Self::TooManyProperties { dropped } => write!(
                f,
                "the sidecar has {dropped} properties more than {OMB_MAX_PROPERTIES}, so they are \
                 left out"
            ),
        }
    }
}

impl OmbSetupError {
    fn new(error: &toml::de::Error, text: &str) -> Self {
        let line = error.span().and_then(|span| {
            text.get(..span.start)
                .map(|before| before.matches('\n').count() + 1)
        });
        Self {
            message: error.message().to_owned(),
            line,
        }
    }
}

impl fmt::Display for OmbSetupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.line {
            Some(line) => write!(
                f,
                "the setup sidecar is not valid TOML at line {line}: {}",
                self.message
            ),
            None => write!(f, "the setup sidecar is not valid TOML: {}", self.message),
        }
    }
}

impl Error for OmbSetupError {}

/// The name of the setup download of the run whose result file is `result_file_name`: the result
/// file name with `.setup.toml` in place of its extension. Every sidecar is named
/// [`OMB_SETUP_FILE_NAME`], so the downloads of several runs would overwrite each other.
pub fn omb_setup_download_name(result_file_name: &str) -> String {
    match Path::new(result_file_name).file_stem() {
        Some(stem) => format!("{}.{OMB_SETUP_FILE_NAME}", stem.to_string_lossy()),
        None => OMB_SETUP_FILE_NAME.to_owned(),
    }
}

impl SidecarReader {
    fn setup(&mut self, table: Table) -> OmbSetup {
        let mut setup = OmbSetup::default();
        for (name, value) in by_key(table) {
            match name.as_str() {
                "timestamp" => setup.timestamp = self.time("timestamp", value),
                "suite" => setup.suite = self.text("suite", value),
                "environment" => {
                    setup.environment = self
                        .table("environment", value)
                        .map(|table| self.environment(table));
                }
                "server" => {
                    setup.server = self.table("server", value).map(|table| self.server(table));
                }
                "omb" => setup.omb = self.table("omb", value).map(|table| self.omb(table)),
                "properties" => {
                    setup.properties = self
                        .table("properties", value)
                        .map(|table| self.properties(table))
                        .unwrap_or_default();
                }
                _ => self.unknown_fields.push(name),
            }
        }
        setup
    }

    fn environment(&mut self, table: Table) -> OmbEnvironment {
        let mut environment = OmbEnvironment::default();
        for (name, value) in by_key(table) {
            match name.as_str() {
                "label" => environment.label = self.text("environment.label", value),
                "server_instance" => {
                    environment.server_instance = self.text("environment.server_instance", value);
                }
                "server_nodes" => {
                    environment.server_nodes = self.count("environment.server_nodes", value);
                }
                "client_instance" => {
                    environment.client_instance = self.text("environment.client_instance", value);
                }
                "client_hosts" => {
                    environment.client_hosts = self.count("environment.client_hosts", value);
                }
                _ => self.unknown_fields.push(format!("environment.{name}")),
            }
        }
        environment
    }

    fn server(&mut self, table: Table) -> OmbServerBuild {
        let mut server = OmbServerBuild::default();
        for (name, value) in by_key(table) {
            match name.as_str() {
                "version" => server.version = self.text("server.version", value),
                "gitref" => server.gitref = self.text("server.gitref", value),
                "gitref_date" => server.gitref_date = self.time("server.gitref_date", value),
                _ => self.unknown_fields.push(format!("server.{name}")),
            }
        }
        server
    }

    fn omb(&mut self, table: Table) -> OmbBuild {
        let mut omb = OmbBuild::default();
        for (name, value) in by_key(table) {
            match name.as_str() {
                "gitref" => omb.gitref = self.text("omb.gitref", value),
                "gitref_date" => omb.gitref_date = self.time("omb.gitref_date", value),
                _ => self.unknown_fields.push(format!("omb.{name}")),
            }
        }
        omb
    }

    /// Tables become groups, and other values go to the group "Other". The secret check comes
    /// first, so a secret never reaches a later warning by its value.
    fn properties(&mut self, table: Table) -> Vec<OmbPropertyGroup> {
        let mut groups: BTreeMap<String, BTreeMap<String, OmbPropertyValue>> = BTreeMap::new();
        for (name, value) in by_key(table) {
            match value {
                Value::Table(_) if is_secret(&name) => {
                    self.warnings
                        .push(OmbSetupWarning::SecretGroup { group: name });
                }
                Value::Table(group) => {
                    for (key, value) in by_key(group) {
                        self.property(&mut groups, &name, key, value);
                    }
                }
                value => self.property(&mut groups, OMB_OTHER_PROPERTY_GROUP, name, value),
            }
        }
        let mut kept = 0;
        let mut dropped = 0;
        let groups = groups
            .into_iter()
            .filter_map(|(title, properties)| {
                let properties: Vec<_> = properties
                    .into_iter()
                    .filter_map(|(key, value)| {
                        if kept < OMB_MAX_PROPERTIES {
                            kept += 1;
                            Some(OmbProperty { key, value })
                        } else {
                            dropped += 1;
                            None
                        }
                    })
                    .collect();
                (!properties.is_empty()).then_some(OmbPropertyGroup { title, properties })
            })
            .collect();
        if dropped > 0 {
            self.warnings
                .push(OmbSetupWarning::TooManyProperties { dropped });
        }
        groups
    }

    fn property(
        &mut self,
        groups: &mut BTreeMap<String, BTreeMap<String, OmbPropertyValue>>,
        group: &str,
        key: String,
        value: Value,
    ) {
        let group = group.to_owned();
        if is_secret(&key) {
            self.warnings
                .push(OmbSetupWarning::SecretProperty { group, key });
            return;
        }
        let value = match OmbPropertyValue::try_from(value) {
            Ok(value) => value,
            Err(found) => {
                self.warnings
                    .push(OmbSetupWarning::NotScalar { group, key, found });
                return;
            }
        };
        let chars = value.text.chars().count();
        if chars > OMB_MAX_PROPERTY_VALUE_CHARS {
            self.warnings
                .push(OmbSetupWarning::TooLong { group, key, chars });
            return;
        }
        match groups.entry(group.clone()).or_default().entry(key) {
            Entry::Vacant(entry) => {
                entry.insert(value);
            }
            Entry::Occupied(entry) => self.warnings.push(OmbSetupWarning::Duplicate {
                group,
                key: entry.key().clone(),
            }),
        }
    }

    fn table(&mut self, field: &'static str, value: Value) -> Option<Table> {
        match value {
            Value::Table(table) => Some(table),
            _ => self.wrong_type(field, "a table"),
        }
    }

    fn text(&mut self, field: &'static str, value: Value) -> Option<String> {
        match value {
            Value::String(text) => Some(text),
            _ => self.wrong_type(field, "a string"),
        }
    }

    fn count(&mut self, field: &'static str, value: Value) -> Option<u32> {
        match value {
            Value::Integer(integer) => match u32::try_from(integer) {
                Ok(count) => Some(count),
                Err(_) => self.wrong_type(field, "a whole number from 0"),
            },
            _ => self.wrong_type(field, "a whole number from 0"),
        }
    }

    /// Takes a string or a TOML date and keeps it as text. A time without a zone could be any of
    /// 24 hours, so it doesn't order runs and is left out.
    fn time(&mut self, field: &'static str, value: Value) -> Option<String> {
        let text = match value {
            Value::String(text) => text,
            Value::Datetime(datetime) => datetime.to_string(),
            _ => return self.wrong_type(field, "an RFC 3339 time with a zone"),
        };
        if DateTime::parse_from_rfc3339(&text).is_ok() {
            Some(text)
        } else {
            self.warnings.push(OmbSetupWarning::InvalidTime { field });
            None
        }
    }

    fn wrong_type<T>(&mut self, field: &'static str, expected: &'static str) -> Option<T> {
        self.warnings
            .push(OmbSetupWarning::WrongType { field, expected });
        None
    }
}

/// The entries of a table in key order. `toml::Table` keeps the file order when a crate in the
/// build turns on the `preserve_order` feature of `toml`, so the order is set here.
fn by_key(table: Table) -> Vec<(String, Value)> {
    let mut entries: Vec<_> = table.into_iter().collect();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    entries
}

/// Whether a key at any depth looks like a secret. The parser drops a nested table, an array or an
/// unknown field by its outer key, so it never sees the keys inside them.
fn holds_secret_key(table: &Table) -> bool {
    table
        .iter()
        .any(|(key, value)| is_secret(key) || value_holds_secret_key(value))
}

fn value_holds_secret_key(value: &Value) -> bool {
    match value {
        Value::Table(table) => holds_secret_key(table),
        Value::Array(values) => values.iter().any(value_holds_secret_key),
        _ => false,
    }
}

fn is_secret(key: &str) -> bool {
    let key = key.to_lowercase();
    SECRET_WORDS.iter().any(|word| key.contains(word))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::test_fixtures::{
        EDGE8_100_PARTITIONS, EDGE8_DEFAULT, RC1_DEFAULT, RC1_PERSISTED,
    };

    const FULL_SIDECAR: &str = r#"
timestamp = "2026-11-06T13:10:04Z"
suite = "cluster-3-nodes"

[environment]
label = "Cloud, 3 nodes"
server_instance = "large"
server_nodes = 3
client_instance = "medium"
client_hosts = 2

[server]
version = "0.10.0-rc.1"
gitref = "server-0.10.0-rc.1"
gitref_date = "2026-11-04T10:00:00+01:00"

[omb]
gitref = "a1b2c3d"
gitref_date = 2026-10-01T08:00:00Z

[properties]
note = "first run of the day"

[properties.kernel]
"vm.dirty_bytes" = 2147483648
"vm.dirty_background_bytes" = 268435456

[properties.server]
huge_pages = 2048
data_file_system = "ext4"
direct_io = false
cpu_share = 0.5
whole_share = 2.0
"#;

    fn sidecar(text: &str) -> OmbSidecar {
        text.parse().expect("the sidecar parses")
    }

    fn property(key: &str, text: &str, kind: OmbScalarKind) -> OmbProperty {
        OmbProperty {
            key: key.to_owned(),
            value: OmbPropertyValue {
                text: text.to_owned(),
                kind,
            },
        }
    }

    fn keys(setup: &OmbSetup) -> Vec<(&str, &str)> {
        setup
            .properties()
            .map(|(group, property)| (group, property.key.as_str()))
            .collect()
    }

    #[test]
    fn given_full_sidecar_when_parsed_should_read_every_table_without_warnings() {
        let parsed = sidecar(FULL_SIDECAR);
        let setup = parsed.setup;

        assert_eq!(parsed.warnings, []);
        assert_eq!(parsed.unknown_fields, Vec::<String>::new());
        assert!(!setup.file_holds_secrets);
        assert_eq!(setup.timestamp.as_deref(), Some("2026-11-06T13:10:04Z"));
        assert_eq!(setup.suite.as_deref(), Some("cluster-3-nodes"));
        assert_eq!(
            setup.environment,
            Some(OmbEnvironment {
                label: Some("Cloud, 3 nodes".to_owned()),
                server_instance: Some("large".to_owned()),
                server_nodes: Some(3),
                client_instance: Some("medium".to_owned()),
                client_hosts: Some(2),
            })
        );
        assert_eq!(
            setup.server,
            Some(OmbServerBuild {
                version: Some("0.10.0-rc.1".to_owned()),
                gitref: Some("server-0.10.0-rc.1".to_owned()),
                gitref_date: Some("2026-11-04T10:00:00+01:00".to_owned()),
            })
        );
        assert_eq!(
            setup.omb,
            Some(OmbBuild {
                gitref: Some("a1b2c3d".to_owned()),
                gitref_date: Some("2026-10-01T08:00:00Z".to_owned()),
            })
        );
        assert_eq!(
            setup.properties,
            [
                OmbPropertyGroup {
                    title: "Other".to_owned(),
                    properties: vec![property(
                        "note",
                        "first run of the day",
                        OmbScalarKind::String
                    )],
                },
                OmbPropertyGroup {
                    title: "kernel".to_owned(),
                    properties: vec![
                        property(
                            "vm.dirty_background_bytes",
                            "268435456",
                            OmbScalarKind::Integer
                        ),
                        property("vm.dirty_bytes", "2147483648", OmbScalarKind::Integer),
                    ],
                },
                OmbPropertyGroup {
                    title: "server".to_owned(),
                    properties: vec![
                        property("cpu_share", "0.5", OmbScalarKind::Float),
                        property("data_file_system", "ext4", OmbScalarKind::String),
                        property("direct_io", "false", OmbScalarKind::Boolean),
                        property("huge_pages", "2048", OmbScalarKind::Integer),
                        property("whole_share", "2.0", OmbScalarKind::Float),
                    ],
                },
            ]
        );
    }

    #[test]
    fn given_empty_sidecar_when_parsed_should_give_empty_setup() {
        for text in ["", "# Only a comment.\n"] {
            let parsed = sidecar(text);

            assert_eq!(parsed.setup, OmbSetup::default());
            assert_eq!(parsed.warnings, []);
        }
    }

    #[test]
    fn given_broken_sidecar_when_parsed_should_fail_with_line_and_no_file_text() {
        let error = "suite = \"cluster\"\n[environment\nroot_password = \"hunter2\"\n"
            .parse::<OmbSidecar>()
            .expect_err("the table header is not closed");

        assert_eq!(error.line, Some(2));
        assert!(
            error
                .to_string()
                .starts_with("the setup sidecar is not valid TOML at line 2: ")
        );
        assert!(!error.to_string().contains("hunter2"));
        assert!(
            "timestamp = \"2026-11-06T13:10:04Z\"\ntimestamp = \"2026-11-06T13:10:05Z\"\n"
                .parse::<OmbSidecar>()
                .is_err()
        );
    }

    #[test]
    fn given_unknown_fields_when_parsed_should_list_them_and_keep_known_fields() {
        let parsed = sidecar(
            r#"
writer = "prepare step"
[environment]
server_nodes = 3
region = "somewhere"
[server]
version = "0.10.0"
sha256 = "abc"
[omb]
tarball = "omb.tar.gz"
[results]
kept = true
"#,
        );

        assert_eq!(parsed.warnings, []);
        assert_eq!(
            parsed.unknown_fields,
            [
                "environment.region",
                "omb.tarball",
                "results",
                "server.sha256",
                "writer"
            ]
        );
        assert!(!parsed.setup.file_holds_secrets);
        assert_eq!(
            parsed
                .setup
                .environment
                .and_then(|environment| environment.server_nodes),
            Some(3)
        );
        assert_eq!(
            parsed.setup.server.and_then(|server| server.version),
            Some("0.10.0".to_owned())
        );
    }

    #[test]
    fn given_values_of_wrong_type_when_parsed_should_warn_and_leave_them_out() {
        let parsed = sidecar(
            r#"
suite = 7
properties = "none"
[environment]
label = "Cloud"
server_nodes = "three"
client_hosts = -2
[server]
version = 10
[omb]
gitref = ["a", "b"]
"#,
        );

        assert_eq!(
            parsed.warnings,
            [
                OmbSetupWarning::WrongType {
                    field: "environment.client_hosts",
                    expected: "a whole number from 0"
                },
                OmbSetupWarning::WrongType {
                    field: "environment.server_nodes",
                    expected: "a whole number from 0"
                },
                OmbSetupWarning::WrongType {
                    field: "omb.gitref",
                    expected: "a string"
                },
                OmbSetupWarning::WrongType {
                    field: "properties",
                    expected: "a table"
                },
                OmbSetupWarning::WrongType {
                    field: "server.version",
                    expected: "a string"
                },
                OmbSetupWarning::WrongType {
                    field: "suite",
                    expected: "a string"
                },
            ]
        );
        assert_eq!(
            parsed.setup,
            OmbSetup {
                environment: Some(OmbEnvironment {
                    label: Some("Cloud".to_owned()),
                    ..OmbEnvironment::default()
                }),
                server: Some(OmbServerBuild::default()),
                omb: Some(OmbBuild::default()),
                ..OmbSetup::default()
            }
        );
    }

    #[test]
    fn given_times_without_zone_or_not_rfc_3339_when_parsed_should_warn_and_leave_them_out() {
        let parsed = sidecar(
            r#"
timestamp = "2026-11-06T13:10:04"
[server]
gitref_date = 2026-11-04T10:00:00
[omb]
gitref_date = "yesterday"
"#,
        );

        assert_eq!(parsed.setup.timestamp, None);
        assert_eq!(
            parsed.setup.server.and_then(|server| server.gitref_date),
            None
        );
        assert_eq!(parsed.setup.omb.and_then(|omb| omb.gitref_date), None);
        assert_eq!(
            parsed.warnings,
            [
                OmbSetupWarning::InvalidTime {
                    field: "omb.gitref_date"
                },
                OmbSetupWarning::InvalidTime {
                    field: "server.gitref_date"
                },
                OmbSetupWarning::InvalidTime { field: "timestamp" },
            ]
        );
    }

    #[test]
    fn given_toml_date_with_zone_when_parsed_should_keep_it_as_rfc_3339_text() {
        let parsed = sidecar("timestamp = 2026-11-06T14:10:04+01:00\n");

        assert_eq!(parsed.warnings, []);
        assert_eq!(
            parsed.setup.timestamp.as_deref(),
            Some("2026-11-06T14:10:04+01:00")
        );
    }

    #[test]
    fn given_secret_keys_in_any_case_when_parsed_should_drop_them_with_warning() {
        let parsed = sidecar(
            r#"
[properties]
API_TOKEN = "abc"
[properties.server]
root_Password = "hunter2"
huge_pages = 2048
[properties.Secrets]
anything = "x"
[properties.client]
session_token_ttl = 60
"#,
        );

        assert_eq!(keys(&parsed.setup), [("server", "huge_pages")]);
        assert_eq!(
            parsed.warnings,
            [
                OmbSetupWarning::SecretProperty {
                    group: "Other".to_owned(),
                    key: "API_TOKEN".to_owned()
                },
                OmbSetupWarning::SecretGroup {
                    group: "Secrets".to_owned()
                },
                OmbSetupWarning::SecretProperty {
                    group: "client".to_owned(),
                    key: "session_token_ttl".to_owned()
                },
                OmbSetupWarning::SecretProperty {
                    group: "server".to_owned(),
                    key: "root_Password".to_owned()
                },
            ]
        );
        for warning in &parsed.warnings {
            assert!(!warning.to_string().contains("hunter2"), "{warning}");
        }
        assert!(parsed.setup.file_holds_secrets);
    }

    #[test]
    fn given_unknown_field_named_like_secret_when_parsed_should_mark_file_as_holding_secrets() {
        let parsed = sidecar(
            r#"
[server]
version = "0.10.0"
access_token = "abc"
"#,
        );

        assert_eq!(parsed.warnings, []);
        assert_eq!(parsed.unknown_fields, ["server.access_token"]);
        assert!(parsed.setup.file_holds_secrets);
    }

    #[test]
    fn given_secret_keys_below_dropped_values_when_parsed_should_mark_file_as_holding_secrets() {
        for text in [
            "[properties.server.auth]\npassword = \"x\"\n",
            "[[properties.server.users]]\npassword = \"x\"\n",
            "[properties.server]\nauth = { password = \"x\" }\n",
            "properties = [{ password = \"x\" }]\n",
            "suite = { password = \"x\" }\n",
            "[cluster]\nroot_password = \"x\"\n",
            "[environment.cloud]\nsecret_key = \"x\"\n",
            "[server]\nversion = { token = \"x\" }\n",
        ] {
            let parsed = sidecar(text);

            assert!(parsed.setup.file_holds_secrets, "{text}");
        }
    }

    #[test]
    fn given_arrays_deeper_tables_and_dates_when_parsed_should_drop_them_with_warning() {
        let parsed = sidecar(
            r#"
[properties]
cores = [0, 1, 2]
started = 2026-11-06T13:10:04Z
[properties.server]
huge_pages = 2048
[properties.server.limits]
open_files = 1024
"#,
        );

        assert_eq!(keys(&parsed.setup), [("server", "huge_pages")]);
        assert_eq!(
            parsed.warnings,
            [
                OmbSetupWarning::NotScalar {
                    group: "Other".to_owned(),
                    key: "cores".to_owned(),
                    found: OmbNonScalar::Array
                },
                OmbSetupWarning::NotScalar {
                    group: "server".to_owned(),
                    key: "limits".to_owned(),
                    found: OmbNonScalar::Table
                },
                OmbSetupWarning::NotScalar {
                    group: "Other".to_owned(),
                    key: "started".to_owned(),
                    found: OmbNonScalar::Date
                },
            ]
        );
        assert!(!parsed.setup.file_holds_secrets);
    }

    #[test]
    fn given_value_over_length_limit_when_parsed_should_drop_it_with_warning() {
        let at_limit = "x".repeat(OMB_MAX_PROPERTY_VALUE_CHARS);
        let over_limit = "ż".repeat(OMB_MAX_PROPERTY_VALUE_CHARS + 1);
        let parsed = sidecar(&format!(
            "[properties.server]\nat_limit = \"{at_limit}\"\nover_limit = \"{over_limit}\"\n"
        ));

        assert_eq!(keys(&parsed.setup), [("server", "at_limit")]);
        assert_eq!(
            parsed.warnings,
            [OmbSetupWarning::TooLong {
                group: "server".to_owned(),
                key: "over_limit".to_owned(),
                chars: OMB_MAX_PROPERTY_VALUE_CHARS + 1
            }]
        );
    }

    #[test]
    fn given_more_properties_than_limit_when_parsed_should_keep_first_in_group_and_key_order() {
        let mut text = String::from("[properties.a_kernel]\n");
        for index in 0..60 {
            text.push_str(&format!("key_{index:03} = {index}\n"));
        }
        text.push_str("[properties.b_server]\n");
        for index in 0..60 {
            text.push_str(&format!("key_{index:03} = {index}\n"));
        }

        let parsed = sidecar(&text);
        let kept = keys(&parsed.setup);

        assert_eq!(kept.len(), OMB_MAX_PROPERTIES);
        assert_eq!(kept.last(), Some(&("b_server", "key_039")));
        assert_eq!(
            parsed.warnings,
            [OmbSetupWarning::TooManyProperties { dropped: 20 }]
        );
    }

    #[test]
    fn given_key_in_other_table_and_directly_under_properties_when_parsed_should_keep_one() {
        let parsed = sidecar("[properties]\nnote = \"a\"\n[properties.Other]\nnote = \"b\"\n");

        assert_eq!(keys(&parsed.setup), [("Other", "note")]);
        assert_eq!(
            parsed.warnings,
            [OmbSetupWarning::Duplicate {
                group: "Other".to_owned(),
                key: "note".to_owned()
            }]
        );
    }

    #[test]
    fn given_empty_property_group_when_parsed_should_leave_group_out() {
        let parsed = sidecar("[properties.kernel]\n[properties.server]\nhuge_pages = 0\n");

        assert_eq!(parsed.setup.properties.len(), 1);
        assert_eq!(parsed.setup.properties[0].title, "server");
    }

    #[test]
    fn given_real_sidecars_when_parsed_should_read_them_without_warnings() {
        for fixture in [
            RC1_DEFAULT,
            RC1_PERSISTED,
            EDGE8_DEFAULT,
            EDGE8_100_PARTITIONS,
        ] {
            let parsed = sidecar(fixture.sidecar.expect("these fixtures have a sidecar"));

            assert_eq!(parsed.warnings, [], "{}", fixture.file_name);
            assert_eq!(parsed.unknown_fields, Vec::<String>::new());
            assert!(!parsed.setup.file_holds_secrets, "{}", fixture.file_name);
        }
    }

    #[test]
    fn given_real_sidecar_when_parsed_should_read_its_values() {
        let rc1 = RC1_DEFAULT.setup().expect("the fixture has a sidecar");
        let edge = EDGE8_100_PARTITIONS
            .setup()
            .expect("the fixture has a sidecar");

        assert_eq!(rc1.timestamp.as_deref(), Some("2026-11-06T13:22:01Z"));
        assert_eq!(rc1.suite.as_deref(), Some("cluster-3-nodes"));
        assert_eq!(
            rc1.environment,
            Some(OmbEnvironment {
                label: Some("AWS i4i.4xlarge, 3 nodes".to_owned()),
                server_instance: Some("i4i.4xlarge".to_owned()),
                server_nodes: Some(3),
                client_instance: Some("i4i.4xlarge".to_owned()),
                client_hosts: Some(2),
            })
        );
        assert_eq!(
            rc1.server,
            Some(OmbServerBuild {
                version: Some("0.10.0-rc.1".to_owned()),
                ..OmbServerBuild::default()
            })
        );
        assert_eq!(rc1.omb, None);
        assert_eq!(
            keys(&rc1),
            [
                ("kernel", "vm.dirty_background_bytes"),
                ("kernel", "vm.dirty_bytes"),
                ("kernel", "vm.dirty_expire_centisecs"),
                ("server", "data_file_system"),
                ("server", "huge_pages"),
                ("server", "placement_strategy"),
                ("server", "server_environment"),
            ]
        );
        assert_eq!(
            rc1.properties()
                .find(|(_, property)| property.key == "vm.dirty_bytes"),
            Some((
                "kernel",
                &property("vm.dirty_bytes", "2147483648", OmbScalarKind::Integer)
            ))
        );
        assert_eq!(
            edge.environment
                .as_ref()
                .and_then(|environment| environment.server_instance.as_deref()),
            Some("i7i.xlarge")
        );
        assert_eq!(
            edge.server.and_then(|server| server.version),
            Some("0.9.1-edge.8".to_owned())
        );
    }

    #[test]
    fn given_environment_when_checked_should_ignore_label() {
        let labelled = OmbEnvironment {
            label: Some("Cloud".to_owned()),
            ..OmbEnvironment::default()
        };
        let known = OmbEnvironment {
            server_nodes: Some(3),
            ..labelled.clone()
        };

        assert!(!labelled.is_known());
        assert!(known.is_known());
        assert_eq!(known.without_label().label, None);
        assert_eq!(known.without_label().server_nodes, Some(3));
    }

    #[test]
    fn given_setup_when_serialized_should_round_trip() {
        let setup = sidecar(FULL_SIDECAR).setup;
        let json = serde_json::to_string(&setup).expect("setups serialize");

        assert_eq!(
            serde_json::from_str::<OmbSetup>(&json).expect("setups deserialize"),
            setup
        );
    }

    #[test]
    fn given_result_file_name_when_naming_setup_download_should_replace_extension_with_setup_toml()
    {
        for (result_file_name, expected) in [
            (
                "12-topics-1-partition-1kb-150mb-Iggy-2026-11-06-13-26-21.json",
                "12-topics-1-partition-1kb-150mb-Iggy-2026-11-06-13-26-21.setup.toml",
            ),
            ("workload.v2.json", "workload.v2.setup.toml"),
            ("result", "result.setup.toml"),
            ("", OMB_SETUP_FILE_NAME),
        ] {
            assert_eq!(
                omb_setup_download_name(result_file_name),
                expected,
                "{result_file_name}"
            );
        }
    }
}
