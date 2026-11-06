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

//! The v1 test file format.
//!
//! The types reject unknown keys, so they are the schema of a test file: a
//! misspelled key fails the load instead of silently weakening a test.

use crate::api::Method;
use crate::iggy_errors;
use crate::matcher;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use walkdir::WalkDir;

/// The format version that this runner reads.
pub const FORMAT_VERSION: FormatVersion = FormatVersion { major: 1, minor: 0 };

/// Objects that are not entities.
pub const ORCHESTRATOR: &str = "orchestrator";
pub const PROXY: &str = "proxy";

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestFile {
    pub schema_version: String,
    pub description: String,
    #[serde(default)]
    pub run_on: Vec<Requirements>,
    #[serde(default)]
    pub initial_data: Vec<AdminCall>,
    pub tests: Vec<TestCase>,
}

/// A set of conditions that a run must meet. A list of sets passes when one
/// set passes, and an empty list always passes.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requirements {
    pub topology: Option<Topology>,
    #[serde(default)]
    pub transports: Vec<Transport>,
    #[serde(default)]
    pub capabilities: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Topology {
    Single,
    Cluster,
    NoQuorum,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Transport {
    Tcp,
    Http,
    Quic,
    Websocket,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestCase {
    pub description: String,
    #[serde(default)]
    pub run_on: Vec<Requirements>,
    #[serde(default)]
    pub create_entities: Vec<EntitySpec>,
    #[serde(default)]
    pub operations: Vec<Operation>,
    #[serde(default)]
    pub expect_frames: Vec<FrameExpectation>,
    #[serde(default)]
    pub outcome: Vec<AdminCall>,
}

/// One entity to create. YAML has no single-key enum form, so each kind is
/// a field, and a client is the only kind that a test creates directly.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntitySpec {
    pub client: ClientSpec,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientSpec {
    pub id: String,
    #[serde(default)]
    pub login: Login,
    #[serde(default)]
    pub node: Node,
    /// Routes the client through a fault proxy listener of its own.
    #[serde(default)]
    pub through_proxy: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum Login {
    Keyword(LoginKeyword),
    Credentials(Credentials),
}

impl Default for Login {
    fn default() -> Self {
        Self::Keyword(LoginKeyword::Root)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoginKeyword {
    Root,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

/// The server node that a client dials.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Node {
    #[default]
    Default,
    NoQuorum,
    Leader,
    Follower,
}

impl Node {
    #[must_use]
    pub fn topology(self) -> Topology {
        match self {
            Self::Default => Topology::Single,
            Self::NoQuorum => Topology::NoQuorum,
            Self::Leader | Self::Follower => Topology::Cluster,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Operation {
    pub object: String,
    pub name: String,
    #[serde(default)]
    pub arguments: Map<String, Value>,
    #[serde(default, deserialize_with = "present")]
    pub expect_result: Option<Value>,
    pub expect_error: Option<ErrorExpectation>,
    pub save_result_as: Option<String>,
    /// Starts the call as a task with this name. `await` collects its answer.
    pub background: Option<String>,
    #[serde(default, deserialize_with = "duration")]
    pub within: Option<Duration>,
    #[serde(default, deserialize_with = "duration")]
    pub not_before: Option<Duration>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ErrorExpectation {
    pub name: Option<String>,
    pub code: Option<u32>,
    #[serde(default, deserialize_with = "present")]
    pub operation_id: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Request,
    Reply,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrameExpectation {
    pub client: String,
    /// Protocol command names, such as `consumer_group.join`. Empty keeps all.
    #[serde(default)]
    pub commands: Vec<String>,
    pub direction: Option<Direction>,
    #[serde(default)]
    pub include_pings: bool,
    pub frames: Value,
}

/// A call that the orchestrator makes with its own client, which connects to
/// the server directly.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminCall {
    pub name: String,
    #[serde(default)]
    pub arguments: Map<String, Value>,
    #[serde(default, deserialize_with = "present")]
    pub expect_result: Option<Value>,
}

/// A loaded file with the path that names its tests.
#[derive(Debug)]
pub struct LoadedFile {
    /// The path below the corpus root, with `/` separators.
    pub name: String,
    pub path: PathBuf,
    pub file: TestFile,
}

impl LoadedFile {
    /// The stable id of a test: `<file>#<description>`.
    #[must_use]
    pub fn test_id(&self, test: &TestCase) -> String {
        format!("{}#{}", self.name, test.description)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FormatVersion {
    pub major: u32,
    pub minor: u32,
}

impl FromStr for FormatVersion {
    type Err = LoadError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let invalid = || LoadError::Version(text.to_owned());
        let (major, minor) = text.split_once('.').ok_or_else(invalid)?;
        Ok(Self {
            major: major.parse().map_err(|_| invalid())?,
            minor: minor.parse().map_err(|_| invalid())?,
        })
    }
}

impl fmt::Display for FormatVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.major, self.minor)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("cannot read {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path}: {source}")]
    Parse {
        path: PathBuf,
        source: serde_yaml_ng::Error,
    },
    #[error("schema_version {0:?} is not MAJOR.MINOR")]
    Version(String),
    #[error("{path}: {message}")]
    Invalid { path: PathBuf, message: String },
    #[error("cannot walk {path}: {source}")]
    Walk {
        path: PathBuf,
        source: walkdir::Error,
    },
}

/// A file that this runner does not read, with the reason.
#[derive(Debug)]
pub struct UnreadFile {
    pub name: String,
    pub reason: String,
}

/// Every `*.yaml` file below `root`, in path order. A file that needs a newer
/// minor format version is returned as unread, not as an error.
///
/// # Errors
///
/// Returns the first file that cannot be read, parsed or validated.
pub fn load_corpus(root: &Path) -> Result<(Vec<LoadedFile>, Vec<UnreadFile>), LoadError> {
    let mut paths = Vec::new();
    for entry in WalkDir::new(root).sort_by_file_name() {
        let entry = entry.map_err(|source| LoadError::Walk {
            path: root.to_path_buf(),
            source,
        })?;
        let is_yaml = entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "yaml" || extension == "yml");
        if entry.file_type().is_file() && is_yaml {
            paths.push(entry.into_path());
        }
    }
    let base = if root.is_file() {
        root.parent().unwrap_or(root)
    } else {
        root
    };
    let mut loaded = Vec::new();
    let mut unread = Vec::new();
    for path in paths {
        let name = path
            .strip_prefix(base)
            .unwrap_or(&path)
            .components()
            .map(|component| component.as_os_str().to_string_lossy())
            .collect::<Vec<_>>()
            .join("/");
        match load_file(&path)? {
            Loaded::File(file) => loaded.push(LoadedFile { name, path, file }),
            Loaded::NewerMinor(version) => unread.push(UnreadFile {
                name,
                reason: format!("needs format {version}, this runner reads {FORMAT_VERSION}"),
            }),
        }
    }
    Ok((loaded, unread))
}

enum Loaded {
    File(TestFile),
    NewerMinor(FormatVersion),
}

fn load_file(path: &Path) -> Result<Loaded, LoadError> {
    let text = fs::read_to_string(path).map_err(|source| LoadError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let header: VersionHeader =
        serde_yaml_ng::from_str(&text).map_err(|source| LoadError::Parse {
            path: path.to_path_buf(),
            source,
        })?;
    let version: FormatVersion = header.schema_version.parse()?;
    if version.major != FORMAT_VERSION.major {
        return Err(LoadError::Invalid {
            path: path.to_path_buf(),
            message: format!(
                "format {version} has another major version than this runner, {FORMAT_VERSION}"
            ),
        });
    }
    if version > FORMAT_VERSION {
        return Ok(Loaded::NewerMinor(version));
    }
    let file: TestFile = serde_yaml_ng::from_str(&text).map_err(|source| LoadError::Parse {
        path: path.to_path_buf(),
        source,
    })?;
    validate(&file).map_err(|message| LoadError::Invalid {
        path: path.to_path_buf(),
        message,
    })?;
    Ok(Loaded::File(file))
}

/// Only the version, so that a newer minor version is recognized before its
/// unknown keys fail the strict parse.
#[derive(Deserialize)]
struct VersionHeader {
    schema_version: String,
}

/// Checks what the types cannot: names, combinations and expectations.
fn validate(file: &TestFile) -> Result<(), String> {
    for call in &file.initial_data {
        validate_admin_call(call).map_err(|message| format!("initial_data: {message}"))?;
    }
    for test in &file.tests {
        validate_test(test).map_err(|message| format!("test {:?}: {message}", test.description))?;
    }
    let mut descriptions: Vec<&str> = file
        .tests
        .iter()
        .map(|test| test.description.as_str())
        .collect();
    descriptions.sort_unstable();
    if let Some(pair) = descriptions.windows(2).find(|pair| pair[0] == pair[1]) {
        return Err(format!("two tests share the description {:?}", pair[0]));
    }
    Ok(())
}

fn validate_test(test: &TestCase) -> Result<(), String> {
    let mut entities: Vec<&str> = Vec::new();
    for EntitySpec { client } in &test.create_entities {
        if [ORCHESTRATOR, PROXY].contains(&client.id.as_str()) {
            return Err(format!("{:?} is a reserved object name", client.id));
        }
        if entities.contains(&client.id.as_str()) {
            return Err(format!("entity {:?} is created twice", client.id));
        }
        entities.push(&client.id);
    }
    for (index, operation) in test.operations.iter().enumerate() {
        validate_operation(operation)
            .map_err(|message| format!("operation {index} ({}): {message}", operation.name))?;
    }
    for expectation in &test.expect_frames {
        matcher::validate_expectation(&expectation.frames)
            .map_err(|message| format!("expect_frames: {message}"))?;
    }
    for call in &test.outcome {
        validate_admin_call(call).map_err(|message| format!("outcome: {message}"))?;
        if call.expect_result.is_none() {
            return Err(format!("outcome call {} has no expect_result", call.name));
        }
    }
    Ok(())
}

fn validate_operation(operation: &Operation) -> Result<(), String> {
    if operation.expect_result.is_some() && operation.expect_error.is_some() {
        return Err("expect_result and expect_error exclude each other".to_owned());
    }
    if operation.background.is_some()
        && (operation.expect_result.is_some()
            || operation.expect_error.is_some()
            || operation.within.is_some()
            || operation.not_before.is_some()
            || operation.save_result_as.is_some())
    {
        return Err("a background call has no expectation; await its task instead".to_owned());
    }
    if let Some(expected) = &operation.expect_result {
        matcher::validate_expectation(expected)?;
    }
    if let Some(ErrorExpectation {
        name,
        code,
        operation_id,
    }) = &operation.expect_error
    {
        if name.is_some() == code.is_some() {
            return Err("expect_error needs exactly one of name and code".to_owned());
        }
        if let Some(name) = name
            && iggy_errors::code_of(name).is_none()
        {
            return Err(format!("unknown error name {name:?}"));
        }
        if let Some(operation_id) = operation_id {
            matcher::validate_expectation(operation_id)?;
        }
    }
    match operation.object.as_str() {
        ORCHESTRATOR => OrchestratorCall::from_str(&operation.name).map(drop),
        PROXY => ProxyCall::from_str(&operation.name).map(drop),
        _ => Method::from_str(&operation.name)
            .map(drop)
            .map_err(|_| format!("unknown method {:?}", operation.name)),
    }
}

fn validate_admin_call(call: &AdminCall) -> Result<(), String> {
    Method::from_str(&call.name).map_err(|_| format!("unknown method {:?}", call.name))?;
    if let Some(expected) = &call.expect_result {
        matcher::validate_expectation(expected)?;
    }
    Ok(())
}

/// Calls on the `orchestrator` object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrchestratorCall {
    /// Sleeps for `duration_ms`.
    Wait,
    /// Collects the answer of the background task `task`.
    Await,
    /// Cancels the background task `task`.
    Cancel,
    /// Closes the entity `entity` in the agent.
    Close,
}

impl FromStr for OrchestratorCall {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "wait" => Ok(Self::Wait),
            "await" => Ok(Self::Await),
            "cancel" => Ok(Self::Cancel),
            "close" => Ok(Self::Close),
            _ => Err(format!("unknown orchestrator call {name:?}")),
        }
    }
}

/// Calls on the `proxy` object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProxyCall {
    FailFrames,
    ClearRules,
}

impl FromStr for ProxyCall {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        match name {
            "fail_frames" => Ok(Self::FailFrames),
            "clear_rules" => Ok(Self::ClearRules),
            _ => Err(format!("unknown proxy call {name:?}")),
        }
    }
}

/// Keeps an explicit `null`, which serde would otherwise read as absent.
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

fn duration<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Duration>, D::Error> {
    let text = String::deserialize(deserializer)?;
    humantime::parse_duration(&text)
        .map(Some)
        .map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> Result<TestFile, String> {
        let file: TestFile = serde_yaml_ng::from_str(text).map_err(|error| error.to_string())?;
        validate(&file)?;
        Ok(file)
    }

    #[test]
    fn given_an_unknown_key_when_loading_should_fail() {
        let error = parse(
            "schema_version: '1.0'\ndescription: d\ntests:\n  - description: t\n    operations: []\n    expect_results: 1\n",
        )
        .unwrap_err();
        assert!(error.contains("expect_results"), "{error}");
    }

    #[test]
    fn given_an_explicit_null_result_when_loading_should_keep_it() {
        let file = parse(
            "schema_version: '1.0'\ndescription: d\ntests:\n  - description: t\n    create_entities:\n      - client: {id: client0}\n    operations:\n      - {object: client0, name: get_stream, arguments: {stream_id: 1}, expect_result: null}\n",
        )
        .unwrap();
        assert_eq!(file.tests[0].operations[0].expect_result, Some(Value::Null));
    }

    #[test]
    fn given_both_result_and_error_when_loading_should_fail() {
        let error = parse(
            "schema_version: '1.0'\ndescription: d\ntests:\n  - description: t\n    operations:\n      - {object: client0, name: ping, expect_result: null, expect_error: {code: 40}}\n",
        )
        .unwrap_err();
        assert!(error.contains("exclude each other"), "{error}");
    }

    #[test]
    fn given_an_unknown_method_when_loading_should_fail() {
        let error = parse(
            "schema_version: '1.0'\ndescription: d\ntests:\n  - description: t\n    operations:\n      - {object: client0, name: fetch_stream}\n",
        )
        .unwrap_err();
        assert!(error.contains("fetch_stream"), "{error}");
    }

    #[test]
    fn given_a_newer_minor_version_when_comparing_should_order_after_the_runner() {
        let newer: FormatVersion = "1.7".parse().unwrap();
        assert!(newer > FORMAT_VERSION);
        assert!("x".parse::<FormatVersion>().is_err());
    }
}
