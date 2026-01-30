/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::io;
use std::path::PathBuf;

#[derive(Debug)]
pub enum TestBinaryError {
    Io(io::Error),
    ProcessSpawn {
        binary: String,
        source: io::Error,
    },
    ProcessCrashed {
        binary: String,
        exit_code: Option<i32>,
        stdout: String,
        stderr: String,
    },
    StartupTimeout {
        binary: String,
        timeout_secs: u64,
    },
    HealthCheckFailed {
        binary: String,
        address: String,
        retries: u32,
    },
    ConfigLoad {
        path: PathBuf,
        message: String,
    },
    InvalidState {
        message: String,
    },
    ClientCreation {
        transport: String,
        address: String,
        source: String,
    },
    ClientConnection {
        transport: String,
        address: String,
        source: String,
    },
    FileSystemError {
        path: PathBuf,
        source: io::Error,
    },
    MissingServer,
    MissingMcp,
    MissingConnectorsRuntime,
    NotStarted,
    AlreadyStarted,
    SeedFailed(String),
    FixtureSetup {
        fixture_type: String,
        message: String,
    },
}

impl std::fmt::Display for TestBinaryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestBinaryError::Io(e) => write!(f, "IO error: {}", e),
            TestBinaryError::ProcessSpawn { binary, source } => {
                write!(f, "Failed to spawn {}: {}", binary, source)
            }
            TestBinaryError::ProcessCrashed {
                binary,
                exit_code,
                stdout,
                stderr,
            } => {
                write!(
                    f,
                    "{} crashed with exit code {:?}\n=== STDOUT ===\n{}\n=== STDERR ===\n{}",
                    binary, exit_code, stdout, stderr
                )
            }
            TestBinaryError::StartupTimeout {
                binary,
                timeout_secs,
            } => {
                write!(
                    f,
                    "{} failed to start within {} seconds",
                    binary, timeout_secs
                )
            }
            TestBinaryError::HealthCheckFailed {
                binary,
                address,
                retries,
            } => {
                write!(
                    f,
                    "Health check failed for {} at {} after {} retries",
                    binary, address, retries
                )
            }
            TestBinaryError::ConfigLoad { path, message } => {
                write!(f, "Failed to load config from {:?}: {}", path, message)
            }
            TestBinaryError::InvalidState { message } => {
                write!(f, "Invalid state: {}", message)
            }
            TestBinaryError::ClientCreation {
                transport,
                address,
                source,
            } => {
                write!(
                    f,
                    "Failed to create {} client for {}: {}",
                    transport, address, source
                )
            }
            TestBinaryError::ClientConnection {
                transport,
                address,
                source,
            } => {
                write!(
                    f,
                    "Failed to connect {} client to {}: {}",
                    transport, address, source
                )
            }
            TestBinaryError::FileSystemError { path, source } => {
                write!(f, "Filesystem error for {:?}: {}", path, source)
            }
            TestBinaryError::MissingServer => {
                write!(f, "Server not configured in harness")
            }
            TestBinaryError::MissingMcp => {
                write!(f, "MCP server not configured in harness")
            }
            TestBinaryError::MissingConnectorsRuntime => {
                write!(f, "Connectors runtime not configured in harness")
            }
            TestBinaryError::NotStarted => {
                write!(f, "Harness not started")
            }
            TestBinaryError::AlreadyStarted => {
                write!(f, "Harness already started")
            }
            TestBinaryError::SeedFailed(msg) => {
                write!(f, "Seed function failed: {}", msg)
            }
            TestBinaryError::FixtureSetup {
                fixture_type,
                message,
            } => {
                write!(f, "Fixture setup failed for {}: {}", fixture_type, message)
            }
        }
    }
}

impl std::error::Error for TestBinaryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            TestBinaryError::Io(e) => Some(e),
            TestBinaryError::ProcessSpawn { source, .. } => Some(source),
            TestBinaryError::FileSystemError { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<io::Error> for TestBinaryError {
    fn from(e: io::Error) -> Self {
        TestBinaryError::Io(e)
    }
}
