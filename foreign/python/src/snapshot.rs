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

use iggy::prelude::{
    SnapshotCompression as RustSnapshotCompression, SystemSnapshotType as RustSystemSnapshotType,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::gen_stub_pyclass_enum;

/// Compression method for server snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum SnapshotCompression {
    /// Store the file as is without compression.
    Stored,
    /// Compress the file using Deflate.
    Deflated,
    /// Compress the file using BZIP2.
    Bzip2,
    /// Compress the file using ZStandard.
    Zstd,
    /// Compress the file using LZMA.
    Lzma,
    /// Compress the file using XZ.
    Xz,
}

impl From<SnapshotCompression> for RustSnapshotCompression {
    fn from(compression: SnapshotCompression) -> Self {
        match compression {
            SnapshotCompression::Stored => RustSnapshotCompression::Stored,
            SnapshotCompression::Deflated => RustSnapshotCompression::Deflated,
            SnapshotCompression::Bzip2 => RustSnapshotCompression::Bzip2,
            SnapshotCompression::Zstd => RustSnapshotCompression::Zstd,
            SnapshotCompression::Lzma => RustSnapshotCompression::Lzma,
            SnapshotCompression::Xz => RustSnapshotCompression::Xz,
        }
    }
}

impl From<RustSnapshotCompression> for SnapshotCompression {
    fn from(compression: RustSnapshotCompression) -> Self {
        match compression {
            RustSnapshotCompression::Stored => SnapshotCompression::Stored,
            RustSnapshotCompression::Deflated => SnapshotCompression::Deflated,
            RustSnapshotCompression::Bzip2 => SnapshotCompression::Bzip2,
            RustSnapshotCompression::Zstd => SnapshotCompression::Zstd,
            RustSnapshotCompression::Lzma => SnapshotCompression::Lzma,
            RustSnapshotCompression::Xz => SnapshotCompression::Xz,
        }
    }
}

/// Type of system data to include in a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum SystemSnapshotType {
    /// Overview of the filesystem.
    FilesystemOverview,
    /// List of currently running processes.
    ProcessList,
    /// Resource usage statistics of the system.
    ResourceUsage,
    /// Test snapshot type for development purposes.
    Test,
    /// Server logs.
    ServerLogs,
    /// Server configuration.
    ServerConfig,
    /// Everything.
    All,
}

impl From<SystemSnapshotType> for RustSystemSnapshotType {
    fn from(snapshot_type: SystemSnapshotType) -> Self {
        match snapshot_type {
            SystemSnapshotType::FilesystemOverview => RustSystemSnapshotType::FilesystemOverview,
            SystemSnapshotType::ProcessList => RustSystemSnapshotType::ProcessList,
            SystemSnapshotType::ResourceUsage => RustSystemSnapshotType::ResourceUsage,
            SystemSnapshotType::Test => RustSystemSnapshotType::Test,
            SystemSnapshotType::ServerLogs => RustSystemSnapshotType::ServerLogs,
            SystemSnapshotType::ServerConfig => RustSystemSnapshotType::ServerConfig,
            SystemSnapshotType::All => RustSystemSnapshotType::All,
        }
    }
}

impl From<RustSystemSnapshotType> for SystemSnapshotType {
    fn from(snapshot_type: RustSystemSnapshotType) -> Self {
        match snapshot_type {
            RustSystemSnapshotType::FilesystemOverview => SystemSnapshotType::FilesystemOverview,
            RustSystemSnapshotType::ProcessList => SystemSnapshotType::ProcessList,
            RustSystemSnapshotType::ResourceUsage => SystemSnapshotType::ResourceUsage,
            RustSystemSnapshotType::Test => SystemSnapshotType::Test,
            RustSystemSnapshotType::ServerLogs => SystemSnapshotType::ServerLogs,
            RustSystemSnapshotType::ServerConfig => SystemSnapshotType::ServerConfig,
            RustSystemSnapshotType::All => SystemSnapshotType::All,
        }
    }
}
