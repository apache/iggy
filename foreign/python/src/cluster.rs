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
    ClusterMetadata as RustClusterMetadata, ClusterNode as RustClusterNode,
    ClusterNodeRole as RustClusterNodeRole, ClusterNodeStatus as RustClusterNodeStatus,
    TransportEndpoints as RustTransportEndpoints,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

/// The role of a node within the cluster.
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, skip_from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClusterNodeRole {
    /// Primary/Leader node - handles all writes.
    Leader,
    /// Follower/Secondary node - read replica.
    Follower,
}

impl From<RustClusterNodeRole> for ClusterNodeRole {
    fn from(role: RustClusterNodeRole) -> Self {
        match role {
            RustClusterNodeRole::Leader => ClusterNodeRole::Leader,
            RustClusterNodeRole::Follower => ClusterNodeRole::Follower,
        }
    }
}

/// The current status of a node.
#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, hash, skip_from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClusterNodeStatus {
    /// Node is healthy and responsive.
    Healthy,
    /// Node is starting up.
    Starting,
    /// Node is shutting down.
    Stopping,
    /// Node is unreachable.
    Unreachable,
    /// Node is in maintenance mode.
    Maintenance,
    /// Node is unknown.
    Unknown,
}

impl From<RustClusterNodeStatus> for ClusterNodeStatus {
    fn from(status: RustClusterNodeStatus) -> Self {
        match status {
            RustClusterNodeStatus::Healthy => ClusterNodeStatus::Healthy,
            RustClusterNodeStatus::Starting => ClusterNodeStatus::Starting,
            RustClusterNodeStatus::Stopping => ClusterNodeStatus::Stopping,
            RustClusterNodeStatus::Unreachable => ClusterNodeStatus::Unreachable,
            RustClusterNodeStatus::Maintenance => ClusterNodeStatus::Maintenance,
            RustClusterNodeStatus::Unknown => ClusterNodeStatus::Unknown,
        }
    }
}

/// Transport endpoints exposed by a cluster node.
///
/// A port of 0 means the node does not advertise that transport.
#[gen_stub_pyclass]
#[pyclass(frozen)]
pub struct TransportEndpoints {
    /// TCP port.
    #[pyo3(get)]
    pub tcp: u16,
    /// QUIC port.
    #[pyo3(get)]
    pub quic: u16,
    /// HTTP port.
    #[pyo3(get)]
    pub http: u16,
    /// WebSocket port.
    #[pyo3(get)]
    pub websocket: u16,
}

impl From<&RustTransportEndpoints> for TransportEndpoints {
    fn from(endpoints: &RustTransportEndpoints) -> Self {
        Self {
            tcp: endpoints.tcp,
            quic: endpoints.quic,
            http: endpoints.http,
            websocket: endpoints.websocket,
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl TransportEndpoints {
    fn __repr__(&self) -> String {
        format!(
            "TransportEndpoints(tcp={}, quic={}, http={}, websocket={})",
            self.tcp, self.quic, self.http, self.websocket
        )
    }
}

/// A node in the cluster with its transport endpoints, role, and status.
#[gen_stub_pyclass]
#[pyclass]
pub struct ClusterNode {
    pub(crate) inner: RustClusterNode,
}

impl From<RustClusterNode> for ClusterNode {
    fn from(node: RustClusterNode) -> Self {
        Self { inner: node }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ClusterNode {
    /// The name of the node.
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// The address clients should use to reach the node.
    ///
    /// This is a bare host (a hostname or an IP address) with no port and no
    /// brackets, so an IPv6 address needs brackets before a port is appended,
    /// for example `[::1]:8090`.
    #[getter]
    pub fn ip(&self) -> String {
        self.inner.ip.clone()
    }

    /// The transport endpoints of the node.
    #[getter]
    pub fn endpoints(&self) -> TransportEndpoints {
        TransportEndpoints::from(&self.inner.endpoints)
    }

    /// The role of the node in the cluster.
    #[getter]
    pub fn role(&self) -> ClusterNodeRole {
        ClusterNodeRole::from(self.inner.role)
    }

    /// The current status of the node.
    #[getter]
    pub fn status(&self) -> ClusterNodeStatus {
        ClusterNodeStatus::from(self.inner.status)
    }

    fn __repr__(&self) -> String {
        format!(
            "ClusterNode(name='{}', ip='{}', endpoints={}, role=ClusterNodeRole.{:?}, status=ClusterNodeStatus.{:?})",
            self.inner.name,
            self.inner.ip,
            TransportEndpoints::from(&self.inner.endpoints).__repr__(),
            ClusterNodeRole::from(self.inner.role),
            ClusterNodeStatus::from(self.inner.status),
        )
    }
}

/// Metadata of all nodes in the cluster.
///
/// A server running without clustering reports itself as the only node.
#[gen_stub_pyclass]
#[pyclass]
pub struct ClusterMetadata {
    pub(crate) inner: RustClusterMetadata,
}

impl From<RustClusterMetadata> for ClusterMetadata {
    fn from(metadata: RustClusterMetadata) -> Self {
        Self { inner: metadata }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl ClusterMetadata {
    /// The name of the cluster.
    #[getter]
    pub fn name(&self) -> String {
        self.inner.name.clone()
    }

    /// The list of all nodes in the cluster.
    ///
    /// Each access builds a fresh list of new `ClusterNode` objects, so cache
    /// the result when reading it more than once.
    #[getter]
    pub fn nodes(&self) -> Vec<ClusterNode> {
        self.inner
            .nodes
            .iter()
            .cloned()
            .map(ClusterNode::from)
            .collect()
    }

    fn __repr__(&self) -> String {
        format!(
            "ClusterMetadata(name='{}', nodes_count={})",
            self.inner.name,
            self.inner.nodes.len(),
        )
    }
}
