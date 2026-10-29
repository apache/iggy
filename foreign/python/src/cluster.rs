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
    TransportEndpoints as RustTransportEndpoints,
};
use pyo3::{
    prelude::*,
    types::{PyDict, PyString},
};
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

/// Python name and members of the node role enum. Each value is the lowercase
/// name the server uses, so a role converts by its `as_ref()` string.
pub const ROLE_ENUM: &str = "ClusterNodeRole";
pub const ROLE_MEMBERS: [(&str, &str); 2] = [("LEADER", "leader"), ("FOLLOWER", "follower")];

/// Python name and members of the node status enum, keyed like the role enum.
pub const STATUS_ENUM: &str = "ClusterNodeStatus";
pub const STATUS_MEMBERS: [(&str, &str); 6] = [
    ("HEALTHY", "healthy"),
    ("STARTING", "starting"),
    ("STOPPING", "stopping"),
    ("UNREACHABLE", "unreachable"),
    ("MAINTENANCE", "maintenance"),
    ("UNKNOWN", "unknown"),
];

/// Adds `ClusterNodeRole` and `ClusterNodeStatus` to the module as str-backed
/// `enum.Enum` types, the way `Durability` is exported, so `.name`, `.value`,
/// and iteration work as the stub declares.
pub fn register_enums(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    for (name, members) in [
        (ROLE_ENUM, ROLE_MEMBERS.as_slice()),
        (STATUS_ENUM, STATUS_MEMBERS.as_slice()),
    ] {
        let kwargs = PyDict::new(py);
        kwargs.set_item("type", py.get_type::<PyString>())?;
        kwargs.set_item("module", "apache_iggy")?;
        let enumeration = py
            .import("enum")?
            .getattr("Enum")?
            .call((name, members.to_vec()), Some(&kwargs))?;
        module.add(name, enumeration)?;
    }
    Ok(())
}

/// Returns the member of the exported enum `name` whose value is `value`.
fn enum_member<'py>(py: Python<'py>, name: &str, value: &str) -> PyResult<Bound<'py, PyAny>> {
    py.import("apache_iggy")?.getattr(name)?.call1((value,))
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
#[pyclass(frozen)]
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
    /// The replica id of the node, as accepted by
    /// `IggyClient.send_binary_request_to_replica`.
    #[getter]
    pub fn replica_id(&self) -> u8 {
        self.inner.replica_id
    }

    /// The name of the node.
    #[getter]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The address clients should use to reach the node.
    ///
    /// This is a bare host (a hostname or an IP address) with no port and no
    /// brackets, so an IPv6 address needs brackets before a port is appended,
    /// for example `[::1]:8090`.
    #[getter]
    pub fn ip(&self) -> &str {
        &self.inner.ip
    }

    /// The transport endpoints of the node.
    #[getter]
    pub fn endpoints(&self) -> TransportEndpoints {
        TransportEndpoints::from(&self.inner.endpoints)
    }

    /// The role of the node in the cluster.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "ClusterNodeRole"))]
    pub fn role<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        enum_member(py, ROLE_ENUM, self.inner.role.as_ref())
    }

    /// The current status of the node.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "ClusterNodeStatus"))]
    pub fn status<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        enum_member(py, STATUS_ENUM, self.inner.status.as_ref())
    }

    fn __repr__(&self) -> String {
        format!(
            "ClusterNode(replica_id={}, name='{}', ip='{}', endpoints={}, role={ROLE_ENUM}.{}, status={STATUS_ENUM}.{})",
            self.inner.replica_id,
            self.inner.name,
            self.inner.ip,
            TransportEndpoints::from(&self.inner.endpoints).__repr__(),
            self.inner.role.as_ref().to_uppercase(),
            self.inner.status.as_ref().to_uppercase(),
        )
    }
}

/// Metadata of all nodes in the cluster.
///
/// A server running without clustering reports itself as the only node.
#[gen_stub_pyclass]
#[pyclass(frozen)]
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
    pub fn name(&self) -> &str {
        &self.inner.name
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
