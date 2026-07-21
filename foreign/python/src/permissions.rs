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
    GlobalPermissions as RustGlobalPermissions, Permissions as RustPermissions,
    StreamPermissions as RustStreamPermissions, TopicPermissions as RustTopicPermissions,
};
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};
use std::collections::BTreeMap;

/// The permissions of a user: global permissions applied to all streams,
/// optionally extended by per-stream permissions.
#[derive(Debug, Clone, PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, from_py_object)]
pub struct Permissions {
    pub(crate) inner: RustPermissions,
}

impl From<RustPermissions> for Permissions {
    fn from(permissions: RustPermissions) -> Self {
        Self { inner: permissions }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl Permissions {
    /// Create permissions from global permissions and optional per-stream permissions.
    ///
    /// Args:
    ///     global_: Global permissions as `GlobalPermissions | None`; defaults to all denied.
    ///     streams: Per-stream permissions keyed by stream ID as
    ///         `dict[int, StreamPermissions] | None`.
    #[new]
    #[pyo3(signature = (global_=None, streams=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "GlobalPermissions | None"))] global_: Option<
            GlobalPermissions,
        >,
        #[gen_stub(override_type(type_repr = "dict[int, StreamPermissions] | None"))]
        streams: Option<BTreeMap<u32, StreamPermissions>>,
    ) -> Self {
        Self {
            inner: RustPermissions {
                global: global_.map(|global| global.inner).unwrap_or_default(),
                streams: streams.map(|streams| {
                    streams
                        .into_iter()
                        .map(|(stream_id, stream)| (stream_id as usize, stream.inner))
                        .collect()
                }),
            },
        }
    }

    /// The global permissions, applied to all streams.
    #[getter]
    fn global_(&self) -> GlobalPermissions {
        GlobalPermissions {
            inner: self.inner.global.clone(),
        }
    }

    /// The per-stream permissions keyed by stream ID, or `None` when not set.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "dict[int, StreamPermissions] | None"))]
    fn streams(&self) -> Option<BTreeMap<u32, StreamPermissions>> {
        self.inner.streams.as_ref().map(|streams| {
            streams
                .iter()
                .map(|(stream_id, stream)| {
                    // IDs are u32 on the wire, so the cast cannot truncate.
                    (
                        *stream_id as u32,
                        StreamPermissions {
                            inner: stream.clone(),
                        },
                    )
                })
                .collect()
        })
    }
}

/// Global permissions, applied to all streams without specifying them one by one.
#[derive(Debug, Clone, PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, from_py_object)]
pub struct GlobalPermissions {
    pub(crate) inner: RustGlobalPermissions,
}

#[gen_stub_pymethods]
#[pymethods]
impl GlobalPermissions {
    /// Create global permissions. Every flag defaults to `False`.
    ///
    /// Args:
    ///     manage_servers: Allow managing servers; includes `read_servers`.
    ///     read_servers: Allow reading server info (stats, clients).
    ///     manage_users: Allow managing users; includes `read_users`.
    ///     read_users: Allow reading user info.
    ///     manage_streams: Allow managing all streams; includes `manage_topics`.
    ///     read_streams: Allow reading all streams; includes `read_topics`.
    ///     manage_topics: Allow managing all topics; includes `read_topics`.
    ///     read_topics: Allow reading all topics and consumer groups.
    ///     poll_messages: Allow polling messages from all streams.
    ///     send_messages: Allow sending messages to all streams.
    #[new]
    #[pyo3(signature = (
        manage_servers=false,
        read_servers=false,
        manage_users=false,
        read_users=false,
        manage_streams=false,
        read_streams=false,
        manage_topics=false,
        read_topics=false,
        poll_messages=false,
        send_messages=false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        manage_servers: bool,
        read_servers: bool,
        manage_users: bool,
        read_users: bool,
        manage_streams: bool,
        read_streams: bool,
        manage_topics: bool,
        read_topics: bool,
        poll_messages: bool,
        send_messages: bool,
    ) -> Self {
        Self {
            inner: RustGlobalPermissions {
                manage_servers,
                read_servers,
                manage_users,
                read_users,
                manage_streams,
                read_streams,
                manage_topics,
                read_topics,
                poll_messages,
                send_messages,
            },
        }
    }

    /// Whether managing servers is allowed; includes `read_servers`.
    #[getter]
    fn manage_servers(&self) -> bool {
        self.inner.manage_servers
    }

    /// Whether reading server info (stats, clients) is allowed.
    #[getter]
    fn read_servers(&self) -> bool {
        self.inner.read_servers
    }

    /// Whether managing users is allowed; includes `read_users`.
    #[getter]
    fn manage_users(&self) -> bool {
        self.inner.manage_users
    }

    /// Whether reading user info is allowed.
    #[getter]
    fn read_users(&self) -> bool {
        self.inner.read_users
    }

    /// Whether managing all streams is allowed; includes `manage_topics`.
    #[getter]
    fn manage_streams(&self) -> bool {
        self.inner.manage_streams
    }

    /// Whether reading all streams is allowed; includes `read_topics`.
    #[getter]
    fn read_streams(&self) -> bool {
        self.inner.read_streams
    }

    /// Whether managing all topics is allowed; includes `read_topics`.
    #[getter]
    fn manage_topics(&self) -> bool {
        self.inner.manage_topics
    }

    /// Whether reading all topics and consumer groups is allowed.
    #[getter]
    fn read_topics(&self) -> bool {
        self.inner.read_topics
    }

    /// Whether polling messages from all streams is allowed.
    #[getter]
    fn poll_messages(&self) -> bool {
        self.inner.poll_messages
    }

    /// Whether sending messages to all streams is allowed.
    #[getter]
    fn send_messages(&self) -> bool {
        self.inner.send_messages
    }
}

/// Permissions for a specific stream and all its topics, optionally refined per topic.
/// They extend the global permissions, they do not override them.
#[derive(Debug, Clone, PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, from_py_object)]
pub struct StreamPermissions {
    pub(crate) inner: RustStreamPermissions,
}

#[gen_stub_pymethods]
#[pymethods]
impl StreamPermissions {
    /// Create stream permissions. Every flag defaults to `False`.
    ///
    /// Args:
    ///     manage_stream: Allow managing the stream; includes `read_stream`.
    ///     read_stream: Allow reading the stream; includes `read_topics`.
    ///     manage_topics: Allow managing the stream topics; includes `read_topics`.
    ///     read_topics: Allow reading the stream topics and consumer groups.
    ///     poll_messages: Allow polling messages from the stream.
    ///     send_messages: Allow sending messages to the stream.
    ///     topics: Per-topic permissions keyed by topic ID as
    ///         `dict[int, TopicPermissions] | None`.
    #[new]
    #[pyo3(signature = (
        manage_stream=false,
        read_stream=false,
        manage_topics=false,
        read_topics=false,
        poll_messages=false,
        send_messages=false,
        topics=None,
    ))]
    fn new(
        manage_stream: bool,
        read_stream: bool,
        manage_topics: bool,
        read_topics: bool,
        poll_messages: bool,
        send_messages: bool,
        #[gen_stub(override_type(type_repr = "dict[int, TopicPermissions] | None"))] topics: Option<
            BTreeMap<u32, TopicPermissions>,
        >,
    ) -> Self {
        Self {
            inner: RustStreamPermissions {
                manage_stream,
                read_stream,
                manage_topics,
                read_topics,
                poll_messages,
                send_messages,
                topics: topics.map(|topics| {
                    topics
                        .into_iter()
                        .map(|(topic_id, topic)| (topic_id as usize, topic.inner))
                        .collect()
                }),
            },
        }
    }

    /// Whether managing the stream is allowed; includes `read_stream`.
    #[getter]
    fn manage_stream(&self) -> bool {
        self.inner.manage_stream
    }

    /// Whether reading the stream is allowed; includes `read_topics`.
    #[getter]
    fn read_stream(&self) -> bool {
        self.inner.read_stream
    }

    /// Whether managing the stream topics is allowed; includes `read_topics`.
    #[getter]
    fn manage_topics(&self) -> bool {
        self.inner.manage_topics
    }

    /// Whether reading the stream topics and consumer groups is allowed.
    #[getter]
    fn read_topics(&self) -> bool {
        self.inner.read_topics
    }

    /// Whether polling messages from the stream is allowed.
    #[getter]
    fn poll_messages(&self) -> bool {
        self.inner.poll_messages
    }

    /// Whether sending messages to the stream is allowed.
    #[getter]
    fn send_messages(&self) -> bool {
        self.inner.send_messages
    }

    /// The per-topic permissions keyed by topic ID, or `None` when not set.
    #[getter]
    #[gen_stub(override_return_type(type_repr = "dict[int, TopicPermissions] | None"))]
    fn topics(&self) -> Option<BTreeMap<u32, TopicPermissions>> {
        self.inner.topics.as_ref().map(|topics| {
            topics
                .iter()
                .map(|(topic_id, topic)| {
                    // IDs are u32 on the wire, so the cast cannot truncate.
                    (
                        *topic_id as u32,
                        TopicPermissions {
                            inner: topic.clone(),
                        },
                    )
                })
                .collect()
        })
    }
}

/// Permissions for a specific topic of a stream. The lowest level of permissions.
#[derive(Debug, Clone, PartialEq)]
#[gen_stub_pyclass]
#[pyclass(eq, from_py_object)]
pub struct TopicPermissions {
    pub(crate) inner: RustTopicPermissions,
}

#[gen_stub_pymethods]
#[pymethods]
impl TopicPermissions {
    /// Create topic permissions. Every flag defaults to `False`.
    ///
    /// Args:
    ///     manage_topic: Allow managing the topic; includes `read_topic`.
    ///     read_topic: Allow reading the topic and its consumer groups.
    ///     poll_messages: Allow polling messages from the topic.
    ///     send_messages: Allow sending messages to the topic.
    #[new]
    #[pyo3(signature = (
        manage_topic=false,
        read_topic=false,
        poll_messages=false,
        send_messages=false,
    ))]
    fn new(manage_topic: bool, read_topic: bool, poll_messages: bool, send_messages: bool) -> Self {
        Self {
            inner: RustTopicPermissions {
                manage_topic,
                read_topic,
                poll_messages,
                send_messages,
            },
        }
    }

    /// Whether managing the topic is allowed; includes `read_topic`.
    #[getter]
    fn manage_topic(&self) -> bool {
        self.inner.manage_topic
    }

    /// Whether reading the topic and its consumer groups is allowed.
    #[getter]
    fn read_topic(&self) -> bool {
        self.inner.read_topic
    }

    /// Whether polling messages from the topic is allowed.
    #[getter]
    fn poll_messages(&self) -> bool {
        self.inner.poll_messages
    }

    /// Whether sending messages to the topic is allowed.
    #[getter]
    fn send_messages(&self) -> bool {
        self.inner.send_messages
    }
}
