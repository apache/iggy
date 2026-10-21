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
    CompressionAlgorithm, IggyByteSize, IggyError, IggyExpiry as RustIggyExpiry,
    MaxTopicSize as RustMaxTopicSize, OptionKey as RustOptionKey,
    PartitionResizePolicy as RustPartitionResizePolicy, StreamCreateOptions, StreamEncryptionKey,
    TopicCreateOptions, UserCreateOptions,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use pyo3_stub_gen::derive::{
    gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pyfunction, gen_stub_pymethods,
};
use std::collections::BTreeMap;
use std::str::FromStr;

use crate::durability::Durability;
use crate::topic::{IggyExpiry, MaxTopicSize};

/// Options for `create_stream`.
///
/// The encryption key is write-only: the server stores it wrapped and returns
/// only its source and fingerprints, and this object never shows it.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct StreamOptions {
    pub(crate) inner: StreamCreateOptions,
}

/// Options for `create_topic`.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct TopicOptions {
    pub(crate) inner: TopicCreateOptions,
}

/// Options for `create_user`. No user key exists yet, so only the raw escape
/// hatch is available.
#[gen_stub_pyclass]
#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct UserOptions {
    pub(crate) inner: UserCreateOptions,
}

/// A catalog option key, as `update_stream`, `update_topic` and `update_user`
/// take it in `reset`. `str()` gives the key as it travels on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum OptionKey {
    /// Stream or topic name.
    Name,
    /// User name.
    Username,
    /// User status.
    Status,
    /// Topic compression algorithm, fixed at creation.
    CompressionAlgorithm,
    /// Topic message expiry, resettable to never expire.
    MessageExpiry,
    /// Topic size cap, resettable to unlimited.
    MaxTopicSize,
    /// Topic segment size, fixed at creation.
    SegmentSize,
    /// Topic message durability, fixed at creation.
    Durability,
    /// Topic consumer offset durability, fixed at creation.
    ConsumerOffsetDurability,
    /// Topic partition resize policy, fixed at creation.
    PartitionResizePolicy,
    /// Topic message-count flush threshold, fixed at creation.
    MessagesRequiredToSave,
    /// Topic byte flush threshold, fixed at creation.
    SizeOfMessagesRequiredToSave,
    /// Topic segment preallocation, fixed at creation.
    PreallocateSegments,
    /// Stream encryption key, write-only and fixed at creation.
    EncryptionKey,
    /// Who chose the stream's key, computed by the server.
    EncryptionKeySource,
    /// Fingerprint of the stream's key, computed by the server.
    EncryptionKeyFingerprint,
    /// Fingerprints of the server keys wrapping the stream's key, computed by
    /// the server.
    EncryptionKekFingerprint,
}

/// Whether partitions can be added to or deleted from a topic after creation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[gen_stub_pyclass_enum]
#[pyclass(eq, from_py_object)]
pub enum PartitionResizePolicy {
    /// Partitions can be added and deleted.
    Mutable,
    /// The partition set is fixed at topic creation.
    Fixed,
}

#[derive(FromPyObject)]
pub(crate) enum PyStreamKey<'py> {
    #[pyo3(transparent, annotation = "bytes")]
    Raw(Bound<'py, PyBytes>),
    #[pyo3(transparent, annotation = "str")]
    Base64(Bound<'py, PyString>),
}

#[gen_stub_pymethods]
#[pymethods]
impl StreamOptions {
    /// Constructs stream create options.
    ///
    /// Args:
    ///     encryption_key: The stream's own data key: exactly 32 bytes, or
    ///         their canonical standard padded base64 text. Use 32 random
    ///         bytes, for example `secrets.token_bytes(32)`. The key travels
    ///         inside the create request over whatever transport the client
    ///         uses, so protecting it in transit (TLS) is the caller's
    ///         responsibility. The server never returns it, only its source
    ///         and fingerprints. Without a key, a server with encryption
    ///         enabled generates one for the stream.
    ///     raw: Additional option keys as `dict[str, str]`, sent as string
    ///         values so a key newer than this build can be set. A typed field
    ///         wins on collision.
    ///
    /// Raises:
    ///     ValueError: If `encryption_key` is not exactly 32 bytes or their
    ///         canonical base64 text. The message never contains the key.
    ///     TypeError: If `encryption_key` is neither `bytes` nor `str`.
    #[new]
    #[pyo3(signature = (*, encryption_key=None, raw=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.bytes | builtins.str | None"))]
        encryption_key: Option<PyStreamKey<'_>>,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, builtins.str] | None"))]
        raw: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        Ok(Self {
            inner: StreamCreateOptions {
                encryption_key: encryption_key
                    .map(StreamEncryptionKey::try_from)
                    .transpose()?,
                raw: raw.unwrap_or_default(),
            },
        })
    }

    /// Whether an encryption key is set, without exposing the key itself.
    #[getter]
    fn has_encryption_key(&self) -> bool {
        self.inner.encryption_key.is_some()
    }

    fn __repr__(&self) -> String {
        let encryption_key = if self.inner.encryption_key.is_some() {
            "..."
        } else {
            "None"
        };
        let secret_key: &str = RustOptionKey::EncryptionKey.into();
        let raw = self
            .inner
            .raw
            .iter()
            .map(|(key, value)| {
                let shown = if key == secret_key {
                    "...".to_owned()
                } else {
                    format!("{value:?}")
                };
                format!("{key:?}: {shown}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        format!("StreamOptions(encryption_key={encryption_key}, raw={{{raw}}})")
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl TopicOptions {
    /// Constructs topic create options.
    ///
    /// Every setting left as `None` resolves against the server default at
    /// admission. Only `message_expiry`, `max_topic_size` and the name can
    /// change after creation, through `update_topic`.
    ///
    /// Args:
    ///     partitions_count: Number of partitions as `int`, at most 1000.
    ///         Defaults to 1.
    ///     compression_algorithm: Compression algorithm as `str | None`.
    ///     message_expiry: Message expiry as `IggyExpiry | None`.
    ///         `IggyExpiry.ServerDefault()` is the same as `None`.
    ///     max_topic_size: Maximum topic size as `MaxTopicSize | None`.
    ///         `MaxTopicSize.ServerDefault()` is the same as `None`.
    ///     segment_size: Per-topic segment size in bytes as `int | None`.
    ///     durability: Message completion policy, defaulting to replicated.
    ///     consumer_offset_durability: Independent offset policy, defaulting
    ///         to replicated.
    ///     partition_resize_policy: `PartitionResizePolicy.Fixed` makes the
    ///         server reject later partition creation and deletion on this
    ///         topic.
    ///     messages_required_to_save: Message-count flush threshold as
    ///         `int | None`.
    ///     size_of_messages_required_to_save: Byte flush threshold as
    ///         `int | None`.
    ///     preallocate_segments: Reserve segment bytes on open as `bool | None`.
    ///     raw: Additional option keys as `dict[str, str]`, sent as string
    ///         values so a key newer than this build can be set. A typed field
    ///         wins on collision.
    ///
    /// Raises:
    ///     ValueError: If `message_expiry` or `max_topic_size` is out of range.
    ///     RuntimeError: If `compression_algorithm` names no known algorithm.
    ///     TypeError: If a durability is not a `Durability`.
    #[new]
    #[pyo3(
        signature = (*, partitions_count = 1, compression_algorithm = None, message_expiry = None, max_topic_size = None, segment_size = None, durability = None, consumer_offset_durability = None, partition_resize_policy = None, messages_required_to_save = None, size_of_messages_required_to_save = None, preallocate_segments = None, raw = None)
    )]
    #[allow(clippy::too_many_arguments)]
    fn new(
        partitions_count: u32,
        #[gen_stub(override_type(type_repr = "builtins.str | None"))] compression_algorithm: Option<
            String,
        >,
        #[gen_stub(override_type(type_repr = "IggyExpiry | None"))] message_expiry: Option<
            &IggyExpiry,
        >,
        #[gen_stub(override_type(type_repr = "MaxTopicSize | None"))] max_topic_size: Option<
            &MaxTopicSize,
        >,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))] segment_size: Option<u64>,
        #[gen_stub(override_type(type_repr = "Durability | None"))] durability: Option<
            &Bound<'_, PyAny>,
        >,
        #[gen_stub(override_type(type_repr = "Durability | None"))]
        consumer_offset_durability: Option<&Bound<'_, PyAny>>,
        #[gen_stub(override_type(type_repr = "PartitionResizePolicy | None"))]
        partition_resize_policy: Option<PartitionResizePolicy>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))]
        messages_required_to_save: Option<u32>,
        #[gen_stub(override_type(type_repr = "builtins.int | None"))]
        size_of_messages_required_to_save: Option<u64>,
        #[gen_stub(override_type(type_repr = "builtins.bool | None"))] preallocate_segments: Option<
            bool,
        >,
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, builtins.str] | None"))]
        raw: Option<BTreeMap<String, String>>,
    ) -> PyResult<Self> {
        let compression_algorithm = compression_algorithm
            .map(|name| CompressionAlgorithm::from_str(&name).map_err(PyRuntimeError::new_err))
            .transpose()?;
        // `None` is what tells admission to resolve the server default, so the
        // explicit sentinels collapse back to it.
        let message_expiry = message_expiry
            .map(RustIggyExpiry::try_from)
            .transpose()?
            .filter(|expiry| *expiry != RustIggyExpiry::ServerDefault);
        let max_topic_size = max_topic_size
            .map(RustMaxTopicSize::try_from)
            .transpose()?
            .filter(|size| *size != RustMaxTopicSize::ServerDefault);
        Ok(Self {
            inner: TopicCreateOptions {
                partitions_count: Some(partitions_count),
                compression_algorithm,
                message_expiry,
                max_topic_size,
                segment_size: segment_size.map(IggyByteSize::from),
                durability: Durability::try_from(durability)?.0,
                consumer_offset_durability: Durability::try_from(consumer_offset_durability)?.0,
                partition_resize_policy: partition_resize_policy
                    .map(RustPartitionResizePolicy::from),
                messages_required_to_save,
                size_of_messages_required_to_save: size_of_messages_required_to_save
                    .map(IggyByteSize::from),
                preallocate_segments,
                raw: raw.unwrap_or_default(),
            },
        })
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl UserOptions {
    /// Constructs user create options.
    ///
    /// Args:
    ///     raw: Option keys as `dict[str, str]`, sent as string values so a
    ///         key newer than this build can be set. A current server rejects
    ///         every key.
    #[new]
    #[pyo3(signature = (*, raw=None))]
    fn new(
        #[gen_stub(override_type(type_repr = "builtins.dict[builtins.str, builtins.str] | None"))]
        raw: Option<BTreeMap<String, String>>,
    ) -> Self {
        Self {
            inner: UserCreateOptions {
                raw: raw.unwrap_or_default(),
            },
        }
    }
}

#[gen_stub_pymethods]
#[pymethods]
impl OptionKey {
    fn __str__(&self) -> &'static str {
        RustOptionKey::from(*self).into()
    }
}

/// Fingerprint of a stream encryption key: the first 8 bytes of SHA-256 over
/// the label `iggy/stream-key-fingerprint/v1` followed by the key, as 16
/// lowercase hex characters. Streams report the same value in
/// `encryption_key_fingerprint`, so a caller can match a key to its stream
/// without the server ever returning the key.
///
/// Args:
///     key: Exactly 32 bytes, or their canonical standard padded base64 text.
///
/// Raises:
///     ValueError: If `key` is not exactly 32 bytes or their canonical base64
///         text. The message never contains the key.
///     TypeError: If `key` is neither `bytes` nor `str`.
#[gen_stub_pyfunction]
#[pyfunction]
pub fn encryption_key_fingerprint(
    #[gen_stub(override_type(type_repr = "builtins.bytes | builtins.str"))] key: PyStreamKey<'_>,
) -> PyResult<String> {
    StreamEncryptionKey::try_from(key).map(|key| key.fingerprint())
}

impl From<OptionKey> for RustOptionKey {
    fn from(key: OptionKey) -> Self {
        match key {
            OptionKey::Name => Self::Name,
            OptionKey::Username => Self::Username,
            OptionKey::Status => Self::Status,
            OptionKey::CompressionAlgorithm => Self::CompressionAlgorithm,
            OptionKey::MessageExpiry => Self::MessageExpiry,
            OptionKey::MaxTopicSize => Self::MaxTopicSize,
            OptionKey::SegmentSize => Self::SegmentSize,
            OptionKey::Durability => Self::Durability,
            OptionKey::ConsumerOffsetDurability => Self::ConsumerOffsetDurability,
            OptionKey::PartitionResizePolicy => Self::PartitionResizePolicy,
            OptionKey::MessagesRequiredToSave => Self::MessagesRequiredToSave,
            OptionKey::SizeOfMessagesRequiredToSave => Self::SizeOfMessagesRequiredToSave,
            OptionKey::PreallocateSegments => Self::PreallocateSegments,
            OptionKey::EncryptionKey => Self::EncryptionKey,
            OptionKey::EncryptionKeySource => Self::EncryptionKeySource,
            OptionKey::EncryptionKeyFingerprint => Self::EncryptionKeyFingerprint,
            OptionKey::EncryptionKekFingerprint => Self::EncryptionKekFingerprint,
        }
    }
}

impl From<PartitionResizePolicy> for RustPartitionResizePolicy {
    fn from(policy: PartitionResizePolicy) -> Self {
        match policy {
            PartitionResizePolicy::Mutable => Self::Mutable,
            PartitionResizePolicy::Fixed => Self::Fixed,
        }
    }
}

impl TryFrom<PyStreamKey<'_>> for StreamEncryptionKey {
    type Error = PyErr;

    fn try_from(key: PyStreamKey<'_>) -> PyResult<Self> {
        match key {
            PyStreamKey::Raw(bytes) => Self::try_from(bytes.as_bytes()),
            PyStreamKey::Base64(text) => Self::from_str(text.to_str()?),
        }
        .map_err(|error: IggyError| PyValueError::new_err(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::OptionSpec;
    use crate::stream::StreamDetails;
    use crate::topic::TopicDetails;
    use crate::user::UserInfoDetails;
    use iggy::prelude::{
        StreamDetails as RustStreamDetails, TopicDetails as RustTopicDetails,
        UserInfoDetails as RustUserInfoDetails,
    };
    use iggy_binary_protocol::WireDecode;
    use iggy_binary_protocol::responses::streams::GetStreamResponse;
    use iggy_binary_protocol::responses::system::DescribeOptionsResponse;
    use iggy_binary_protocol::responses::topics::GetTopicResponse;
    use iggy_binary_protocol::responses::users::UserDetailsResponse;
    use iggy_common::wire_conversions::option_specs_from_wire;
    use pyo3::types::PyDict;
    use serde_json::Value;

    const FIXTURE: &str = include_str!("../tests/fixtures/resource_options.json");

    #[test]
    fn given_stream_response_vectors_when_reading_should_match_projections() {
        Python::initialize();
        Python::attach(|py| {
            for vector in vectors("stream_responses") {
                let bytes = decode_hex(&vector["response_hex"]);
                let (response, _) = GetStreamResponse::decode(&bytes).expect("stream decodes");
                let details = RustStreamDetails::try_from(response).expect("stream converts");
                let stream = Bound::new(py, StreamDetails::from(details)).expect("stream wraps");
                let expected = &vector["expected"];

                assert_eq!(
                    attribute::<Option<String>>(stream.as_any(), "encryption_key_source"),
                    expected["encryption_key_source"]
                        .as_str()
                        .map(str::to_owned)
                );
                assert_eq!(
                    attribute::<Option<String>>(stream.as_any(), "encryption_key_fingerprint"),
                    expected["encryption_key_fingerprint"]
                        .as_str()
                        .map(str::to_owned)
                );
                assert_eq!(
                    attribute::<Vec<String>>(stream.as_any(), "encryption_kek_fingerprints"),
                    serde_json::from_value::<Vec<String>>(
                        expected["encryption_kek_fingerprints"].clone()
                    )
                    .expect("fingerprint list")
                );
                assert_options(stream.as_any(), &expected["options"]);
            }
        });
    }

    #[test]
    fn given_topic_and_user_response_vectors_when_reading_should_match_projections() {
        Python::initialize();
        Python::attach(|py| {
            for vector in vectors("topic_responses") {
                let bytes = decode_hex(&vector["response_hex"]);
                let (response, _) = GetTopicResponse::decode(&bytes).expect("topic decodes");
                let details = RustTopicDetails::try_from(response).expect("topic converts");
                let topic = Bound::new(py, TopicDetails::from(details)).expect("topic wraps");
                let expected = &vector["expected"];

                assert_eq!(
                    attribute::<String>(topic.as_any(), "compression_algorithm"),
                    expected["compression_algorithm"]
                        .as_str()
                        .expect("compression")
                );
                let message_expiry = topic.getattr("message_expiry").expect("expiry");
                let duration = message_expiry
                    .getattr("duration")
                    .expect("custom expiry")
                    .extract::<std::time::Duration>()
                    .expect("duration");
                assert_eq!(
                    duration.as_micros().to_string(),
                    expected["message_expiry"].as_str().expect("expiry")
                );
                let max_topic_size = topic.getattr("max_topic_size").expect("max size");
                assert!(max_topic_size.is_instance_of::<crate::topic::MaxTopicSize>());
                assert_eq!(
                    expected["max_topic_size"].as_str(),
                    Some(u64::MAX.to_string().as_str())
                );
                assert!(
                    max_topic_size.getattr("bytes").is_err(),
                    "the unlimited cap carries no byte count"
                );
                assert_options(topic.as_any(), &expected["options"]);
            }

            for vector in vectors("user_responses") {
                let bytes = decode_hex(&vector["response_hex"]);
                let (response, _) = UserDetailsResponse::decode(&bytes).expect("user decodes");
                let details = RustUserInfoDetails::try_from(response).expect("user converts");
                let user = Bound::new(py, UserInfoDetails::from(details)).expect("user wraps");
                let expected = &vector["expected"];

                assert_eq!(
                    attribute::<String>(user.as_any(), "username"),
                    expected["username"].as_str().expect("username")
                );
                assert_options(user.as_any(), &expected["options"]);
            }
        });
    }

    #[test]
    fn given_describe_options_vectors_when_reading_should_expose_every_flag() {
        Python::initialize();
        let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
        let flags = &fixture["constants"]["option_flags"];
        Python::attach(|py| {
            for vector in vectors("describe_options") {
                let bytes = decode_hex(&vector["response_hex"]);
                let (response, _) =
                    DescribeOptionsResponse::decode(&bytes).expect("describe decodes");
                let specs = option_specs_from_wire(response).expect("specs convert");
                let entries = vector["entries"].as_array().expect("entries");
                assert_eq!(specs.len(), entries.len());

                for (spec, entry) in specs.into_iter().zip(entries) {
                    let spec = Bound::new(py, OptionSpec::from(spec)).expect("spec wraps");
                    assert_eq!(
                        attribute::<String>(spec.as_any(), "key"),
                        entry["key"].as_str().expect("key")
                    );
                    let bits = entry["flags"].as_u64().expect("flags");
                    for flag in ["updatable", "resettable", "secret", "read_only", "critical"] {
                        let mask = flags[flag].as_u64().expect("flag bit");
                        assert_eq!(
                            attribute::<bool>(spec.as_any(), flag),
                            bits & mask != 0,
                            "{flag}"
                        );
                    }
                }
            }
        });
    }

    fn vectors(section: &str) -> Vec<Value> {
        let fixture: Value = serde_json::from_str(FIXTURE).expect("fixture parses");
        fixture[section].as_array().expect("vector list").clone()
    }

    fn attribute<'py, T: FromPyObjectOwned<'py>>(object: &Bound<'py, PyAny>, name: &str) -> T {
        object
            .getattr(name)
            .expect("attribute exists")
            .extract::<T>()
            .map_err(Into::<PyErr>::into)
            .expect("attribute extracts")
    }

    /// Compare both provenance getters with the fixture's option map.
    fn assert_options(object: &Bound<'_, PyAny>, expected: &Value) {
        let py = object.py();
        let expected = expected.as_object().expect("option map");
        for (getter, explicit) in [("options", true), ("derived_options", false)] {
            let actual = object
                .getattr(getter)
                .and_then(|headers| headers.call_method0("to_scalar_dict"))
                .expect("scalar options");
            let actual = actual.cast::<PyDict>().expect("dict");
            let wanted = PyDict::new(py);
            for (key, option) in expected
                .iter()
                .filter(|(_, option)| option["explicit"].as_bool() == Some(explicit))
            {
                let text = option["value"].as_str().expect("value text");
                let value: Bound<'_, PyAny> = match option["kind"].as_str() {
                    Some("uint64") => text
                        .parse::<u64>()
                        .expect("u64")
                        .into_pyobject(py)
                        .expect("int")
                        .into_any(),
                    Some("uint32") => text
                        .parse::<u32>()
                        .expect("u32")
                        .into_pyobject(py)
                        .expect("int")
                        .into_any(),
                    Some("bool") => text
                        .parse::<bool>()
                        .expect("bool")
                        .into_pyobject(py)
                        .expect("bool")
                        .to_owned()
                        .into_any(),
                    _ => PyString::new(py, text).into_any(),
                };
                wanted.set_item(key, value).expect("set item");
            }
            assert!(
                actual.eq(&wanted).expect("comparable"),
                "{getter}: {actual} != {wanted}"
            );
        }
    }

    fn decode_hex(text: &Value) -> Vec<u8> {
        let text = text.as_str().expect("hex text");
        (0..text.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex byte"))
            .collect()
    }
}
