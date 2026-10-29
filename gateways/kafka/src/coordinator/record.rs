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

//! The records of the coordinator log and of the owner topic.
//!
//! Each record starts with a format version, a kind, its claim and its checkpoint. Every version
//! keeps that header, and a claim's body starts with its node id in every version. A gateway reads
//! the header of every record and the body of the kinds it knows, so during a rolling upgrade an
//! older gateway still sees a newer gateway's claim. Every number is little endian.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use thiserror::Error;

use crate::group::{GroupSnapshot, MemberSnapshot};

/// The format this gateway writes and the newest it reads.
pub const FORMAT_VERSION: u8 = 1;

/// The checkpoint field of a record written before any checkpoint completed.
pub const NO_CHECKPOINT: u64 = u64::MAX;

const KIND_CLAIM: u8 = 1;
const KIND_CHECKPOINT_BEGIN: u8 = 2;
const KIND_CHECKPOINT_END: u8 = 3;
const KIND_GROUP: u8 = 4;
const KIND_OFFSET: u8 = 5;
const KIND_GROUP_OFFSETS_DELETED: u8 = 6;
const KIND_TOPIC_OFFSETS_DELETED: u8 = 7;

const KIND_ANNOUNCEMENT: u8 = 1;

/// Version, kind, claim and checkpoint.
const HEADER_BYTES: usize = 1 + 1 + 8 + 8;

/// One record of a coordinator log partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The claim of the gateway that wrote the record. A record whose claim is below a claim that
    /// came before it in the partition is void.
    pub claim: u64,
    /// The offset where the last checkpoint that the writer saw complete begins, or
    /// [`NO_CHECKPOINT`]. A load starts there.
    pub checkpoint: u64,
    pub body: Body,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// `node_id` claims the partition with the claim number of the record. The first claim with
    /// the highest number owns it.
    Claim { node_id: u16 },
    /// Every live group and offset of the partition follows, up to [`Body::CheckpointEnd`]. Names
    /// its writer, since a load that starts here reads no claim before it.
    CheckpointBegin { node_id: u16 },
    /// The checkpoint that began at offset `begin` is complete.
    CheckpointEnd { begin: u64 },
    /// The state of a group after a rebalance, or `None` once it has no members.
    Group {
        group_id: String,
        snapshot: Option<GroupSnapshot>,
    },
    /// The offset a group committed for one partition, or `None` once it is deleted.
    Offset {
        group_id: String,
        topic: String,
        partition: i32,
        committed: Option<CommittedOffset>,
    },
    /// Every offset of the group is deleted.
    GroupOffsetsDeleted { group_id: String },
    /// Iggy deleted the topic, so every offset of it, in every group, is gone.
    TopicOffsetsDeleted { topic: String },
    /// A kind or a version this gateway does not know. Only its header counts.
    Unknown { kind: u8 },
}

/// One committed offset, with what Kafka keeps beside it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedOffset {
    pub offset: i64,
    pub leader_epoch: i32,
    pub metadata: Option<String>,
    pub commit_timestamp_ms: i64,
}

/// A gateway owns one coordinator log partition. Written to the owner topic, which every gateway
/// reads to route `FindCoordinator` and to name partition leaders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Announcement {
    pub partition: u32,
    pub claim: u64,
    pub node_id: u16,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RecordError {
    #[error("the record ends before its {0}")]
    Truncated(&'static str),
    #[error("the record's {0} is not UTF-8")]
    NotUtf8(&'static str),
    #[error("the record's {0} does not fit its length prefix")]
    TooLong(&'static str),
}

impl Record {
    /// # Errors
    ///
    /// [`RecordError::TooLong`] when a string or a blob is over its length prefix.
    pub fn encode(&self) -> Result<Bytes, RecordError> {
        let mut buf = BytesMut::with_capacity(HEADER_BYTES + 64);
        buf.put_u8(FORMAT_VERSION);
        buf.put_u8(self.body.kind());
        buf.put_u64_le(self.claim);
        buf.put_u64_le(self.checkpoint);
        match &self.body {
            Body::Claim { node_id } | Body::CheckpointBegin { node_id } => {
                buf.put_u16_le(*node_id);
            }
            Body::CheckpointEnd { begin } => buf.put_u64_le(*begin),
            Body::Group { group_id, snapshot } => {
                put_str(&mut buf, group_id, "group id")?;
                match snapshot {
                    None => buf.put_u8(0),
                    Some(snapshot) => {
                        buf.put_u8(1);
                        put_snapshot(&mut buf, snapshot)?;
                    }
                }
            }
            Body::Offset {
                group_id,
                topic,
                partition,
                committed,
            } => {
                put_str(&mut buf, group_id, "group id")?;
                put_str(&mut buf, topic, "topic")?;
                buf.put_i32_le(*partition);
                match committed {
                    None => buf.put_u8(0),
                    Some(committed) => {
                        buf.put_u8(1);
                        buf.put_i64_le(committed.offset);
                        buf.put_i32_le(committed.leader_epoch);
                        put_optional_str(&mut buf, committed.metadata.as_deref(), "metadata")?;
                        buf.put_i64_le(committed.commit_timestamp_ms);
                    }
                }
            }
            Body::GroupOffsetsDeleted { group_id } => put_str(&mut buf, group_id, "group id")?,
            Body::TopicOffsetsDeleted { topic } => put_str(&mut buf, topic, "topic")?,
            Body::Unknown { .. } => {}
        }
        Ok(buf.freeze())
    }

    /// The record in `bytes`. A kind or a version this gateway does not know gives
    /// [`Body::Unknown`], except a claim, whose body every version starts the same way.
    ///
    /// # Errors
    ///
    /// [`RecordError`] when the header or a known body is cut short, or a string is not UTF-8.
    pub fn decode(mut bytes: &[u8]) -> Result<Self, RecordError> {
        let version = get_u8(&mut bytes, "version")?;
        let kind = get_u8(&mut bytes, "kind")?;
        let claim = get_u64(&mut bytes, "claim")?;
        let checkpoint = get_u64(&mut bytes, "checkpoint")?;
        let body = match kind {
            KIND_CLAIM => Body::Claim {
                node_id: get_u16(&mut bytes, "node id")?,
            },
            _ if version != FORMAT_VERSION => Body::Unknown { kind },
            KIND_CHECKPOINT_BEGIN => Body::CheckpointBegin {
                node_id: get_u16(&mut bytes, "node id")?,
            },
            KIND_CHECKPOINT_END => Body::CheckpointEnd {
                begin: get_u64(&mut bytes, "checkpoint begin")?,
            },
            KIND_GROUP => {
                let group_id = get_str(&mut bytes, "group id")?;
                let snapshot = match get_u8(&mut bytes, "snapshot flag")? {
                    0 => None,
                    _ => Some(get_snapshot(&mut bytes)?),
                };
                Body::Group { group_id, snapshot }
            }
            KIND_OFFSET => {
                let group_id = get_str(&mut bytes, "group id")?;
                let topic = get_str(&mut bytes, "topic")?;
                let partition = get_i32(&mut bytes, "partition")?;
                let committed = match get_u8(&mut bytes, "offset flag")? {
                    0 => None,
                    _ => Some(CommittedOffset {
                        offset: get_i64(&mut bytes, "offset")?,
                        leader_epoch: get_i32(&mut bytes, "leader epoch")?,
                        metadata: get_optional_str(&mut bytes, "metadata")?,
                        commit_timestamp_ms: get_i64(&mut bytes, "commit timestamp")?,
                    }),
                };
                Body::Offset {
                    group_id,
                    topic,
                    partition,
                    committed,
                }
            }
            KIND_GROUP_OFFSETS_DELETED => Body::GroupOffsetsDeleted {
                group_id: get_str(&mut bytes, "group id")?,
            },
            KIND_TOPIC_OFFSETS_DELETED => Body::TopicOffsetsDeleted {
                topic: get_str(&mut bytes, "topic")?,
            },
            _ => Body::Unknown { kind },
        };
        Ok(Self {
            claim,
            checkpoint,
            body,
        })
    }
}

impl Body {
    const fn kind(&self) -> u8 {
        match self {
            Self::Claim { .. } => KIND_CLAIM,
            Self::CheckpointBegin { .. } => KIND_CHECKPOINT_BEGIN,
            Self::CheckpointEnd { .. } => KIND_CHECKPOINT_END,
            Self::Group { .. } => KIND_GROUP,
            Self::Offset { .. } => KIND_OFFSET,
            Self::GroupOffsetsDeleted { .. } => KIND_GROUP_OFFSETS_DELETED,
            Self::TopicOffsetsDeleted { .. } => KIND_TOPIC_OFFSETS_DELETED,
            Self::Unknown { kind } => *kind,
        }
    }

    /// The group the record changes, if any.
    #[must_use]
    pub fn group_id(&self) -> Option<&str> {
        match self {
            Self::Group { group_id, .. }
            | Self::Offset { group_id, .. }
            | Self::GroupOffsetsDeleted { group_id } => Some(group_id),
            Self::Claim { .. }
            | Self::CheckpointBegin { .. }
            | Self::CheckpointEnd { .. }
            | Self::TopicOffsetsDeleted { .. }
            | Self::Unknown { .. } => None,
        }
    }
}

impl Announcement {
    #[must_use]
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(1 + 1 + 4 + 8 + 2);
        buf.put_u8(FORMAT_VERSION);
        buf.put_u8(KIND_ANNOUNCEMENT);
        buf.put_u32_le(self.partition);
        buf.put_u64_le(self.claim);
        buf.put_u16_le(self.node_id);
        buf.freeze()
    }

    /// The announcement in `bytes`, or `None` for a version or a kind this gateway does not know.
    ///
    /// # Errors
    ///
    /// [`RecordError::Truncated`] when a known announcement is cut short.
    pub fn decode(mut bytes: &[u8]) -> Result<Option<Self>, RecordError> {
        let version = get_u8(&mut bytes, "version")?;
        let kind = get_u8(&mut bytes, "kind")?;
        if version != FORMAT_VERSION || kind != KIND_ANNOUNCEMENT {
            return Ok(None);
        }
        Ok(Some(Self {
            partition: get_u32(&mut bytes, "partition")?,
            claim: get_u64(&mut bytes, "claim")?,
            node_id: get_u16(&mut bytes, "node id")?,
        }))
    }
}

fn put_snapshot(buf: &mut BytesMut, snapshot: &GroupSnapshot) -> Result<(), RecordError> {
    buf.put_i32_le(snapshot.generation_id);
    put_str(buf, &snapshot.protocol_type, "protocol type")?;
    put_optional_str(buf, snapshot.protocol_name.as_deref(), "protocol name")?;
    put_optional_str(buf, snapshot.leader.as_deref(), "leader")?;
    let members =
        u32::try_from(snapshot.members.len()).map_err(|_| RecordError::TooLong("members"))?;
    buf.put_u32_le(members);
    for member in &snapshot.members {
        put_str(buf, &member.member_id, "member id")?;
        put_optional_str(buf, member.group_instance_id.as_deref(), "instance id")?;
        put_str(buf, &member.client_id, "client id")?;
        put_str(buf, &member.client_host, "client host")?;
        buf.put_u32_le(member.session_timeout_ms);
        buf.put_u32_le(member.rebalance_timeout_ms);
        put_blob(buf, &member.metadata, "member metadata")?;
        put_blob(buf, &member.assignment, "member assignment")?;
    }
    Ok(())
}

fn get_snapshot(bytes: &mut &[u8]) -> Result<GroupSnapshot, RecordError> {
    let generation_id = get_i32(bytes, "generation")?;
    let protocol_type = get_str(bytes, "protocol type")?;
    let protocol_name = get_optional_str(bytes, "protocol name")?;
    let leader = get_optional_str(bytes, "leader")?;
    let count = get_u32(bytes, "member count")?;
    // Each member takes at least 22 bytes, so a count the record cannot hold allocates nothing.
    let mut members = Vec::with_capacity((count as usize).min(bytes.len() / 22));
    for _ in 0..count {
        members.push(MemberSnapshot {
            member_id: get_str(bytes, "member id")?,
            group_instance_id: get_optional_str(bytes, "instance id")?,
            client_id: get_str(bytes, "client id")?,
            client_host: get_str(bytes, "client host")?,
            session_timeout_ms: get_u32(bytes, "session timeout")?,
            rebalance_timeout_ms: get_u32(bytes, "rebalance timeout")?,
            metadata: get_blob(bytes, "member metadata")?,
            assignment: get_blob(bytes, "member assignment")?,
        });
    }
    Ok(GroupSnapshot {
        generation_id,
        protocol_type,
        protocol_name,
        leader,
        members,
    })
}

fn put_str(buf: &mut BytesMut, value: &str, field: &'static str) -> Result<(), RecordError> {
    let len = u16::try_from(value.len()).map_err(|_| RecordError::TooLong(field))?;
    buf.put_u16_le(len);
    buf.put_slice(value.as_bytes());
    Ok(())
}

/// Length `u16::MAX` stands for `None`, which no Kafka string of these fields reaches.
fn put_optional_str(
    buf: &mut BytesMut,
    value: Option<&str>,
    field: &'static str,
) -> Result<(), RecordError> {
    match value {
        None => {
            buf.put_u16_le(u16::MAX);
            Ok(())
        }
        Some(value) if value.len() >= usize::from(u16::MAX) => Err(RecordError::TooLong(field)),
        Some(value) => put_str(buf, value, field),
    }
}

fn put_blob(buf: &mut BytesMut, value: &[u8], field: &'static str) -> Result<(), RecordError> {
    let len = u32::try_from(value.len()).map_err(|_| RecordError::TooLong(field))?;
    buf.put_u32_le(len);
    buf.put_slice(value);
    Ok(())
}

fn get_str(bytes: &mut &[u8], field: &'static str) -> Result<String, RecordError> {
    let len = usize::from(get_u16(bytes, field)?);
    take_str(bytes, len, field)
}

fn get_optional_str(bytes: &mut &[u8], field: &'static str) -> Result<Option<String>, RecordError> {
    match get_u16(bytes, field)? {
        u16::MAX => Ok(None),
        len => take_str(bytes, usize::from(len), field).map(Some),
    }
}

fn take_str(bytes: &mut &[u8], len: usize, field: &'static str) -> Result<String, RecordError> {
    if bytes.len() < len {
        return Err(RecordError::Truncated(field));
    }
    let (value, rest) = bytes.split_at(len);
    *bytes = rest;
    String::from_utf8(value.to_vec()).map_err(|_| RecordError::NotUtf8(field))
}

fn get_blob(bytes: &mut &[u8], field: &'static str) -> Result<Bytes, RecordError> {
    let len = get_u32(bytes, field)? as usize;
    if bytes.len() < len {
        return Err(RecordError::Truncated(field));
    }
    let (value, rest) = bytes.split_at(len);
    *bytes = rest;
    Ok(Bytes::copy_from_slice(value))
}

fn get_u8(bytes: &mut &[u8], field: &'static str) -> Result<u8, RecordError> {
    if bytes.remaining() < 1 {
        return Err(RecordError::Truncated(field));
    }
    Ok(bytes.get_u8())
}

fn get_u16(bytes: &mut &[u8], field: &'static str) -> Result<u16, RecordError> {
    if bytes.remaining() < 2 {
        return Err(RecordError::Truncated(field));
    }
    Ok(bytes.get_u16_le())
}

fn get_u32(bytes: &mut &[u8], field: &'static str) -> Result<u32, RecordError> {
    if bytes.remaining() < 4 {
        return Err(RecordError::Truncated(field));
    }
    Ok(bytes.get_u32_le())
}

fn get_i32(bytes: &mut &[u8], field: &'static str) -> Result<i32, RecordError> {
    if bytes.remaining() < 4 {
        return Err(RecordError::Truncated(field));
    }
    Ok(bytes.get_i32_le())
}

fn get_u64(bytes: &mut &[u8], field: &'static str) -> Result<u64, RecordError> {
    if bytes.remaining() < 8 {
        return Err(RecordError::Truncated(field));
    }
    Ok(bytes.get_u64_le())
}

fn get_i64(bytes: &mut &[u8], field: &'static str) -> Result<i64, RecordError> {
    if bytes.remaining() < 8 {
        return Err(RecordError::Truncated(field));
    }
    Ok(bytes.get_i64_le())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> GroupSnapshot {
        GroupSnapshot {
            generation_id: 7,
            protocol_type: "consumer".to_string(),
            protocol_name: Some("range".to_string()),
            leader: Some("member-1".to_string()),
            members: vec![
                MemberSnapshot {
                    member_id: "member-1".to_string(),
                    group_instance_id: None,
                    client_id: "consumer-orders-1".to_string(),
                    client_host: "/192.0.2.10".to_string(),
                    session_timeout_ms: 45_000,
                    rebalance_timeout_ms: 300_000,
                    metadata: Bytes::from_static(b"subscription"),
                    assignment: Bytes::from_static(b"assignment"),
                },
                MemberSnapshot {
                    member_id: "member-2".to_string(),
                    group_instance_id: Some("static-2".to_string()),
                    client_id: String::new(),
                    client_host: String::new(),
                    session_timeout_ms: 10_000,
                    rebalance_timeout_ms: 60_000,
                    metadata: Bytes::new(),
                    assignment: Bytes::new(),
                },
            ],
        }
    }

    #[test]
    fn given_each_kind_of_record_when_encoded_should_decode_to_the_same_record() {
        let bodies = [
            Body::Claim { node_id: 3 },
            Body::CheckpointBegin { node_id: 3 },
            Body::CheckpointEnd { begin: 42 },
            Body::Group {
                group_id: "orders".to_string(),
                snapshot: Some(snapshot()),
            },
            Body::Group {
                group_id: "orders".to_string(),
                snapshot: None,
            },
            Body::Offset {
                group_id: "orders".to_string(),
                topic: "payments".to_string(),
                partition: 4,
                committed: Some(CommittedOffset {
                    offset: 99,
                    leader_epoch: -1,
                    metadata: Some("partition time 17".to_string()),
                    commit_timestamp_ms: 1_700_000_000_000,
                }),
            },
            Body::Offset {
                group_id: "orders".to_string(),
                topic: "payments".to_string(),
                partition: 4,
                committed: None,
            },
            Body::GroupOffsetsDeleted {
                group_id: "orders".to_string(),
            },
            Body::TopicOffsetsDeleted {
                topic: "payments".to_string(),
            },
        ];
        for body in bodies {
            let record = Record {
                claim: 5,
                checkpoint: NO_CHECKPOINT,
                body,
            };
            let bytes = record.encode().expect("encodes");
            assert_eq!(Record::decode(&bytes), Ok(record));
        }
    }

    #[test]
    fn given_a_newer_version_when_decoded_should_keep_its_header_and_read_a_claim() {
        let newer = |body: Body| {
            let mut bytes = Record {
                claim: 4,
                checkpoint: 9,
                body,
            }
            .encode()
            .unwrap()
            .to_vec();
            bytes[0] = FORMAT_VERSION + 1;
            // A newer body can grow past what this version reads.
            bytes.extend_from_slice(b"newer");
            Record::decode(&bytes)
        };
        assert_eq!(
            newer(Body::Claim { node_id: 2 }),
            Ok(Record {
                claim: 4,
                checkpoint: 9,
                body: Body::Claim { node_id: 2 },
            })
        );
        assert_eq!(
            newer(Body::CheckpointEnd { begin: 1 }),
            Ok(Record {
                claim: 4,
                checkpoint: 9,
                body: Body::Unknown {
                    kind: KIND_CHECKPOINT_END
                },
            })
        );
    }

    #[test]
    fn given_an_unknown_kind_when_decoded_should_keep_its_header() {
        let mut bytes = Record {
            claim: 1,
            checkpoint: 0,
            body: Body::CheckpointEnd { begin: 3 },
        }
        .encode()
        .unwrap()
        .to_vec();
        bytes[1] = 200;
        assert_eq!(
            Record::decode(&bytes),
            Ok(Record {
                claim: 1,
                checkpoint: 0,
                body: Body::Unknown { kind: 200 },
            })
        );
    }

    #[test]
    fn given_a_cut_record_when_decoded_should_fail() {
        let record = Record {
            claim: 1,
            checkpoint: 0,
            body: Body::Group {
                group_id: "orders".to_string(),
                snapshot: Some(snapshot()),
            },
        };
        let bytes = record.encode().unwrap();
        for len in [0, 1, HEADER_BYTES, bytes.len() - 1] {
            assert!(
                Record::decode(&bytes[..len]).is_err(),
                "a record cut to {len} bytes must fail"
            );
        }
    }

    #[test]
    fn given_an_announcement_when_encoded_should_decode_to_the_same_one() {
        let announcement = Announcement {
            partition: 15,
            claim: 9,
            node_id: 2,
        };
        assert_eq!(
            Announcement::decode(&announcement.encode()),
            Ok(Some(announcement))
        );
    }
}
