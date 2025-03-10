use crate::bytes_serializable::BytesSerializable;
use crate::command::{Command, DELETE_SEGMENTS_CODE};
use crate::error::IggyError;
use crate::identifier::Identifier;
use crate::utils::sizeable::Sizeable;
use crate::validatable::Validatable;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

use super::MAX_SEGMENTS_COUNT;

type SegmentsCountType = u32;

/// `DeletePartitions` command is used to delete partitions from a topic.
/// It has additional payload:
/// - `stream_id` - unique stream ID (numeric or name).
/// - `topic_id` - unique topic ID (numeric or name).
/// - `partitions_count` - number of partitions in the topic to delete, max value is 1000.
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DeleteSegments {
    /// Unique stream ID (numeric or name).
    #[serde(skip)]
    pub stream_id: Identifier,
    /// Unique topic ID (numeric or name).
    #[serde(skip)]
    pub topic_id: Identifier,
    /// Unique partition ID (numeric or name).
    #[serde(skip)]
    pub partition_id: Identifier,
    /// Number of partitions in the topic to delete, max value is 1000.
    pub segments_count: SegmentsCountType,
}

impl Command for DeleteSegments {
    fn code(&self) -> u32 {
        DELETE_SEGMENTS_CODE
    }
}

impl Default for DeleteSegments {
    fn default() -> Self {
        DeleteSegments {
            segments_count: 1,
            stream_id: Identifier::default(),
            topic_id: Identifier::default(),
            partition_id: Identifier::default(),
        }
    }
}

impl Validatable<IggyError> for DeleteSegments {
    fn validate(&self) -> Result<(), IggyError> {
        if !(1..=MAX_SEGMENTS_COUNT).contains(&self.segments_count) {
            return Err(IggyError::TooManySegments);
        }

        Ok(())
    }
}

impl BytesSerializable for DeleteSegments {
    fn to_bytes(&self) -> Bytes {
        let stream_id_bytes = self.stream_id.to_bytes();
        let topic_id_bytes = self.topic_id.to_bytes();
        let partition_id_bytes = self.partition_id.to_bytes();
        let mut bytes = BytesMut::with_capacity(
            std::mem::size_of::<SegmentsCountType>()
                + stream_id_bytes.len()
                + topic_id_bytes.len()
                + partition_id_bytes.len(),
        );
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_slice(&partition_id_bytes);
        bytes.put_u32_le(self.segments_count);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> std::result::Result<DeleteSegments, IggyError> {
        if bytes.len() < 10 {
            return Err(IggyError::InvalidCommand);
        }

        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone())?;
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += topic_id.get_size_bytes().as_bytes_usize();
        let partition_id = Identifier::from_bytes(bytes.slice(position..))?;
        position += partition_id.get_size_bytes().as_bytes_usize();
        let segments_count = SegmentsCountType::from_le_bytes(
            bytes[position..position + std::mem::size_of::<SegmentsCountType>()]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)?,
        );
        let command = DeleteSegments {
            stream_id,
            topic_id,
            partition_id,
            segments_count,
        };
        Ok(command)
    }
}

impl Display for DeleteSegments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}|{}|{}|{}",
            self.stream_id, self.topic_id, self.partition_id, self.segments_count
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn should_be_serialized_as_bytes() {
        let command = DeleteSegments {
            stream_id: Identifier::numeric(1).unwrap(),
            topic_id: Identifier::numeric(2).unwrap(),
            partition_id: Identifier::numeric(3).unwrap(),
            segments_count: 3,
        };

        let bytes = command.to_bytes();
        let mut position = 0;
        let stream_id = Identifier::from_bytes(bytes.clone()).unwrap();
        position += stream_id.get_size_bytes().as_bytes_usize();
        let topic_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
        position += topic_id.get_size_bytes().as_bytes_usize();
        let partition_id = Identifier::from_bytes(bytes.slice(position..)).unwrap();
        position += partition_id.get_size_bytes().as_bytes_usize();
        let segments_count = SegmentsCountType::from_le_bytes(
            bytes[position..position + std::mem::size_of::<SegmentsCountType>()]
                .try_into()
                .map_err(|_| IggyError::InvalidNumberEncoding)
                .unwrap(),
        );

        assert!(!bytes.is_empty());
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partition_id, command.partition_id);
        assert_eq!(segments_count, command.segments_count);
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let stream_id = Identifier::numeric(1).unwrap();
        let topic_id = Identifier::numeric(2).unwrap();
        let partition_id = Identifier::numeric(3).unwrap();
        let segments_count = 3u32;
        let stream_id_bytes = stream_id.to_bytes();
        let topic_id_bytes = topic_id.to_bytes();
        let partition_id_bytes = partition_id.to_bytes();
        let mut bytes = BytesMut::with_capacity(
            std::mem::size_of::<SegmentsCountType>()
                + stream_id_bytes.len()
                + topic_id_bytes.len()
                + partition_id_bytes.len(),
        );
        bytes.put_slice(&stream_id_bytes);
        bytes.put_slice(&topic_id_bytes);
        bytes.put_slice(&partition_id_bytes);
        bytes.put_u32_le(segments_count);
        let command = DeleteSegments::from_bytes(bytes.freeze());
        assert!(command.is_ok());

        let command = command.unwrap();
        assert_eq!(stream_id, command.stream_id);
        assert_eq!(topic_id, command.topic_id);
        assert_eq!(partition_id, command.partition_id);
        assert_eq!(segments_count, command.segments_count);
    }
}
