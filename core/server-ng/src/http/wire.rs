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

//! HTTP -> wire request mappers: produce/poll/consumer-offset encoders and
//! the control-plane [`Message<RequestHeader>`] builder shared by the write path.

use bytes::{Bytes, BytesMut};
use iggy_binary_protocol::consensus::{Command2, HEADER_SIZE};
use iggy_binary_protocol::primitives::consumer::WireConsumer;
use iggy_binary_protocol::primitives::polling_strategy::WirePollingStrategy;
use iggy_binary_protocol::requests::consumer_offsets::{
    DeleteConsumerOffset2Request, GetConsumerOffsetRequest, StoreConsumerOffset2Request,
};
use iggy_binary_protocol::requests::messages::{
    PollMessagesRequest, RawMessage, SendMessagesEncoder,
};
use iggy_binary_protocol::{AckLevel, Operation, RequestHeader};
use iggy_common::get_consumer_offset::GetConsumerOffset;
use iggy_common::poll_messages::DEFAULT_PARTITION_ID;
use iggy_common::store_consumer_offset::StoreConsumerOffset;
use iggy_common::wire_conversions::{consumer_to_wire, identifier_to_wire, partitioning_to_wire};
use iggy_common::{
    Consumer, Identifier, IggyError, IggyMessageView, PollMessages, PolledMessages,
    RESYNC_REQUIRED_PARTITION_SENTINEL, SendMessages,
};
use server_common::Message;

/// Encode a validated HTTP `SendMessages` into the `SendMessagesRequest` wire
/// body, mirroring the SDK's TCP produce encode: identifier + partitioning
/// wire conversion, then `RawMessage` borrows into [`SendMessagesEncoder`].
/// Balanced / messages-key partitioning passes through untouched; the
/// dispatch gates resolve it to a concrete partition server-side.
pub(in crate::http) fn encode_send_messages(
    stream_id: &Identifier,
    topic_id: &Identifier,
    command: &SendMessages,
) -> Result<Bytes, IggyError> {
    let wire_stream_id = identifier_to_wire(stream_id)?;
    let wire_topic_id = identifier_to_wire(topic_id)?;
    let wire_partitioning = partitioning_to_wire(&command.partitioning)?;
    // Two passes because the view accessors' return borrows are tied to the
    // view value, not the batch buffer, so the views must outlive the borrows.
    let views: Vec<IggyMessageView<'_>> = command.batch.iter().collect();
    let raw_messages: Vec<RawMessage<'_>> = views
        .iter()
        .map(|view| RawMessage {
            id: view.header().id(),
            origin_timestamp: view.header().origin_timestamp(),
            headers: view.user_headers(),
            payload: view.payload(),
        })
        .collect();
    let size = SendMessagesEncoder::encoded_size(
        &wire_stream_id,
        &wire_topic_id,
        &wire_partitioning,
        &raw_messages,
    );
    let mut buf = BytesMut::with_capacity(size);
    SendMessagesEncoder::encode(
        &mut buf,
        &wire_stream_id,
        &wire_topic_id,
        &wire_partitioning,
        &raw_messages,
    );
    Ok(buf.freeze())
}

/// Map a validated HTTP poll query onto the wire `PollMessagesRequest` the
/// shared TCP resolver consumes, so both transports resolve one request shape.
/// The query's consumer kind is structurally always `Consumer`
/// (`Consumer::kind` is `#[serde(skip)]` - the flattened `kind` param names
/// the polling strategy), matching the legacy HTTP server.
pub(in crate::http) fn poll_wire_request(
    stream_id: &Identifier,
    topic_id: &Identifier,
    query: &PollMessages,
) -> Result<PollMessagesRequest, IggyError> {
    Ok(PollMessagesRequest {
        consumer: WireConsumer {
            kind: query.consumer.kind.as_code(),
            id: identifier_to_wire(&query.consumer.id)?,
        },
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        partition_id: query.partition_id,
        strategy: WirePollingStrategy {
            kind: query.strategy.kind.as_code(),
            value: query.strategy.value,
        },
        count: query.count,
        auto_commit: query.auto_commit,
    })
}

/// Map a validated HTTP consumer-offset query onto the wire
/// `GetConsumerOffsetRequest` the shared TCP resolver consumes. An omitted
/// `partition_id` defaults to partition 0, matching the legacy server's
/// `resolve_consumer_with_partition_id` (`partition_id.unwrap_or(0)`).
pub(in crate::http) fn consumer_offset_wire_request(
    stream_id: &Identifier,
    topic_id: &Identifier,
    query: &GetConsumerOffset,
) -> Result<GetConsumerOffsetRequest, IggyError> {
    Ok(GetConsumerOffsetRequest {
        consumer: WireConsumer {
            kind: query.consumer.kind.as_code(),
            id: identifier_to_wire(&query.consumer.id)?,
        },
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        partition_id: Some(query.partition_id.unwrap_or(DEFAULT_PARTITION_ID)),
    })
}

/// Map a validated HTTP store-offset body onto the v2 wire request
/// (`StoreConsumerOffset2Request`), `ack` pinned to `Quorum` so the route can
/// await the committed reply. The body's consumer kind is structurally always
/// `Consumer` (`Consumer::kind` is `#[serde(skip)]`), matching the legacy HTTP
/// server; `partition_id` passes through as the wire `Option` (flag byte +
/// u32) for the server-side resolvers to ground.
pub(in crate::http) fn store_offset_wire_request(
    stream_id: &Identifier,
    topic_id: &Identifier,
    command: &StoreConsumerOffset,
) -> Result<StoreConsumerOffset2Request, IggyError> {
    Ok(StoreConsumerOffset2Request {
        consumer: consumer_to_wire(&command.consumer)?,
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        partition_id: command.partition_id,
        offset: command.offset,
        ack: AckLevel::Quorum,
    })
}

/// Map a validated HTTP delete-offset request onto the v2 wire request
/// (`DeleteConsumerOffset2Request`), `ack` pinned to `Quorum` like
/// [`store_offset_wire_request`].
pub(in crate::http) fn delete_offset_wire_request(
    stream_id: &Identifier,
    topic_id: &Identifier,
    consumer: &Consumer,
    partition_id: Option<u32>,
) -> Result<DeleteConsumerOffset2Request, IggyError> {
    Ok(DeleteConsumerOffset2Request {
        consumer: consumer_to_wire(consumer)?,
        stream_id: identifier_to_wire(stream_id)?,
        topic_id: identifier_to_wire(topic_id)?,
        partition_id,
        ack: AckLevel::Quorum,
    })
}

/// The empty `PolledMessages` a fenced consumer-group poll answers, carrying
/// the re-sync sentinel in `partition_id` exactly as the TCP dispatch replies
/// it, so an SDK re-syncs its assignment instead of reading end-of-partition.
pub(in crate::http) const fn resync_required_polled_messages() -> PolledMessages {
    PolledMessages {
        partition_id: RESYNC_REQUIRED_PARTITION_SENTINEL,
        current_offset: 0,
        count: 0,
        messages: Vec::new(),
    }
}

/// Build a `Message<RequestHeader>` for a control-plane write by filling a zeroed
/// `#[repr(C)]` header, mirroring `wire::rewrite_request_body` and the partition
/// reconciler's prepare builder. `body` is the already-encoded wire request,
/// copied in after the header.
pub(in crate::http) fn build_request_message(
    operation: Operation,
    client_id: u128,
    session_id: u64,
    request_id: u64,
    body: &[u8],
) -> Message<RequestHeader> {
    let total = HEADER_SIZE + body.len();
    let mut message = Message::<RequestHeader>::new(total);
    message.as_mut_slice()[HEADER_SIZE..].copy_from_slice(body);
    let header = bytemuck::checked::try_from_bytes_mut::<RequestHeader>(
        &mut message.as_mut_slice()[..HEADER_SIZE],
    )
    .expect("zeroed bytes form a valid RequestHeader");
    header.command = Command2::Request;
    header.operation = operation;
    header.client = client_id;
    header.session = session_id;
    header.request = request_id;
    header.size = u32::try_from(total).expect("control-plane message size fits u32");
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::Query;
    use axum::http::Uri;

    use iggy_binary_protocol::WireDecode;
    use iggy_binary_protocol::WireEncode;
    use iggy_binary_protocol::WireMessageIterator;
    use iggy_binary_protocol::message_layout::WIRE_MESSAGE_INDEX_SIZE;
    use iggy_binary_protocol::requests::messages::SendMessagesHeader;
    use iggy_common::delete_consumer_offset::DeleteConsumerOffset;
    use iggy_common::{
        Consumer, ConsumerKind, IggyMessage, IggyMessagesBatch, IggyTimestamp, Partitioning,
        PartitioningKind, PollingKind, PollingStrategy, Validatable,
    };
    use partitions::{Fragment, PollFragments};
    use server_common::MESSAGE_ALIGN;
    use server_common::iobuf::Owned;
    use server_common::send_messages2::{
        COMMAND_HEADER_SIZE, IggyMessage2, IggyMessage2Header, IggyMessages2, SendMessages2Header,
        SendMessages2Owned,
    };

    use crate::http::error::{Consistency, ConsistencyQuery};
    use crate::responses::build_polled_messages_body;

    fn produce_command(partitioning: Partitioning) -> SendMessages {
        let first = IggyMessage::builder()
            .id(7)
            .payload(Bytes::from_static(b"first"))
            .build()
            .expect("valid message");
        // Raw pre-encoded user headers, mirroring the HTTP deserializer's
        // base64 branch.
        let mut second = IggyMessage::builder()
            .id(8)
            .payload(Bytes::from_static(b"second"))
            .build()
            .expect("valid message");
        let raw_headers = Bytes::from_static(b"raw-header-bytes");
        second.header.user_headers_length =
            u32::try_from(raw_headers.len()).expect("test headers fit u32");
        second.user_headers = Some(raw_headers);
        let messages = vec![first, second];
        SendMessages {
            partitioning,
            batch: IggyMessagesBatch::from(&messages),
            ..Default::default()
        }
    }

    #[test]
    fn encode_send_messages_round_trips_through_wire_decoders() {
        let stream_id = Identifier::from_str_value("1").expect("valid stream id");
        let topic_id = Identifier::from_str_value("orders").expect("valid topic id");
        let command = produce_command(Partitioning::partition_id(3));
        let origin_timestamps: Vec<u64> = command
            .batch
            .iter()
            .map(|view| view.header().origin_timestamp())
            .collect();

        let bytes = encode_send_messages(&stream_id, &topic_id, &command).expect("encodes");

        let metadata_length =
            u32::from_le_bytes(bytes[..4].try_into().expect("length prefix")) as usize;
        let (header, consumed) =
            SendMessagesHeader::decode(&bytes[4..4 + metadata_length]).expect("valid metadata");
        assert_eq!(consumed, metadata_length);
        assert_eq!(header.stream_id, identifier_to_wire(&stream_id).unwrap());
        assert_eq!(header.topic_id, identifier_to_wire(&topic_id).unwrap());
        assert_eq!(
            header.partitioning,
            partitioning_to_wire(&command.partitioning).unwrap()
        );
        assert_eq!(header.messages_count, 2);

        let data_offset = 4 + metadata_length + 2 * WIRE_MESSAGE_INDEX_SIZE;
        let views: Vec<_> = WireMessageIterator::new(&bytes[data_offset..], 2)
            .collect::<Result<Vec<_>, _>>()
            .expect("valid message frames");
        assert_eq!(views[0].id(), 7);
        assert_eq!(views[0].payload(), b"first");
        assert_eq!(views[0].user_headers(), b"");
        assert_eq!(views[0].origin_timestamp(), origin_timestamps[0]);
        assert_eq!(views[1].id(), 8);
        assert_eq!(views[1].payload(), b"second");
        assert_eq!(views[1].user_headers(), b"raw-header-bytes");
        assert_eq!(views[1].origin_timestamp(), origin_timestamps[1]);
    }

    #[test]
    fn send_messages_validate_rejects_oversized_partitioning_key() {
        let command = produce_command(Partitioning {
            kind: PartitioningKind::MessagesKey,
            length: 0,
            value: vec![0u8; 256],
        });
        assert!(command.validate().is_err());
    }

    #[test]
    fn poll_query_parses_with_documented_defaults() {
        let uri: Uri = "/streams/1/topics/1/messages?consumer_id=42"
            .parse()
            .expect("valid uri");
        let Query(query) = Query::<PollMessages>::try_from_uri(&uri).expect("parses");
        assert_eq!(query.consumer.kind, ConsumerKind::Consumer);
        assert_eq!(
            query.consumer.id,
            Identifier::numeric(42).expect("valid id")
        );
        assert_eq!(query.partition_id, Some(0));
        assert_eq!(query.strategy, PollingStrategy::offset(0));
        assert_eq!(query.count, 10);
        assert!(!query.auto_commit);
    }

    #[test]
    fn poll_query_parses_explicit_strategy_count_and_tolerates_consistency_param() {
        let uri: Uri = "/x?consumer_id=app&partition_id=3&kind=timestamp&value=42&count=5&auto_commit=true&consistency=linearizable"
            .parse()
            .expect("valid uri");
        let Query(query) = Query::<PollMessages>::try_from_uri(&uri).expect("parses");
        assert_eq!(
            query.consumer.id,
            Identifier::named("app").expect("valid id")
        );
        assert_eq!(query.partition_id, Some(3));
        assert_eq!(
            query.strategy,
            PollingStrategy::timestamp(IggyTimestamp::from(42))
        );
        assert_eq!(query.count, 5);
        assert!(query.auto_commit);
        let Query(consistency) = Query::<ConsistencyQuery>::try_from_uri(&uri).expect("parses");
        assert!(consistency.consistency == Consistency::Linearizable);
    }

    #[test]
    fn poll_wire_request_maps_query_onto_tcp_request_shape() {
        let stream_id = Identifier::from_str_value("orders").expect("valid stream id");
        let topic_id = Identifier::from_str_value("1").expect("valid topic id");
        let query = PollMessages {
            consumer: Consumer {
                kind: ConsumerKind::Consumer,
                id: Identifier::numeric(9).expect("valid id"),
            },
            partition_id: Some(4),
            strategy: PollingStrategy::next(),
            count: 25,
            auto_commit: true,
            ..Default::default()
        };
        let wire = poll_wire_request(&stream_id, &topic_id, &query).expect("maps");
        assert_eq!(wire.consumer.kind, 1);
        assert_eq!(
            wire.consumer.id,
            identifier_to_wire(&query.consumer.id).unwrap()
        );
        assert_eq!(wire.stream_id, identifier_to_wire(&stream_id).unwrap());
        assert_eq!(wire.topic_id, identifier_to_wire(&topic_id).unwrap());
        assert_eq!(wire.partition_id, Some(4));
        assert_eq!(wire.strategy.kind, PollingKind::Next.as_code());
        assert_eq!(wire.strategy.value, 0);
        assert_eq!(wire.count, 25);
        assert!(wire.auto_commit);
    }

    #[test]
    fn consumer_offset_wire_request_defaults_omitted_partition_to_zero() {
        let stream_id = Identifier::numeric(1).expect("valid stream id");
        let topic_id = Identifier::numeric(1).expect("valid topic id");
        let query = GetConsumerOffset {
            consumer: Consumer::new(Identifier::numeric(7).expect("valid id")),
            partition_id: None,
        };
        let wire = consumer_offset_wire_request(&stream_id, &topic_id, &query).expect("maps");
        assert_eq!(wire.partition_id, Some(DEFAULT_PARTITION_ID));
        assert_eq!(wire.consumer.kind, 1);
    }

    #[test]
    fn store_offset_wire_request_round_trips_with_quorum_ack() {
        let stream_id = Identifier::numeric(1).expect("valid stream id");
        let topic_id = Identifier::named("orders").expect("valid topic id");
        let command = StoreConsumerOffset {
            consumer: Consumer::new(Identifier::named("c1").expect("valid id")),
            partition_id: Some(1),
            offset: 42,
        };
        let request = store_offset_wire_request(&stream_id, &topic_id, &command).expect("maps");
        let bytes = request.to_bytes();
        assert_eq!(*bytes.last().expect("non-empty"), AckLevel::Quorum.as_u8());
        let (decoded, consumed) =
            StoreConsumerOffset2Request::decode(&bytes).expect("decodes as the server does");
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, request);
        assert_eq!(decoded.consumer.kind, 1);
        assert_eq!(decoded.partition_id, Some(1));
        assert_eq!(decoded.offset, 42);
        assert_eq!(decoded.ack, AckLevel::Quorum);
    }

    #[test]
    fn store_offset_wire_request_passes_omitted_partition_through() {
        let stream_id = Identifier::numeric(1).expect("valid stream id");
        let topic_id = Identifier::numeric(2).expect("valid topic id");
        let command = StoreConsumerOffset {
            consumer: Consumer::new(Identifier::numeric(7).expect("valid id")),
            partition_id: None,
            offset: u64::MAX,
        };
        let request = store_offset_wire_request(&stream_id, &topic_id, &command).expect("maps");
        let bytes = request.to_bytes();
        let (decoded, consumed) =
            StoreConsumerOffset2Request::decode(&bytes).expect("decodes as the server does");
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded.partition_id, None);
        assert_eq!(decoded.offset, u64::MAX);
        assert_eq!(decoded.ack, AckLevel::Quorum);
    }

    #[test]
    fn delete_offset_wire_request_round_trips_partition_variants() {
        let stream_id = Identifier::named("stream-1").expect("valid stream id");
        let topic_id = Identifier::numeric(2).expect("valid topic id");
        for partition_id in [Some(1), None] {
            let request = delete_offset_wire_request(
                &stream_id,
                &topic_id,
                &Consumer::new(Identifier::named("c1").expect("valid id")),
                partition_id,
            )
            .expect("maps");
            let bytes = request.to_bytes();
            assert_eq!(*bytes.last().expect("non-empty"), AckLevel::Quorum.as_u8());
            let (decoded, consumed) =
                DeleteConsumerOffset2Request::decode(&bytes).expect("decodes as the server does");
            assert_eq!(consumed, bytes.len());
            assert_eq!(decoded, request);
            assert_eq!(decoded.consumer.kind, 1);
            assert_eq!(decoded.partition_id, partition_id);
            assert_eq!(decoded.ack, AckLevel::Quorum);
        }
    }

    #[test]
    fn delete_offset_query_parses_optional_partition_id() {
        let uri: Uri = "/x?partition_id=3".parse().expect("valid uri");
        let Query(query) = Query::<DeleteConsumerOffset>::try_from_uri(&uri).expect("parses");
        assert_eq!(query.partition_id, Some(3));
        let bare: Uri = "/x".parse().expect("valid uri");
        let Query(query) = Query::<DeleteConsumerOffset>::try_from_uri(&bare).expect("parses");
        assert_eq!(query.partition_id, None);
    }

    /// Wrap one stored `SendMessages2` batch (`[256B header][blob]`) as the
    /// poll fragment the owning shard replies, the shape
    /// `build_polled_messages_body` consumes.
    fn fragment_from_stored_batch(header: &SendMessages2Header, blob: &[u8]) -> PollFragments {
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(COMMAND_HEADER_SIZE + blob.len());
        header.encode_into(buffer.as_mut_slice());
        buffer.as_mut_slice()[COMMAND_HEADER_SIZE..].copy_from_slice(blob);
        let mut fragments = PollFragments::new();
        fragments.push(Fragment::whole(buffer.into()));
        fragments
    }

    /// Round-trip the poll route's encode/decode seam: the store's own batch
    /// writer (`SendMessages2Owned::from_messages`) is the encoder oracle,
    /// `build_polled_messages_body` re-encodes to the legacy wire body, and
    /// the SDK's `PolledMessages::from_bytes` must read back every field.
    #[test]
    fn polled_messages_body_decodes_into_common_polled_messages() {
        let mut messages = IggyMessages2::with_capacity(2);
        messages.push(IggyMessage2 {
            header: IggyMessage2Header {
                id: 7,
                origin_timestamp: 1_000,
                ..Default::default()
            },
            payload: Bytes::from_static(b"first"),
            user_headers: None,
        });
        messages.push(IggyMessage2 {
            header: IggyMessage2Header {
                id: 8,
                origin_timestamp: 1_050,
                ..Default::default()
            },
            payload: Bytes::from_static(b"second"),
            user_headers: Some(Bytes::from_static(b"raw-header-bytes")),
        });
        let namespace = server_common::sharding::IggyNamespace::new(0, 0, 3);
        let stored =
            SendMessages2Owned::from_messages(namespace, &messages).expect("encodes stored batch");
        // The store stamps these on append; `from_messages` leaves them zero.
        let mut header = stored.header;
        header.base_offset = 41;
        header.base_timestamp = 999_999;

        let body =
            build_polled_messages_body(3, 42, fragment_from_stored_batch(&header, &stored.blob))
                .expect("re-encodes wire body");
        let polled = PolledMessages::from_bytes(body).expect("decodes as the SDK does");

        assert_eq!(polled.partition_id, 3);
        assert_eq!(polled.current_offset, 42);
        assert_eq!(polled.count, 2);
        assert_eq!(polled.messages.len(), 2);
        assert_eq!(polled.messages[0].header.id, 7);
        assert_eq!(polled.messages[0].header.offset, 41);
        assert_eq!(polled.messages[0].header.timestamp, 999_999);
        assert_eq!(polled.messages[0].header.origin_timestamp, 1_000);
        assert_eq!(polled.messages[0].payload.as_ref(), b"first");
        assert!(polled.messages[0].user_headers.is_none());
        assert_eq!(polled.messages[1].header.id, 8);
        assert_eq!(polled.messages[1].header.offset, 42);
        assert_eq!(polled.messages[1].header.origin_timestamp, 1_050);
        assert_eq!(polled.messages[1].payload.as_ref(), b"second");
        assert_eq!(
            polled.messages[1].user_headers.as_deref(),
            Some(b"raw-header-bytes".as_ref())
        );
    }

    #[test]
    fn resync_required_polled_messages_carries_sentinel_partition() {
        let polled = resync_required_polled_messages();
        assert_eq!(polled.partition_id, RESYNC_REQUIRED_PARTITION_SENTINEL);
        assert_eq!(polled.count, 0);
        assert!(polled.messages.is_empty());
    }
}
