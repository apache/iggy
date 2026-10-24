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

use crate::traits::binary_auth::fail_if_not_authenticated;
use crate::wire_conversions::clients_from_wire;
use crate::{
    BinaryClient, ClientInfo, ClientInfoDetails, IggyError, MetadataChanges, MetadataChangesQuery,
    NonZeroIggyDuration, OptionSpec, OptionsScope, Snapshot, SnapshotCompression, Stats,
    SystemClient, SystemSnapshotType,
};
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    DESCRIBE_OPTIONS_CODE, GET_CLIENT_CODE, GET_CLIENTS_CODE, GET_ME_CODE,
    GET_METADATA_CHANGES_CODE, GET_SNAPSHOT_FILE_CODE, GET_STATS_CODE, PING_CODE,
};
use iggy_binary_protocol::requests::system::{
    DescribeOptionsRequest, GetClientRequest, GetClientsRequest, GetMeRequest,
    GetMetadataChangesRequest, GetSnapshotRequest, GetStatsRequest, PingRequest,
};
use iggy_binary_protocol::responses::clients::get_client::ClientDetailsResponse;
use iggy_binary_protocol::responses::clients::get_clients::GetClientsResponse;
use iggy_binary_protocol::responses::system::DescribeOptionsResponse;
use iggy_binary_protocol::responses::system::GetMetadataChangesResponse;
use iggy_binary_protocol::responses::system::get_stats::StatsResponse;

#[async_trait::async_trait]
impl<B: BinaryClient> SystemClient for B {
    async fn get_stats(&self) -> Result<Stats, IggyError> {
        let response = self
            .send_raw_with_response(GET_STATS_CODE, GetStatsRequest.to_bytes())
            .await?;
        let wire_resp = super::decode_response::<StatsResponse>(&response)?;
        Ok(Stats::from(wire_resp))
    }

    async fn describe_options(&self, scope: OptionsScope) -> Result<Vec<OptionSpec>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(
                DESCRIBE_OPTIONS_CODE,
                DescribeOptionsRequest {
                    scope: scope.as_code(),
                }
                .to_bytes(),
            )
            .await?;
        let wire_resp = super::decode_response::<DescribeOptionsResponse>(&response)?;
        crate::wire_conversions::option_specs_from_wire(wire_resp)
    }

    async fn get_me(&self) -> Result<ClientInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(GET_ME_CODE, GetMeRequest.to_bytes())
            .await?;
        let wire_resp = super::decode_response::<ClientDetailsResponse>(&response)?;
        Ok(ClientInfoDetails::from(wire_resp))
    }

    async fn get_client(&self, client_id: u32) -> Result<Option<ClientInfoDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(GET_CLIENT_CODE, GetClientRequest { client_id }.to_bytes())
            .await?;
        if response.is_empty() {
            return Ok(None);
        }
        let wire_resp = super::decode_response::<ClientDetailsResponse>(&response)?;
        Ok(Some(ClientInfoDetails::from(wire_resp)))
    }

    async fn get_clients(&self) -> Result<Vec<ClientInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(GET_CLIENTS_CODE, GetClientsRequest.to_bytes())
            .await?;
        if response.is_empty() {
            return Ok(Vec::new());
        }
        let wire_resp = super::decode_response::<GetClientsResponse>(&response)?;
        Ok(clients_from_wire(wire_resp))
    }

    async fn ping(&self) -> Result<(), IggyError> {
        self.send_raw_with_response(PING_CODE, PingRequest.to_bytes())
            .await?;
        Ok(())
    }

    async fn heartbeat_interval(&self) -> NonZeroIggyDuration {
        self.get_heartbeat_interval()
    }

    async fn refresh_consumer_group_assignments(&self) {
        super::messages::refresh_group_assignments(self).await;
    }

    async fn snapshot(
        &self,
        compression: SnapshotCompression,
        snapshot_types: Vec<SystemSnapshotType>,
    ) -> Result<Snapshot, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(
                GET_SNAPSHOT_FILE_CODE,
                GetSnapshotRequest {
                    compression: compression.as_code(),
                    snapshot_types: snapshot_types
                        .iter()
                        .map(SystemSnapshotType::as_code)
                        .collect(),
                }
                .to_bytes(),
            )
            .await?;
        let snapshot = Snapshot::new(response.to_vec());
        Ok(snapshot)
    }

    async fn get_metadata_changes(
        &self,
        query: &MetadataChangesQuery,
    ) -> Result<MetadataChanges, IggyError> {
        fail_if_not_authenticated(self).await?;
        query.validate()?;
        let response = self
            .send_raw_with_response(
                GET_METADATA_CHANGES_CODE,
                GetMetadataChangesRequest::from(query).to_bytes(),
            )
            .await?;
        let wire_resp = super::decode_response::<GetMetadataChangesResponse>(&response)?;
        MetadataChanges::try_from(wire_resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BinaryTransport, Client, ClientState, ConsumerGroupClientState, DiagnosticEvent,
        MetadataChangeStart, MetadataChangeStatus, PartitionContext, VsrSessionControl,
        VsrSessionSealed,
    };
    use async_broadcast::Receiver;
    use bytes::Bytes;
    use iggy_binary_protocol::WireName;
    use iggy_binary_protocol::primitives::metadata_changes::{
        WireChangeOptions, WireMetadataChange, WireMetadataChangeCursor, WireMetadataChangePhase,
        WireMetadataChangeStatus, WireStreamBody, WireStreamKey,
    };
    use iggy_binary_protocol::requests::messages::PollMessagesRequest;
    use iggy_binary_protocol::requests::system::SessionIdentity;
    use iggy_binary_protocol::requests::users::login_register::BindSecret;
    use iggy_binary_protocol::responses::system::WireMetadataChangeRecord;
    use iggy_binary_protocol::{WireDecode, WireOptions};
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn given_query_the_rules_refuse_when_reading_changes_should_not_send_it() {
        let client = ReplyingClient::default();

        let refused = client
            .get_metadata_changes(&MetadataChangesQuery {
                limit: 0,
                ..MetadataChangesQuery::default()
            })
            .await;

        assert_eq!(refused, Err(IggyError::InvalidCommand));
        assert!(client.sent.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn given_change_page_when_reading_changes_should_send_code_17_and_decode_the_page() {
        let body = WireMetadataChange::StreamCreated(WireStreamBody {
            key: WireStreamKey {
                stream_id: 3,
                stream_created_op: 41,
            },
            created_at: 7,
            name: WireName::new("orders").expect("valid name"),
            options: WireChangeOptions::Readable {
                explicit: WireOptions::empty(),
                derived: WireOptions::empty(),
            },
        })
        .body_bytes();
        let page = GetMetadataChangesResponse {
            status: WireMetadataChangeStatus::Ok,
            phase: WireMetadataChangePhase::Changes,
            log_id: Some(0x0d4b),
            oldest_op: 1,
            retained_since: 0,
            head_op: 41,
            next: Some(WireMetadataChangeCursor {
                log_id: 0x0d4b,
                op: 41,
                checksum: 0x41,
                ordinal: 0,
            }),
            has_more: false,
            records: vec![WireMetadataChangeRecord {
                op: 41,
                ordinal: 0,
                checksum: 0x41,
                timestamp: 7,
                actor_user_id: Some(0),
                group: 1,
                kind: 1,
                body,
            }],
            items: Vec::new(),
            current_page_token: None,
        };
        let client = ReplyingClient {
            reply: page.to_bytes(),
            ..ReplyingClient::default()
        };
        let query = MetadataChangesQuery {
            start: MetadataChangeStart::Latest,
            ..MetadataChangesQuery::default()
        };

        let changes = client
            .get_metadata_changes(&query)
            .await
            .expect("a decoded page");

        let sent = client.sent.lock().expect("lock").clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].0, GET_METADATA_CHANGES_CODE);
        assert_eq!(
            GetMetadataChangesRequest::decode_from(&sent[0].1).expect("a request"),
            GetMetadataChangesRequest::from(&query)
        );
        assert_eq!(changes.status, MetadataChangeStatus::Ok);
        assert_eq!(
            changes,
            MetadataChanges::try_from(page).expect("valid page")
        );
    }

    /// An authenticated binary client that records each request and answers
    /// with `reply`.
    #[derive(Debug, Default)]
    struct ReplyingClient {
        sent: Mutex<Vec<(u32, Bytes)>>,
        reply: Bytes,
        consumer_groups: Arc<ConsumerGroupClientState>,
    }

    #[async_trait::async_trait]
    impl BinaryTransport for ReplyingClient {
        async fn get_state(&self) -> ClientState {
            ClientState::Authenticated
        }

        async fn set_state(&self, _state: ClientState) {}

        async fn publish_event(&self, _event: DiagnosticEvent) {}

        async fn send_raw_with_context(
            &self,
            code: u32,
            payload: Bytes,
            _context: PartitionContext,
        ) -> Result<Bytes, IggyError> {
            self.sent.lock().expect("lock").push((code, payload));
            Ok(self.reply.clone())
        }

        async fn send_offset_write_with_response(
            &self,
            _code: u32,
            _payload: Bytes,
            _context: Option<PartitionContext>,
        ) -> Result<Bytes, IggyError> {
            Err(IggyError::FeatureUnavailable)
        }

        async fn send_poll_with_response(
            &self,
            _request: &PollMessagesRequest,
            _context: Option<PartitionContext>,
        ) -> Result<Bytes, IggyError> {
            Err(IggyError::FeatureUnavailable)
        }

        fn get_heartbeat_interval(&self) -> NonZeroIggyDuration {
            NonZeroIggyDuration::ONE_SECOND
        }

        fn consumer_group_state(&self) -> Arc<ConsumerGroupClientState> {
            Arc::clone(&self.consumer_groups)
        }
    }

    #[async_trait::async_trait]
    impl Client for ReplyingClient {
        async fn connect(&self) -> Result<(), IggyError> {
            Ok(())
        }

        async fn disconnect(&self) -> Result<(), IggyError> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), IggyError> {
            Ok(())
        }

        async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
            async_broadcast::broadcast(1).1
        }
    }

    impl VsrSessionSealed for ReplyingClient {}

    #[async_trait::async_trait]
    impl VsrSessionControl for ReplyingClient {
        async fn session_bind_secret(&self) -> Result<BindSecret, IggyError> {
            Err(IggyError::FeatureUnavailable)
        }

        async fn session_identity(&self) -> Result<SessionIdentity, IggyError> {
            Err(IggyError::FeatureUnavailable)
        }

        async fn bind_vsr_session(&self, _session: u64) -> Result<(), IggyError> {
            Ok(())
        }

        async fn reset_vsr_session(&self) -> Result<(), IggyError> {
            Ok(())
        }

        fn sdk_version(&self) -> &'static str {
            env!("CARGO_PKG_VERSION")
        }
    }

    impl BinaryClient for ReplyingClient {}
}
