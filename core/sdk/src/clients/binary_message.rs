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

use crate::prelude::IggyClient;
use async_trait::async_trait;
use bytes::Bytes;
use iggy_common::locking::IggyRwLockFn;
use iggy_common::{BinarySendPartitioning, MessageClient};
use iggy_common::{
    Consumer, Identifier, IggyError, IggyMessage, Partitioning, PollOptions, PolledMessages,
    PollingStrategy, SendMessagesResponse, ServerCommitPolicy,
};
use tokio::time::Instant;

#[async_trait]
impl MessageClient for IggyClient {
    /// The call ends at its own deadline of `max_wait` plus 10 s, so a wait of up to 600 s
    /// outlives the operation timeout of the other calls.
    ///
    /// With an encryptor, `ServerCommitPolicy::OnRead` fails with
    /// [`IggyError::InvalidConfiguration`] before anything is sent: the server would store the
    /// offset of a batch before the client decrypts it, so a batch that fails to decrypt would
    /// be lost.
    async fn poll_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: u32,
        consumer: &Consumer,
        strategy: &PollingStrategy,
        options: PollOptions,
    ) -> Result<PolledMessages, IggyError> {
        if options.server_commit == ServerCommitPolicy::OnRead && self.encryptor.is_some() {
            return Err(IggyError::InvalidConfiguration);
        }
        let deadline = Instant::now()
            .checked_add(options.request_timeout().get_duration())
            .ok_or(IggyError::InvalidCommand)?;
        let mut polled_messages = super::with_client!(self, deadline, client => client.poll_messages(
                stream_id,
                topic_id,
                partition_id,
                consumer,
                strategy,
                options,
            ).await)?;

        if let Some(ref encryptor) = self.encryptor {
            for message in &mut polled_messages.messages {
                encryptor.decrypt_message(message)?;
            }
        }

        Ok(polled_messages)
    }

    async fn send_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &Partitioning,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        self.send_messages_with_partitioning(
            stream_id,
            topic_id,
            &mut BinarySendPartitioning::new(partitioning),
            messages,
        )
        .await
    }
}

impl IggyClient {
    pub(crate) async fn send_messages_with_partitioning(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitioning: &mut BinarySendPartitioning<'_>,
        messages: &mut [IggyMessage],
    ) -> Result<SendMessagesResponse, IggyError> {
        if messages.is_empty() {
            return Err(IggyError::InvalidMessagesCount);
        }

        if !partitioning.is_resolved()
            && let Some(encryptor) = &self.encryptor
        {
            for message in &mut *messages {
                message.payload = Bytes::from(encryptor.encrypt(&message.payload)?);
                message.header.payload_length = message.payload.len() as u32;

                if let Some(ref user_headers) = message.user_headers {
                    let encrypted_headers = encryptor.encrypt(user_headers)?;
                    message.header.user_headers_length = encrypted_headers.len() as u32;
                    message.user_headers = Some(Bytes::from(encrypted_headers));
                }
            }
        }

        super::with_client!(self, client => client.send_messages_with_partitioning(stream_id, topic_id, partitioning, messages).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client_wrappers::client_wrapper::ClientWrapper;
    use crate::links::OPERATION_TIMEOUT;
    use crate::links::fake_cluster::{Answer, FakeCluster, connected, endpoint};
    use crate::tcp::tcp_client::TcpClient;
    use iggy_binary_protocol::codes::POLL_MESSAGES_DEFERRED_CODE;
    use iggy_binary_protocol::requests::messages::DeferredPollMessagesRequest;
    use iggy_binary_protocol::responses::messages::{
        PartitionRouteResponse, PollMessagesResponseHeader,
    };
    use iggy_binary_protocol::{WireDecode, WireEncode};
    use iggy_common::{Aes256GcmEncryptor, EncryptorKind, IggyDuration, PartitionContext};
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use tokio::sync::Notify;
    use tokio::time::sleep;

    const PARTITION_ID: u32 = 0;
    const ENCRYPTION_KEY: [u8; 32] = [1; 32];

    /// A cluster of one replica that routes the partition.
    fn routed_cluster() -> Arc<FakeCluster> {
        let cluster = FakeCluster::new(1);
        *cluster.route.lock().unwrap() = Some(PartitionRouteResponse {
            incarnation: 7,
            metadata_op: 11,
            view: 0,
            partitions_count: 1,
            primary_replica: 0,
        });
        cluster
    }

    fn empty_reply() -> Vec<u8> {
        PollMessagesResponseHeader {
            partition_id: PARTITION_ID,
            current_offset: 0,
            messages_count: 0,
            context: PartitionContext::default(),
        }
        .to_bytes()
        .to_vec()
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_wait_past_the_operation_timeout_when_polling_should_wait_for_the_reply() {
        let reply_after = OPERATION_TIMEOUT * 2;
        let max_wait = OPERATION_TIMEOUT * 3;
        let cluster = routed_cluster();
        let release = Arc::new(Notify::new());
        cluster.script(
            &endpoint(0),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::GatedBody {
                release: Arc::clone(&release),
                completed: Arc::new(AtomicBool::new(false)),
                body: empty_reply(),
            }],
        );
        let client = IggyClient::new(ClientWrapper::Tcp(TcpClient::from_links(
            connected(&cluster, 1).await,
        )));
        let id = Identifier::numeric(1).unwrap();
        let consumer = Consumer::new(id.clone());
        let strategy = PollingStrategy::offset(0);
        let start = Instant::now();

        let (result, ()) = tokio::join!(
            client.poll_messages(
                &id,
                &id,
                PARTITION_ID,
                &consumer,
                &strategy,
                PollOptions::wait(max_wait.into()),
            ),
            async {
                sleep(reply_after).await;
                release.notify_one();
            }
        );

        let polled = result.expect("the poll must outlive the operation timeout of other calls");
        assert_eq!(polled.partition_id, PARTITION_ID);
        assert_eq!(start.elapsed(), reply_after);
        let request = DeferredPollMessagesRequest::decode_from(
            &cluster.received(POLL_MESSAGES_DEFERRED_CODE)[0].body,
        )
        .unwrap();
        assert_eq!(request.wait_us, IggyDuration::from(max_wait).as_micros());
    }

    /// The server would store the offset of an `OnRead` batch before the client decrypts it.
    #[tokio::test(start_paused = true)]
    async fn given_an_encryptor_when_polling_with_on_read_should_refuse_it_before_sending() {
        let cluster = routed_cluster();
        cluster.script(
            &endpoint(0),
            POLL_MESSAGES_DEFERRED_CODE,
            [Answer::Body(empty_reply())],
        );
        let encryptor = EncryptorKind::Aes256Gcm(Aes256GcmEncryptor::new(&ENCRYPTION_KEY).unwrap());
        let client = IggyClient::create(
            ClientWrapper::Tcp(TcpClient::from_links(connected(&cluster, 1).await)),
            None,
            Some(Arc::new(encryptor)),
        );
        let id = Identifier::numeric(1).unwrap();
        let consumer = Consumer::new(id.clone());
        let next = PollingStrategy::next();
        let poll = |server_commit| {
            client.poll_messages(
                &id,
                &id,
                PARTITION_ID,
                &consumer,
                &next,
                PollOptions {
                    server_commit,
                    ..PollOptions::default()
                },
            )
        };

        let refused = poll(ServerCommitPolicy::OnRead).await;
        let sent_after_refusal = cluster.count(POLL_MESSAGES_DEFERRED_CODE);
        let polled = poll(ServerCommitPolicy::Disabled).await;

        assert_eq!(refused.map(drop), Err(IggyError::InvalidConfiguration));
        assert_eq!(sent_after_refusal, 0);
        assert_eq!(polled.map(|polled| polled.count), Ok(0));
        assert_eq!(cluster.count(POLL_MESSAGES_DEFERRED_CODE), 1);
    }
}
