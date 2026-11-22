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

use super::*;

#[test]
fn resolve_acting_user_id_skips_register_and_logout() {
    // Session-lifecycle ops keep the user id their request builder set
    // (Register's login identity, Logout's ungated value); the ClientTable
    // is never consulted, so an empty table still yields `Ok(None)`.
    let client_table = RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX));
    for operation in [Operation::Register, Operation::Logout] {
        assert!(
            matches!(
                resolve_acting_user_id(operation, 1, &client_table),
                Ok(None)
            ),
            "{operation:?} must not be stamped"
        );
    }
}

// The login frame's `client` field is caller-supplied and
// `resolve_acting_user_id` resolves authority from the entry it names, so
// the register ownership gate must refuse an entry owned by another user
// rather than resume the caller onto it. Two shapes reach this: a caller
// presenting someone else's id with its own valid credentials, and an
// honest login landing on a recovered entry after a restart (the HTTP id
// minter restarts at 1 while WAL replay rebuilds the previous boot's
// entries).
#[compio::test]
async fn register_gate_refuses_an_entry_owned_by_another_user() {
    const CLIENT: u128 = 1;
    const OWNER: u32 = 7;
    const IMPOSTOR: u32 = 9;

    let dir = tempfile::tempdir().unwrap();
    let journal =
        journal::prepare_journal::PrepareJournal::open(&dir.path().join("journal.wal"), 0)
            .await
            .unwrap();
    let consensus = VsrConsensus::new(
        1,
        0,
        1,
        server_common::sharding::METADATA_GROUP,
        NoopBus,
        LocalPipeline::new(),
    );
    consensus.init();
    let md: IggyMetadata<_, journal::prepare_journal::PrepareJournal, (), TestMux> =
        IggyMetadata::new(
            Some(consensus),
            Some(journal),
            None,
            None,
            TestMux::new((session_users(), (Streams::default(), ()))),
            None,
        );
    md.client_table
        .borrow_mut()
        .commit_register(CLIENT, OWNER, [0x5a; 32], register_reply(CLIENT, 1))
        .unwrap();

    assert_eq!(
        md.submit_register_in_process(
            CLIENT,
            IMPOSTOR,
            RegisterRequest {
                user_created_at: 1,
                bind_verifier: [0x5a; 32]
            }
        )
        .await,
        Err(MetadataSubmitError::ClientIdOwnedByAnotherUser),
        "a different user must not be resumed onto this entry"
    );
    assert!(
        !MetadataSubmitError::ClientIdOwnedByAnotherUser.is_transient(),
        "the refusal is terminal; retrying anywhere cannot help"
    );
    assert_eq!(
        md.client_table.borrow().get_user_id(CLIENT),
        Some(OWNER),
        "the refused attempt must not rewrite the entry's owner"
    );

    assert_eq!(
        md.submit_register_in_process(
            CLIENT,
            OWNER,
            RegisterRequest {
                user_created_at: 1,
                bind_verifier: [0x5a; 32]
            }
        )
        .await,
        Ok(BoundSession {
            epoch: 1,
            watermark: 0
        })
    );
}

#[compio::test]
async fn given_reserved_client_id_when_registering_in_process_should_reject_before_queueing() {
    const REGISTRATION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
    let metadata = metadata_plane();
    metadata.mux_stm.users().ensure_root_user("root", "hash");
    let consensus = metadata.consensus.as_ref().unwrap();
    for commit_max in [0, 1] {
        consensus.advance_commit_max(commit_max);
        for client in [
            message_bus::server_internal_client_id(server_common::sharding::METADATA_GROUP),
            message_bus::AUTO_COMMIT_CLIENT_ID,
        ] {
            let result = compio::time::timeout(
                REGISTRATION_TIMEOUT,
                metadata.submit_register_in_process(
                    client,
                    0,
                    RegisterRequest {
                        user_created_at: 1,
                        bind_verifier: [0x5a; 32],
                    },
                ),
            )
            .await
            .expect("reserved IDs must be rejected without waiting for consensus");
            let error = result.expect_err("reserved IDs cannot acquire a session");
            assert_eq!(error, MetadataSubmitError::Unauthenticated);
            assert!(!error.is_transient());
            assert_eq!(metadata.client_table.borrow().get_epoch(client), None);
            assert!(!consensus.pipeline_has_message_from_client(client));
        }
    }
}

#[test]
fn resolve_acting_user_id_stamps_from_client_table() {
    // A gated client op takes the acting user from the committed session,
    // independent of any wire-supplied header value.
    const CLIENT: u128 = 1;
    const SESSION: u64 = 10;
    const ACTING_USER: u32 = 7;
    let mut table = ClientTable::new(CLIENTS_TABLE_MAX);
    table
        .commit_register(
            CLIENT,
            ACTING_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .unwrap();
    let client_table = RefCell::new(table);

    match resolve_acting_user_id(Operation::CreateStream, CLIENT, &client_table) {
        Ok(Some(user_id)) => assert_eq!(user_id, ACTING_USER),
        other => panic!("expected Ok(Some({ACTING_USER})), got {other:?}"),
    }
}

#[test]
fn resolve_acting_user_id_fails_closed_for_unknown_session() {
    // A gated client op with no committed session is denied, never
    // defaulted to root (user id 0).
    let client_table = RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX));
    match resolve_acting_user_id(Operation::CreateStream, 999, &client_table) {
        Err(IggyError::Unauthenticated) => {}
        other => panic!("expected Err(Unauthenticated), got {other:?}"),
    }
}

#[compio::test]
async fn given_committed_lifecycle_intent_when_waiting_for_completion_should_not_prove_final_result()
 {
    const CLIENT: u128 = 1;
    const SESSION: u64 = 1;
    const ROOT_USER: u32 = 0;
    let directory = tempfile::tempdir().unwrap();
    let mut metadata = metadata_plane();
    metadata.journal = Some(
        PrepareJournal::open(&directory.path().join("journal.wal"), 0)
            .await
            .unwrap(),
    );
    metadata.mux_stm = Rc::new(TestMux::new((
        users_with_topic_manager(),
        (stream_with_one_partition().into(), ()),
    )));
    metadata
        .client_table
        .borrow_mut()
        .commit_register(
            CLIENT,
            ROOT_USER,
            [0x5a; 32],
            register_reply(CLIENT, SESSION),
        )
        .unwrap();
    let consensus = metadata.consensus.as_ref().unwrap();
    let request_header = RoutedRequestHeader {
        client: CLIENT,
        session: SESSION,
        request: 1,
        user_id: ROOT_USER,
        group: server_common::sharding::METADATA_GROUP,
        ..Default::default()
    };
    let body = DeleteTopicRequest {
        stream_id: WireIdentifier::numeric(0),
        topic_id: WireIdentifier::numeric(0),
    }
    .to_bytes();
    let prepare = consensus::seal_prepare_capacity(
        build_prepare_message(consensus, &request_header, Operation::DeleteTopic, &body),
        consensus.retry_capacity(&metadata.client_table.borrow()),
    );
    let intent_op = prepare.header().op;
    let pending = metadata
        .dispatch_prepare_and_await(consensus, prepare)
        .await
        .unwrap();
    assert!(metadata.mux_stm.streams().lifecycle_pending(intent_op));
    assert_eq!(
        iggy_binary_protocol::result_code(pending.body()),
        Some(IggyError::TransientNotCommitted.as_code())
    );
    assert!(
        !pending.header().has_committed_result(),
        "a committed intent cannot prove its final outcome before every partition finishes"
    );

    metadata
        .submit_complete_lifecycle_in_process(
            CompleteLifecycleRequest::new(
                intent_op,
                0,
                vec![LifecycleCompletion {
                    topic_id: 0,
                    partition_id: 0,
                    partition_op: 1,
                }],
            )
            .unwrap(),
            0,
        )
        .await
        .unwrap();
    assert!(!metadata.mux_stm.streams().lifecycle_pending(intent_op));
    let final_reply = metadata
        .client_table
        .borrow()
        .get_reply(CLIENT)
        .unwrap()
        .clone()
        .into_message();
    assert!(final_reply.header().has_committed_result());
    assert_eq!(
        iggy_binary_protocol::result_code(final_reply.body()),
        Some(0)
    );
}
