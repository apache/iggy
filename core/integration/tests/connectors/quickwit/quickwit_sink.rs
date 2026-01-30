/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use anyhow::Result;
use bytes::Bytes;
use iggy_common::IggyMessage;
use serde::{Deserialize, Serialize};

use crate::connectors::create_test_messages;
use crate::connectors::quickwit::{QuickwitTestSetup, start_quickwit_container};

async fn send_test_messages(
    test_setup: &QuickwitTestSetup,
    message_count: usize,
) -> Result<Vec<Bytes>> {
    let iggy_client = test_setup.runtime.create_client().await;

    let message_payloads = create_test_messages(message_count)
        .iter()
        .map(|message| {
            serde_json::to_vec(message)
                .map(Bytes::from)
                .map_err(Into::into)
        })
        .collect::<Result<Vec<Bytes>>>()?;

    let mut test_messages = message_payloads
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, message)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(message)
                .build()
                .map_err(Into::into)
        })
        .collect::<Result<Vec<IggyMessage>>>()?;
    iggy_client.send_messages(&mut test_messages).await;

    Ok(message_payloads)
}

async fn assert_test_index_documents_match_message_payloads(
    test_setup: &QuickwitTestSetup,
    message_payloads: &[Bytes],
) -> Result<()> {
    test_setup.flush_quickwit_test_index().await?;

    let search_response = test_setup.get_quickwit_test_index_all_search().await?;
    assert_eq!(search_response.num_hits, message_payloads.len());

    for (quickwit_hit, message_payload) in search_response
        .hits
        .into_iter()
        .zip(message_payloads.iter())
    {
        assert_eq!(
            quickwit_hit,
            serde_json::from_slice::<serde_json::Value>(message_payload)?
        );
    }

    Ok(())
}

#[tokio::test]
async fn given_existent_quickwit_index_should_store() -> Result<()> {
    let quickwit_container = start_quickwit_container().await;
    let test_setup = QuickwitTestSetup::try_new_with_precreate_index(quickwit_container).await?;

    let message_count = 11;
    let sent_payloads = send_test_messages(&test_setup, message_count).await?;

    assert_test_index_documents_match_message_payloads(&test_setup, &sent_payloads).await?;

    Ok(())
}

#[tokio::test]
async fn given_nonexistent_quickwit_index_should_create_and_store() -> Result<()> {
    let test_setup = QuickwitTestSetup::try_new().await?;

    let message_count = 13;
    let sent_payloads = send_test_messages(&test_setup, message_count).await?;

    assert_test_index_documents_match_message_payloads(&test_setup, &sent_payloads).await?;

    Ok(())
}

#[tokio::test]
async fn given_bulk_message_send_should_store() -> Result<()> {
    let test_setup = QuickwitTestSetup::try_new().await?;

    let message_count = 1000;
    let sent_payloads = send_test_messages(&test_setup, message_count).await?;

    assert_test_index_documents_match_message_payloads(&test_setup, &sent_payloads).await?;

    Ok(())
}

#[tokio::test]
async fn given_invalid_messages_should_not_store() -> Result<()> {
    let test_setup = QuickwitTestSetup::try_new().await?;
    let iggy_client = test_setup.runtime.create_client().await;

    let first_valid = Bytes::from(serde_json::to_vec(&create_test_messages(1)[0])?);
    let second_valid = Bytes::from(serde_json::to_vec(&create_test_messages(1)[0])?);

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct NotTestMessage {
        not_a_test_message_field: f64,
    }
    let first_invalid = Bytes::from(serde_json::to_vec(&NotTestMessage {
        not_a_test_message_field: 17.,
    })?);

    for (message_index, message_payload) in
        [first_valid.clone(), first_invalid, second_valid.clone()]
            .into_iter()
            .enumerate()
    {
        iggy_client
            .send_messages(&mut [IggyMessage::builder()
                .id(message_index as u128 + 1)
                .payload(message_payload)
                .build()?])
            .await;
    }

    assert_test_index_documents_match_message_payloads(&test_setup, &[first_valid, second_valid])
        .await?;

    Ok(())
}
