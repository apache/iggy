/* Licensed to the Apache Software Foundation (ASF) under one
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

use crate::state::StateSetup;
use bytes::Bytes;
use iggy::prelude::BytesSerializable;
use iggy_common::create_stream::CreateStream;
use iggy_common::create_user::CreateUser;
use server::state::command::EntryCommand;
use server::state::entry::StateEntry;
use server::state::models::{CreateStreamWithId, CreateUserWithId};

#[compio::test]
async fn should_be_empty_given_initialized_state() {
    let setup = StateSetup::init().await;
    let state = setup.state();
    state.init().await.unwrap();
    let entries = state.load_entries().await.unwrap();
    assert!(entries.is_empty());
}

#[compio::test]
async fn should_apply_single_entry() {
    let setup = StateSetup::init().await;
    let state = setup.state();
    state.init().await.unwrap();

    let user_id = 0;
    let command = EntryCommand::CreateUser(CreateUserWithId {
        user_id: 1,
        command: CreateUser {
            username: "test".to_string(),
            password: "secret".to_string(),
            status: Default::default(),
            permissions: None,
        },
    });
    let command_bytes = command.to_bytes();

    state.apply(user_id, &command).await.unwrap();

    let mut entries = state.load_entries().await.unwrap();
    assert_eq!(entries.len(), 1);
    let entry = entries.remove(0);
    assert_entry(entry, 0, setup.version(), user_id, command_bytes);
}

#[compio::test]
async fn should_apply_encrypted_entry() {
    let setup = StateSetup::init_with_encryptor().await;
    let state = setup.state();
    state.init().await.unwrap();

    let user_id = 0;
    let command = EntryCommand::CreateUser(CreateUserWithId {
        user_id: 1,
        command: CreateUser {
            username: "test".to_string(),
            password: "secret".to_string(),
            status: Default::default(),
            permissions: None,
        },
    });
    let command_bytes = command.to_bytes();

    state.apply(user_id, &command).await.unwrap();

    let mut entries = state.load_entries().await.unwrap();
    assert_eq!(entries.len(), 1);
    let entry = entries.remove(0);
    assert_entry(entry, 0, setup.version(), user_id, command_bytes);
}

#[compio::test]
async fn should_apply_multiple_entries() {
    let setup = StateSetup::init().await;
    let state = setup.state();
    let entries = state.init().await.unwrap();

    assert!(entries.is_empty());
    assert_eq!(state.current_index(), 0);
    assert_eq!(state.entries_count(), 0);
    assert_eq!(state.term(), 0);

    let first_user_id = 0; // Root user
    let created_user_id = 1; // First created user
    let create_user = EntryCommand::CreateUser(CreateUserWithId {
        user_id: created_user_id,
        command: CreateUser {
            username: "test".to_string(),
            password: "secret".to_string(),
            status: Default::default(),
            permissions: None,
        },
    });
    let create_user_bytes = create_user.to_bytes();

    state.apply(first_user_id, &create_user).await.unwrap();

    assert_eq!(state.current_index(), 0);
    assert_eq!(state.entries_count(), 1);

    let second_user_id = 1;
    let stream_id = 1;
    let create_stream = EntryCommand::CreateStream(CreateStreamWithId {
        stream_id,
        command: CreateStream {
            name: "test".to_string(),
        },
    });
    let create_stream_bytes = create_stream.to_bytes();

    state.apply(second_user_id, &create_stream).await.unwrap();

    assert_eq!(state.current_index(), 1);
    assert_eq!(state.entries_count(), 2);

    let mut entries = state.load_entries().await.unwrap();
    assert_eq!(entries.len(), 2);

    let create_user_entry = entries.remove(0);
    assert_entry(
        create_user_entry,
        0,
        setup.version(),
        first_user_id,
        create_user_bytes,
    );

    let create_stream_entry = entries.remove(0);
    assert_entry(
        create_stream_entry,
        1,
        setup.version(),
        second_user_id,
        create_stream_bytes,
    );
}

fn assert_entry(entry: StateEntry, index: u64, version: u32, user_id: u32, command: Bytes) {
    assert_eq!(entry.index, index);
    assert_eq!(entry.term, 0);
    assert_eq!(entry.version, version);
    assert_eq!(entry.flags, 0);
    assert!(entry.checksum > 0);
    assert!(entry.timestamp.as_micros() > 0);
    assert_eq!(entry.user_id, user_id);
    assert_eq!(entry.command, command);
    assert!(entry.context.is_empty());
}
