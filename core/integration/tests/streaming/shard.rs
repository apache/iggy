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

use crate::streaming::common::test_setup::TestSetup;
use compio::fs;
use iggy::prelude::Identifier;
use iggy_common::locking::IggyRwLockFn;
use server::configs::server::{DataMaintenanceConfig, PersonalAccessTokenConfig, ServerConfig};
use server::configs::system::SystemConfig;
use server::shard::IggyShard;
use server::streaming::session::Session;
use std::net::{Ipv4Addr, SocketAddr};
use std::rc::Rc;

//TODO: Fix me use shard instead of system
#[compio::test]
async fn should_initialize_system_and_base_directories() {
    let setup = TestSetup::init().await;
    let server_config = ServerConfig {
        system: setup.config.clone(),
        ..Default::default()
    };
    let shard = IggyShard::default_from_config(server_config);
    shard.init().await.unwrap();

    let mut dir_entries = server::io::fs_utils::walk_dir(&setup.config.path)
        .await
        .unwrap();
    let mut names = Vec::new();
    for dir_entry in dir_entries.iter_mut() {
        if dir_entry.is_dir {
            names.push(dir_entry.name.clone().unwrap());
        } else {
            dir_entry.name = Some(
                dir_entry
                    .path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
            );
        }
    }

    // Root + streams + state + runtime
    assert_eq!(names.len(), 4);
    let mut has_stream = false;
    for name in names {
        if name.contains(&setup.config.stream.path) {
            has_stream = true;
        }
    }
    assert!(has_stream);
}

#[compio::test]
async fn should_create_and_persist_stream() {
    let setup = TestSetup::init().await;
    let server_config = ServerConfig {
        system: setup.config.clone(),
        ..Default::default()
    };
    let shard = IggyShard::default_from_config(server_config);
    let stream_id = 1;
    let stream_name = "test";
    let session = Session::new(1, 1, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234));
    let session = Rc::new(session);
    shard.init().await.unwrap();
    shard.add_active_session(session.clone());

    shard
        .create_stream_bypass_auth(Some(stream_id), stream_name)
        .unwrap();
    let stream = shard
        .get_stream(&Identifier::numeric(stream_id).unwrap())
        .unwrap();
    stream.persist().await.unwrap();

    assert_persisted_stream(&setup.config.get_streams_path(), stream_id).await;
}

#[compio::test]
async fn should_create_and_persist_stream_with_automatically_generated_id() {
    let setup = TestSetup::init().await;
    let server_config = ServerConfig {
        system: setup.config.clone(),
        ..Default::default()
    };
    let shard = IggyShard::default_from_config(server_config);
    let stream_id = 1;
    let stream_name = "test";
    shard.init().await.unwrap();
    let session = Session::new(1, 1, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234));
    let session = Rc::new(session);
    shard.add_active_session(session.clone());

    let stream_id = shard.create_stream_bypass_auth(None, stream_name).unwrap();
    let stream = shard
        .get_stream(&Identifier::numeric(stream_id).unwrap())
        .unwrap();
    stream.persist().await.unwrap();

    assert_persisted_stream(&setup.config.get_streams_path(), stream_id).await;
}

#[compio::test]
async fn should_delete_persisted_stream() {
    let setup = TestSetup::init().await;
    let server_config = ServerConfig {
        system: setup.config.clone(),
        ..Default::default()
    };
    let shard = IggyShard::default_from_config(server_config);
    let stream_id = 1;
    let stream_name = "test";
    let session = Session::new(1, 1, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234));
    let session = Rc::new(session);
    shard.init().await.unwrap();
    shard.add_active_session(session.clone());

    let id = shard
        .create_stream_bypass_auth(Some(stream_id), stream_name)
        .unwrap();
    let stream = shard.get_stream(&Identifier::numeric(id).unwrap()).unwrap();
    stream.persist().await.unwrap();
    drop(stream);
    assert_persisted_stream(&setup.config.get_streams_path(), stream_id).await;
    let stream_path = shard
        .get_stream(&Identifier::numeric(stream_id).unwrap())
        .unwrap()
        .path
        .clone();

    let stream = shard
        .delete_stream_bypass_auth(&Identifier::numeric(stream_id).unwrap())
        .unwrap();
    for topic in stream.get_topics() {
        let partitions = topic.get_partitions();
        for partition in partitions {
            let mut part = partition.write().await;
            part.delete().await.unwrap();
        }
        topic.delete().await.unwrap();
    }
    stream.delete().await.unwrap();
    assert!(fs::metadata(stream_path).await.is_err());
}

async fn assert_persisted_stream(streams_path: &str, stream_id: u32) {
    let streams_metadata = fs::metadata(streams_path).await.unwrap();
    assert!(streams_metadata.is_dir());
    let stream_path = format!("{streams_path}/{stream_id}");
    let stream_metadata = fs::metadata(stream_path).await.unwrap();
    assert!(stream_metadata.is_dir());
}
