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
use iggy::prelude::Identifier;
use server::configs::server::{DataMaintenanceConfig, PersonalAccessTokenConfig};
use server::streaming::session::Session;
use std::net::{Ipv4Addr, SocketAddr};
use tokio::fs;

//TODO: Fix me use shard instead of system
/*
#[tokio::test]
async fn should_initialize_system_and_base_directories() {
    let setup = TestSetup::init().await;
    let mut system = System::new(
        setup.config.clone(),
        DataMaintenanceConfig::default(),
        PersonalAccessTokenConfig::default(),
    );

    system.init().await.unwrap();

    let mut dir_entries = fs::read_dir(&setup.config.path).await.unwrap();
    let mut names = Vec::new();
    while let Some(entry) = dir_entries.next_entry().await.unwrap() {
        let metadata = entry.metadata().await.unwrap();
        assert!(metadata.is_dir());
        names.push(entry.file_name().into_string().unwrap());
    }

    assert_eq!(names.len(), 3);
    assert!(names.contains(&setup.config.stream.path));
}

#[tokio::test]
async fn should_create_and_persist_stream() {
    let setup = TestSetup::init().await;
    let mut system = System::new(
        setup.config.clone(),
        DataMaintenanceConfig::default(),
        PersonalAccessTokenConfig::default(),
    );
    let stream_id = 1;
    let stream_name = "test";
    let session = Session::new(1, 1, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234));
    system.init().await.unwrap();

    system
        .create_stream(&session, Some(stream_id), stream_name)
        .await
        .unwrap();

    assert_persisted_stream(&setup.config.get_streams_path(), stream_id).await;
}

#[tokio::test]
async fn should_create_and_persist_stream_with_automatically_generated_id() {
    let setup = TestSetup::init().await;
    let mut system = System::new(
        setup.config.clone(),
        DataMaintenanceConfig::default(),
        PersonalAccessTokenConfig::default(),
    );
    let stream_id = 1;
    let stream_name = "test";
    let session = Session::new(1, 1, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234));
    system.init().await.unwrap();

    system
        .create_stream(&session, None, stream_name)
        .await
        .unwrap();

    assert_persisted_stream(&setup.config.get_streams_path(), stream_id).await;
}

#[tokio::test]
async fn should_delete_persisted_stream() {
    let setup = TestSetup::init().await;
    let mut system = System::new(
        setup.config.clone(),
        DataMaintenanceConfig::default(),
        PersonalAccessTokenConfig::default(),
    );
    let stream_id = 1;
    let stream_name = "test";
    let session = Session::new(1, 1, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234));
    system.init().await.unwrap();
    system
        .create_stream(&session, Some(stream_id), stream_name)
        .await
        .unwrap();
    assert_persisted_stream(&setup.config.get_streams_path(), stream_id).await;
    let stream_path = system
        .get_stream(&Identifier::numeric(stream_id).unwrap())
        .unwrap()
        .path
        .clone();

    system
        .delete_stream(&session, &Identifier::numeric(1).unwrap())
        .await
        .unwrap();

    assert!(fs::metadata(stream_path).await.is_err());
}

async fn assert_persisted_stream(streams_path: &str, stream_id: u32) {
    let streams_metadata = fs::metadata(streams_path).await.unwrap();
    assert!(streams_metadata.is_dir());
    let stream_path = format!("{streams_path}/{stream_id}");
    let stream_metadata = fs::metadata(stream_path).await.unwrap();
    assert!(stream_metadata.is_dir());
}

*/
