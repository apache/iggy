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

//TODO: Fix me use shard instead of system
/*
use crate::streaming::common::test_setup::TestSetup;
use iggy::prelude::{SnapshotCompression, SystemSnapshotType};
use server::configs::cluster::ClusterConfig;
use server::configs::server::{DataMaintenanceConfig, PersonalAccessTokenConfig};
use server::streaming::session::Session;
use std::io::{Cursor, Read};
use std::net::{Ipv4Addr, SocketAddr};
use zip::ZipArchive;


#[tokio::test]
async fn should_create_snapshot_file() {
    let setup = TestSetup::init().await;
    let mut system = System::new(
        setup.config.clone(),
        ClusterConfig::default(),
        DataMaintenanceConfig::default(),
        PersonalAccessTokenConfig::default(),
    );

    system.init().await.unwrap();

    let session = Session::new(1, 1, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234));

    let snapshot = system
        .get_snapshot(
            &session,
            SnapshotCompression::Deflated,
            &vec![SystemSnapshotType::Test],
        )
        .await
        .unwrap();
    assert!(!snapshot.0.is_empty());

    let cursor = Cursor::new(snapshot.0);
    let mut zip = ZipArchive::new(cursor).unwrap();
    let mut test_file = zip.by_name("test.txt").unwrap();
    let mut test_content = String::new();
    test_file.read_to_string(&mut test_content).unwrap();
    assert_eq!(test_content, "test\n");
}

*/
