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

use iggy::prelude::*;
use integration::test_server::{ClientFactory, assert_clean_system};
use std::io::{Cursor, Read};
use zip::ZipArchive;

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);

    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    let snapshot = client
        .snapshot(
            SnapshotCompression::Deflated,
            vec![SystemSnapshotType::Test],
        )
        .await
        .unwrap();

    assert!(!snapshot.0.is_empty());

    let cursor = Cursor::new(snapshot.0);
    let mut zip = ZipArchive::new(cursor).unwrap();
    let mut test_file = zip.by_name("test.txt").unwrap();
    let mut test_content = String::new();
    test_file.read_to_string(&mut test_content).unwrap();
    assert_eq!(test_content.trim(), "test");

    assert_clean_system(&client).await;
}
