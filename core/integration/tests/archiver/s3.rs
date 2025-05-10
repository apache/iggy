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

use server::archiver::Archiver;
use server::archiver::s3::S3Archiver;
use server::configs::server::S3ArchiverConfig;

#[tokio::test]
async fn should_not_be_initialized_given_invalid_configuration() {
    let config = S3ArchiverConfig {
        key_id: "test".to_owned(),
        key_secret: "secret".to_owned(),
        bucket: "iggy".to_owned(),
        endpoint: Some("https://iggy.s3.com".to_owned()),
        region: None,
        tmp_upload_dir: "tmp".to_owned(),
    };
    let archiver = S3Archiver::new(config);
    assert!(archiver.is_ok());
    let archiver = archiver.unwrap();
    let init = archiver.init().await;
    assert!(init.is_err());
}
