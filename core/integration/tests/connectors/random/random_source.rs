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

use crate::connectors::random::setup;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn given_valid_configuration_random_source_connector_should_produce_messages() {
    let runtime = setup().await;
    let client = runtime.create_client().await;
    // Wait for some messages to be produced
    sleep(Duration::from_secs(1)).await;
    let messages = client.get_messages().await.expect("Failed to get messages");
    assert!(
        !messages.messages.is_empty(),
        "No messages received from random source"
    );
    assert!(
        messages.current_offset > 0,
        "Current offset should be greater than 0"
    );
}
