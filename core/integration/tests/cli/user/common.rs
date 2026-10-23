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

use iggy::prelude::StreamCreateOptions;
use iggy::prelude::{Client, Identifier, Permissions, TopicCreateOptions};
use std::collections::BTreeMap;

/// Creates every stream and topic that `permissions` grants on, because the
/// server refuses a grant on a missing id. Ids are handed out lowest free, so
/// creating fills the gaps below an id until the id exists.
pub(crate) async fn create_grant_targets(client: &dyn Client, permissions: Option<&Permissions>) {
    let Some(streams) = permissions.and_then(|permissions| permissions.streams.as_ref()) else {
        return;
    };
    for (&stream_id, stream_permissions) in streams {
        let stream = Identifier::numeric(stream_id as u32).unwrap();
        let mut created = 0;
        while client.get_stream(&stream).await.unwrap().is_none() {
            client
                .create_stream(
                    &format!("grant-target-{stream_id}-{created}"),
                    &StreamCreateOptions::default(),
                )
                .await
                .unwrap();
            created += 1;
        }
        for &topic_id in stream_permissions.topics.iter().flat_map(BTreeMap::keys) {
            let topic = Identifier::numeric(topic_id as u32).unwrap();
            let mut created = 0;
            while client.get_topic(&stream, &topic).await.unwrap().is_none() {
                client
                    .create_topic(
                        &stream,
                        &format!("grant-target-{topic_id}-{created}"),
                        &TopicCreateOptions::default(),
                    )
                    .await
                    .unwrap();
                created += 1;
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct PermissionsTestArgs {
    pub(crate) global_permissions: Option<String>,
    pub(crate) stream_permissions: Vec<String>,
    pub(crate) expected_permissions: Option<Permissions>,
}

impl PermissionsTestArgs {
    pub(crate) fn new(
        global_permissions: Option<String>,
        stream_permissions: Vec<String>,
        expected_permissions: Option<Permissions>,
    ) -> Self {
        Self {
            global_permissions,
            stream_permissions,
            expected_permissions,
        }
    }

    pub(crate) fn as_arg(&self) -> Vec<String> {
        let mut args = vec![];
        if let Some(global_permissions) = &self.global_permissions {
            args.push(String::from("--global-permissions"));
            args.push(global_permissions.clone());
        }

        args.extend(
            self.stream_permissions
                .iter()
                .flat_map(|i| vec![String::from("--stream-permissions"), i.clone()])
                .collect::<Vec<String>>(),
        );
        args
    }
}
