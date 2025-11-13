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

use super::COMPONENT;
use crate::shard::IggyShard;
use crate::streaming::clients::client_manager::Client;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::{Identifier, TransportProtocol};
use std::net::SocketAddr;
use tracing::{error, info};

impl IggyShard {
    pub fn add_client(&self, address: &SocketAddr, transport: TransportProtocol) -> Session {
        let session = self.client_manager.add_client(address, transport);
        self.metrics.increment_clients(1);
        session
    }

    pub fn delete_client(&self, client_id: u32) {
        let consumer_groups: Vec<(u32, u32, u32)>;

        {
            let client = self.client_manager.delete_client(client_id);
            if client.is_none() {
                error!("Client with ID: {client_id} was not found in the client manager.",);
                return;
            }

            self.metrics.decrement_clients(1);
            let client = client.unwrap();
            consumer_groups = client
                .consumer_groups
                .iter()
                .map(|c| (c.stream_id, c.topic_id, c.group_id))
                .collect();

            info!(
                "Deleted {} client with ID: {} for IP address: {}",
                client.transport, client.session.client_id, client.session.ip_address
            );
        }

        for (stream_id, topic_id, consumer_group_id) in consumer_groups.into_iter() {
            _ = self.leave_consumer_group_base(
                &Identifier::numeric(stream_id).unwrap(),
                &Identifier::numeric(topic_id).unwrap(),
                &Identifier::numeric(consumer_group_id).unwrap(),
                client_id,
            )
        }
    }

    pub fn get_client(
        &self,
        session: &Session,
        client_id: u32,
    ) -> Result<Option<Client>, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
        .borrow()
            .get_client(session.get_user_id())
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - permission denied to get client with ID: {client_id} by user ID: {}",
                    session.get_user_id()
                )
            })?;

        Ok(self.client_manager.try_get_client(client_id))
    }

    pub fn get_clients(&self, session: &Session) -> Result<Vec<Client>, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner
            .borrow()
            .get_clients(session.get_user_id())
            .with_error(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get clients by user ID {}",
                    session.get_user_id()
                )
            })?;

        Ok(self.client_manager.get_clients())
    }
}
