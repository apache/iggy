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

use crate::streaming::session::Session;
use crate::streaming::utils::{hash, ptr::EternalPtr};
use dashmap::DashMap;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use iggy_common::TransportProtocol;
use iggy_common::UserId;
use std::net::SocketAddr;

pub struct ClientManager {
    clients: EternalPtr<DashMap<u32, Client>>,
}

impl ClientManager {
    pub fn new(clients: EternalPtr<DashMap<u32, Client>>) -> Self {
        Self { clients }
    }
}

impl Clone for ClientManager {
    fn clone(&self) -> Self {
        Self {
            clients: self.clients.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Client {
    pub user_id: Option<u32>,
    pub session: Session,
    pub transport: TransportProtocol,
    pub consumer_groups: Vec<ConsumerGroup>,
    pub last_heartbeat: IggyTimestamp,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub stream_id: u32,
    pub topic_id: u32,
    pub group_id: u32,
}

impl ClientManager {
    pub fn add_client(&self, address: &SocketAddr, transport: TransportProtocol) -> Session {
        let client_id = hash::calculate_32(address.to_string().as_bytes());
        let session = Session::from_client_id(client_id, *address);
        let client = Client {
            user_id: None,
            session: session.clone(),
            transport,
            consumer_groups: Vec::new(),
            last_heartbeat: IggyTimestamp::now(),
        };
        self.clients.insert(client_id, client);
        session
    }

    pub fn set_user_id(&self, client_id: u32, user_id: UserId) -> Result<(), IggyError> {
        self.clients
            .get_mut(&client_id)
            .ok_or(IggyError::ClientNotFound(client_id))?
            .user_id = Some(user_id);
        Ok(())
    }

    pub fn clear_user_id(&self, client_id: u32) -> Result<(), IggyError> {
        self.clients
            .get_mut(&client_id)
            .ok_or(IggyError::ClientNotFound(client_id))?
            .user_id = None;
        Ok(())
    }

    pub fn try_get_client(&self, client_id: u32) -> Option<Client> {
        self.clients.get(&client_id).map(|c| c.clone())
    }

    pub fn try_get_client_mut(
        &'_ self,
        client_id: u32,
    ) -> Option<dashmap::mapref::one::RefMut<'_, u32, Client>> {
        self.clients.get_mut(&client_id)
    }

    pub fn get_clients(&self) -> Vec<Client> {
        self.clients
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    pub fn delete_clients_for_user(&self, user_id: UserId) -> Result<(), IggyError> {
        let clients_to_remove: Vec<u32> = self
            .clients
            .iter()
            .filter(|entry| entry.value().user_id == Some(user_id))
            .map(|entry| *entry.key())
            .collect();

        for client_id in clients_to_remove {
            self.clients.remove(&client_id);
        }
        Ok(())
    }

    pub fn delete_client(&self, client_id: u32) -> Option<Client> {
        self.clients.remove(&client_id).map(|(_, client)| client)
    }

    pub fn get_client_count(&self) -> usize {
        self.clients.len()
    }

    pub fn heartbeat(&mut self, client_id: u32) -> Result<(), IggyError> {
        let mut client = self
            .clients
            .get_mut(&client_id)
            .ok_or(IggyError::ClientNotFound(client_id))?;
        client.last_heartbeat = IggyTimestamp::now();
        Ok(())
    }

    pub fn join_consumer_group(
        &self,
        client_id: u32,
        stream_id: usize,
        topic_id: usize,
        group_id: usize,
    ) -> Result<(), IggyError> {
        let stream_id = stream_id as u32;
        let topic_id = topic_id as u32;
        let group_id = group_id as u32;

        let mut client = self
            .clients
            .get_mut(&client_id)
            .ok_or(IggyError::ClientNotFound(client_id))?;

        if client.consumer_groups.iter().any(|consumer_group| {
            consumer_group.group_id == group_id
                && consumer_group.topic_id == topic_id
                && consumer_group.stream_id == stream_id
        }) {
            return Ok(());
        }

        client.consumer_groups.push(ConsumerGroup {
            stream_id,
            topic_id,
            group_id,
        });
        Ok(())
    }

    pub fn leave_consumer_group(
        &self,
        client_id: u32,
        stream_id: usize,
        topic_id: usize,
        consumer_group_id: usize,
    ) -> Result<(), IggyError> {
        let stream_id = stream_id as u32;
        let topic_id = topic_id as u32;
        let consumer_group_id = consumer_group_id as u32;

        let mut client = self
            .clients
            .get_mut(&client_id)
            .ok_or(IggyError::ClientNotFound(client_id))?;

        if let Some(index) = client.consumer_groups.iter().position(|consumer_group| {
            consumer_group.stream_id == stream_id
                && consumer_group.topic_id == topic_id
                && consumer_group.group_id == consumer_group_id
        }) {
            client.consumer_groups.remove(index);
        }
        Ok(())
    }

    pub fn delete_consumer_group(&self, stream_id: usize, topic_id: usize, group_id: usize) {
        let stream_id = stream_id as u32;
        let topic_id = topic_id as u32;
        let group_id = group_id as u32;

        for mut client in self.clients.iter_mut() {
            client.consumer_groups.retain(|consumer_group| {
                !(consumer_group.stream_id == stream_id
                    && consumer_group.topic_id == topic_id
                    && consumer_group.group_id == group_id)
            });
        }
    }

    pub fn delete_consumer_groups_for_stream(&self, stream_id: usize) {
        let stream_id = stream_id as u32;

        for mut client in self.clients.iter_mut() {
            client
                .consumer_groups
                .retain(|consumer_group| consumer_group.stream_id != stream_id);
        }
    }

    pub fn delete_consumer_groups_for_topic(&self, stream_id: usize, topic_id: usize) {
        let stream_id = stream_id as u32;
        let topic_id = topic_id as u32;

        for mut client in self.clients.iter_mut() {
            client.consumer_groups.retain(|consumer_group| {
                !(consumer_group.stream_id == stream_id && consumer_group.topic_id == topic_id)
            });
        }
    }
}
