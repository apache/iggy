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
use crate::streaming::utils::hash;
use ahash::AHashMap;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use iggy_common::UserId;
use std::fmt::{Display, Formatter};
use std::net::SocketAddr;
use std::rc::Rc;

#[derive(Debug, Default)]
pub struct ClientManager {
    clients: AHashMap<u32, Client>,
}

#[derive(Debug, Clone)]
pub struct Client {
    pub user_id: Option<u32>,
    pub session: Rc<Session>,
    pub transport: Transport,
    pub consumer_groups: Vec<ConsumerGroup>,
    pub last_heartbeat: IggyTimestamp,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub stream_id: u32,
    pub topic_id: u32,
    pub group_id: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum Transport {
    Tcp,
    Quic,
}

impl Display for Transport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Transport::Tcp => write!(f, "TCP"),
            Transport::Quic => write!(f, "QUIC"),
        }
    }
}

impl ClientManager {
    pub fn add_client(&mut self, address: &SocketAddr, transport: Transport) -> Rc<Session> {
        let client_id = hash::calculate_32(address.to_string().as_bytes());
        let session = Rc::new(Session::from_client_id(client_id, *address));
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

    pub fn set_user_id(&mut self, client_id: u32, user_id: UserId) -> Result<(), IggyError> {
        let client = self.clients.get_mut(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        let client = client.unwrap();
        client.user_id = Some(user_id);
        Ok(())
    }

    pub fn clear_user_id(&mut self, client_id: u32) -> Result<(), IggyError> {
        let client = self.clients.get_mut(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        let client = client.unwrap();
        client.user_id = None;
        Ok(())
    }

    pub fn try_get_client(&self, client_id: u32) -> Option<Client> {
        self.clients.get(&client_id).cloned()
    }

    pub fn try_get_client_mut(&mut self, client_id: u32) -> Option<&mut Client> {
        self.clients.get_mut(&client_id)
    }

    pub fn get_clients(&self) -> Vec<Client> {
        self.clients.values().cloned().collect()
    }

    pub fn delete_clients_for_user(&mut self, user_id: UserId) -> Result<(), IggyError> {
        let mut clients_to_remove = Vec::new();
        for client in self.clients.values() {
            if let Some(client_user_id) = client.user_id
                && client_user_id == user_id
            {
                clients_to_remove.push(client.session.client_id);
            }
        }

        for client_id in clients_to_remove {
            self.clients.remove(&client_id);
        }

        Ok(())
    }

    pub fn delete_client(&mut self, client_id: u32) -> Option<Client> {
        if let Some(mut client) = self.clients.remove(&client_id) {
            client.session.clear_user_id();
            Some(client)
        } else {
            None
        }
    }

    pub fn join_consumer_group(
        &mut self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        group_id: u32,
    ) -> Result<(), IggyError> {
        let client = self.clients.get_mut(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }

        let client = client.unwrap();
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
        &mut self,
        client_id: u32,
        stream_id: u32,
        topic_id: u32,
        consumer_group_id: u32,
    ) -> Result<(), IggyError> {
        let client = self.clients.get_mut(&client_id);
        if client.is_none() {
            return Err(IggyError::ClientNotFound(client_id));
        }
        let mut client = client.unwrap();
        for (index, consumer_group) in client.consumer_groups.iter().enumerate() {
            if consumer_group.stream_id == stream_id
                && consumer_group.topic_id == topic_id
                && consumer_group.group_id == consumer_group_id
            {
                client.consumer_groups.remove(index);
                return Ok(());
            }
        }
        Ok(())
    }

    pub fn delete_consumer_groups_for_stream(&mut self, stream_id: u32) {
        for client in self.clients.values_mut() {
            let indexes_to_remove = client
                .consumer_groups
                .iter()
                .enumerate()
                .filter_map(|(index, consumer_group)| {
                    if consumer_group.stream_id == stream_id {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            for index in indexes_to_remove {
                client.consumer_groups.remove(index);
            }
        }
    }

    pub fn delete_consumer_groups_for_topic(&mut self, stream_id: u32, topic_id: u32) {
        for client in self.clients.values_mut() {
            let indexes_to_remove = client
                .consumer_groups
                .iter()
                .enumerate()
                .filter_map(|(index, consumer_group)| {
                    if consumer_group.stream_id == stream_id && consumer_group.topic_id == topic_id
                    {
                        Some(index)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            for index in indexes_to_remove {
                client.consumer_groups.remove(index);
            }
        }
    }
}
