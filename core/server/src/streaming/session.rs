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

use iggy_common::UserId;
use std::cell::Cell;
use std::fmt::Display;
use std::net::SocketAddr;

// This might be extended with more fields in the future e.g. custom name, permissions etc.
#[derive(Debug, Clone)]
pub struct Session {
    pub client_id: u32,
    user_id: Cell<UserId>,
    active: Cell<bool>,
    pub ip_address: SocketAddr,
}

impl Session {
    pub fn new(client_id: u32, user_id: UserId, ip_address: SocketAddr) -> Self {
        Self {
            client_id,
            user_id: Cell::new(user_id),
            active: Cell::new(true),
            ip_address,
        }
    }

    pub fn stateless(user_id: UserId, ip_address: SocketAddr) -> Self {
        Self::new(0, user_id, ip_address)
    }

    pub fn from_client_id(client_id: u32, ip_address: SocketAddr) -> Self {
        Self::new(client_id, u32::MAX, ip_address)
    }

    pub fn get_user_id(&self) -> UserId {
        self.user_id.get()
    }

    pub fn set_user_id(&self, user_id: UserId) {
        self.user_id.set(user_id);
    }

    pub fn set_stale(&self) {
        self.active.set(false);
    }

    pub fn clear_user_id(&self) {
        self.set_user_id(u32::MAX);
    }

    pub fn is_active(&self) -> bool {
        self.active.get()
    }

    pub fn is_authenticated(&self) -> bool {
        self.get_user_id() != u32::MAX
    }
}

impl Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let user_id = self.get_user_id();
        if user_id != u32::MAX {
            write!(
                f,
                "client ID: {}, user ID: {}, IP address: {}",
                self.client_id, user_id, self.ip_address
            )
        } else {
            write!(
                f,
                "client ID: {}, IP address: {}",
                self.client_id, self.ip_address
            )
        }
    }
}
