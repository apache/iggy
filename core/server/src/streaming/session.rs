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

use iggy_common::{AtomicUserId, UserId};
use std::fmt::Display;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};

// This might be extended with more fields in the future e.g. custom name, permissions etc.
#[derive(Debug, Clone)]
pub struct Session {
    // TODO: Fixme, those don't have to be atomics anymore, simple integers will suffice
    // Once the atomics are removed, we can impl Copy trait aswell.
    user_id: Rc<AtomicUserId>,
    active: Rc<AtomicBool>,
    pub client_id: u32,
    pub ip_address: SocketAddr,
}

impl Session {
    pub fn new(client_id: u32, user_id: UserId, ip_address: SocketAddr) -> Self {
        Self {
            client_id,
            active: Rc::new(AtomicBool::new(true)),
            user_id: Rc::new(AtomicUserId::new(user_id)),
            ip_address,
        }
    }

    pub fn stateless(user_id: UserId, ip_address: SocketAddr) -> Self {
        Self::new(0, user_id, ip_address)
    }

    pub fn from_client_id(client_id: u32, ip_address: SocketAddr) -> Self {
        Self::new(client_id, 0, ip_address)
    }

    pub fn get_user_id(&self) -> UserId {
        self.user_id.load(Ordering::Relaxed)
    }

    pub fn set_user_id(&self, user_id: UserId) {
        self.user_id.store(user_id, Ordering::Relaxed)
    }

    pub fn set_stale(&self) {
        self.active.store(false, Ordering::Relaxed)
    }

    pub fn clear_user_id(&self) {
        self.set_user_id(0)
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub fn is_authenticated(&self) -> bool {
        self.get_user_id() > 0
    }
}

impl Display for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let user_id = self.get_user_id();
        if user_id > 0 {
            return write!(
                f,
                "client ID: {}, user ID: {}, IP address: {}",
                self.client_id, user_id, self.ip_address
            );
        }

        write!(
            f,
            "client ID: {}, IP address: {}",
            self.client_id, self.ip_address
        )
    }
}
