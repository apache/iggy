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

use keyring::{Entry, Result};

const SESSION_TOKEN_NAME: &str = "iggy-cli-session";
const SESSION_KEYRING_SERVICE_NAME: &str = "iggy-cli-session";

pub struct ServerSession {
    server_address: String,
}

impl ServerSession {
    pub fn new(server_address: String) -> Self {
        Self { server_address }
    }

    pub fn get_server_address(&self) -> &str {
        &self.server_address
    }

    fn get_service_name(&self) -> String {
        format!("{SESSION_KEYRING_SERVICE_NAME}:{}", self.server_address)
    }

    pub fn get_token_name(&self) -> String {
        String::from(SESSION_TOKEN_NAME)
    }

    pub fn is_active(&self) -> bool {
        if let Ok(entry) = Entry::new(&self.get_service_name(), &self.get_token_name()) {
            return entry.get_password().is_ok();
        }

        false
    }

    pub fn store(&self, token: &str) -> Result<()> {
        let entry = Entry::new(&self.get_service_name(), &self.get_token_name())?;
        entry.set_password(token)?;
        Ok(())
    }

    pub fn get_token(&self) -> Option<String> {
        if let Ok(entry) = Entry::new(&self.get_service_name(), &self.get_token_name())
            && let Ok(token) = entry.get_password()
        {
            return Some(token);
        }

        None
    }

    pub fn delete(&self) -> Result<()> {
        let entry = Entry::new(&self.get_service_name(), &self.get_token_name())?;
        entry.delete_credential()
    }
}
