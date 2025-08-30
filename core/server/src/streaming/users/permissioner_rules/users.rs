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

use crate::streaming::users::permissioner::Permissioner;
use iggy_common::IggyError;

impl Permissioner {
    pub fn get_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.read_users(user_id)
    }

    pub fn get_users(&self, user_id: u32) -> Result<(), IggyError> {
        self.read_users(user_id)
    }

    pub fn create_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    pub fn delete_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    pub fn update_user(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    pub fn update_permissions(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    pub fn change_password(&self, user_id: u32) -> Result<(), IggyError> {
        self.manager_users(user_id)
    }

    fn manager_users(&self, user_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id)
            && global_permissions.manage_users
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    fn read_users(&self, user_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id)
            && (global_permissions.manage_users || global_permissions.read_users)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }
}
