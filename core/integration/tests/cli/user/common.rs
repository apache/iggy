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

use iggy::prelude::Permissions;

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
