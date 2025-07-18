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

use compio::fs::create_dir;
use server::archiver::disk::DiskArchiver;
use server::configs::server::DiskArchiverConfig;
use uuid::Uuid;

mod disk;

pub struct DiskArchiverSetup {
    base_path: String,
    archive_path: String,
    archiver: DiskArchiver,
}

impl DiskArchiverSetup {
    pub async fn init() -> DiskArchiverSetup {
        let base_path = format!("test_local_data_{}", Uuid::now_v7().to_u128_le());
        let archive_path = format!("{base_path}/archive");
        let config = DiskArchiverConfig {
            path: archive_path.clone(),
        };
        let archiver = DiskArchiver::new(config);
        create_dir(&base_path).await.unwrap();

        Self {
            base_path,
            archive_path,
            archiver,
        }
    }

    pub fn archiver(&self) -> &DiskArchiver {
        &self.archiver
    }
}

impl Drop for DiskArchiverSetup {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.base_path).unwrap();
    }
}
