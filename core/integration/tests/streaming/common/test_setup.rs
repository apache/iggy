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

use compio::fs;
use server::bootstrap::create_directories;
use server::configs::system::SystemConfig;
use server::streaming::utils::MemoryPool;
use std::sync::Arc;
use uuid::Uuid;

pub struct TestSetup {
    pub config: Arc<SystemConfig>,
}

impl TestSetup {
    pub async fn init() -> TestSetup {
        Self::init_with_config(SystemConfig::default()).await
    }

    pub async fn init_with_config(mut config: SystemConfig) -> TestSetup {
        config.path = format!("local_data_{}", Uuid::now_v7().to_u128_le());
        config.partition.enforce_fsync = true;
        config.state.enforce_fsync = true;

        let config = Arc::new(config);
        fs::create_dir(config.get_system_path()).await.unwrap();
        create_directories(&config).await.unwrap();
        MemoryPool::init_pool(config.clone());
        TestSetup { config }
    }
}

impl Drop for TestSetup {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.config.get_system_path()).unwrap();
    }
}
