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

use crate::configs::server::TelemetryConfig;
use crate::configs::system::LoggingConfig;
use crate::server_error::ServerError;
use tracing_subscriber::prelude::*;

pub struct Logging {}

impl Logging {
    pub fn new(_: TelemetryConfig) -> Self {
        Self {}
    }

    pub fn early_init(&mut self) {
        let console_layer = console_subscriber::spawn();

        let subscriber = tracing_subscriber::registry().with(console_layer);

        tracing::subscriber::set_global_default(subscriber)
            .expect("Setting global default subscriber failed");
    }

    pub fn late_init(
        &mut self,
        _base_directory: String,
        _config: &LoggingConfig,
    ) -> Result<(), ServerError> {
        Ok(())
    }
}

impl Default for Logging {
    fn default() -> Self {
        Self::new(TelemetryConfig::default())
    }
}
