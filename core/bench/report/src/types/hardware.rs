// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use serde::{Deserialize, Serialize};
use sysinfo::System;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct BenchmarkHardware {
    pub identifier: Option<String>,
    pub cpu_name: String,
    pub cpu_cores: usize,
    pub total_memory_mb: u64,
    pub os_name: String,
    pub os_version: String,
}

impl BenchmarkHardware {
    pub fn get_system_info_with_identifier(identifier: Option<String>) -> Self {
        let sys = System::new().ok().map(|mut sys| {
            sys.refresh_all();
            sys
        });
        let cpus = sys.as_ref().map_or(&[][..], System::cpus);

        Self {
            identifier,
            cpu_name: cpus
                .first()
                .map_or_else(|| String::from("unknown"), |cpu| cpu.brand().to_string()),
            cpu_cores: cpus.len(),
            total_memory_mb: sys.as_ref().map_or(0, System::total_memory) / 1024 / 1024,
            os_name: System::name().unwrap_or_else(|_| String::from("unknown")),
            os_version: System::kernel_version().unwrap_or_else(|_| String::from("unknown")),
        }
    }
}
