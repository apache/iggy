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

use sysinfo::System;

pub fn append_cpu_name_lowercase(to: &mut String) {
    let mut sys = System::new();
    sys.refresh_all();

    let cpu = sys
        .cpus()
        .first()
        .map(|cpu| (cpu.brand().to_string(), cpu.frequency()))
        .unwrap_or_else(|| (String::from("unknown"), 0))
        .0
        .to_lowercase()
        .replace(' ', "_");

    to.push('_');
    to.push_str(&cpu);
}
