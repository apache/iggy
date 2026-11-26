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

const PRODUCTION_API_BASE_URL: &str = "https://benchmarks.iggy.apache.org";

/// The origin of the page. Outside the browser it is empty, so the links that the server renders
/// are relative to the page, and the browser makes them absolute when it hydrates the page.
pub fn get_api_base_url() -> String {
    if !cfg!(target_arch = "wasm32") {
        return String::new();
    }
    // The dashboard server hosts the page next to the API, and `trunk serve` proxies the API
    // for the dev page (see Trunk.toml), so the API is always on the page origin.
    web_sys::window()
        .and_then(|window| window.location().origin().ok())
        .unwrap_or_else(|| PRODUCTION_API_BASE_URL.to_string())
}
