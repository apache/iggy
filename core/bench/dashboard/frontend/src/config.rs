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

use js_sys::Reflect;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = window)]
    fn location() -> JsValue;
}

pub fn get_api_base_url() -> String {
    if cfg!(debug_assertions) {
        // In debug mode, always use localhost
        return "http://127.0.0.1:8061".to_string();
    }

    // In release mode, try to get from window.API_BASE_URL if it exists
    let window = match web_sys::window() {
        Some(win) => win,
        None => return "https://benchmarks.iggy.apache.org".to_string(),
    };

    if let Some(api_url) = Reflect::get(&window, &JsValue::from_str("API_BASE_URL"))
        .ok()
        .and_then(|val| val.as_string())
    {
        return api_url;
    }

    // For other hosts in release mode, use the same host with port 8061
    if let Ok(location) = window.location().host() {
        if let Some(colon_pos) = location.find(':') {
            return format!("https://{}", location.replace(&location[colon_pos..], ""));
        }
        return format!("https://{location}");
    }

    // Fallback to production URL
    "https://benchmarks.iggy.apache.org".to_string()
}
