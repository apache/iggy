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

/// Declares modules that are only available on non-WASM targets.
macro_rules! native_modules {
    ($($(#[$meta:meta])* $vis:vis mod $name:ident;)*) => {
        $(
            #[cfg(not(target_arch = "wasm32"))]
            $(#[$meta])*
            $vis mod $name;
        )*
    };
}

// Cross-platform modules.
pub mod binary;
pub mod http;
pub mod prelude;

// Native-only modules (require tokio, native networking, etc.).
native_modules! {
    pub mod client_provider;
    pub mod client_wrappers;
    pub mod clients;
    pub mod consumer_ext;
    mod leader_aware;
    pub mod quic;
    pub mod stream_builder;
    pub mod tcp;
    pub mod websocket;
}
