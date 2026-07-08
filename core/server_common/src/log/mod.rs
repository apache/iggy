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

pub mod logger;
pub mod runtime;
pub mod settings;

pub use logger::{LogError, Logging};
pub use settings::{
    LoggingSettings, TelemetryEndpointSettings, TelemetrySettings, TelemetryTransport,
};

/// Log the CI build fingerprint (or a developer-build notice).
///
/// A macro rather than a function so the `option_env!` lookups expand in
/// the calling binary crate, where its `build.rs` emitted the `VERGEN_*`
/// values; expanded here they would always read as unset.
#[macro_export]
macro_rules! print_build_info {
    ($version:expr) => {
        if option_env!("IGGY_CI_BUILD") == Some("true") {
            let hash = option_env!("VERGEN_GIT_SHA").unwrap_or("unknown");
            let built_at = option_env!("VERGEN_BUILD_TIMESTAMP").unwrap_or("unknown");
            let rust_version = option_env!("VERGEN_RUSTC_SEMVER").unwrap_or("unknown");
            let target = option_env!("VERGEN_CARGO_TARGET_TRIPLE").unwrap_or("unknown");
            ::tracing::info!(
                "Version: {version}, hash: {hash}, built at: {built_at} using rust version: {rust_version} for target: {target}",
                version = $version,
            );
        } else {
            ::tracing::info!(
                "It seems that you are a developer. Environment variable IGGY_CI_BUILD is not set to 'true', skipping build info print."
            )
        }
    };
}
