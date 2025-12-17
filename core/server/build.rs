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

use std::path::PathBuf;
use std::{env, error};
use vergen_git2::{BuildBuilder, CargoBuilder, Emitter, Git2Builder, RustcBuilder, SysinfoBuilder};

const WEB_ASSETS_PATH: &str = "web/build/static";
const WEB_INDEX_FILE: &str = "web/build/static/index.html";

fn main() -> Result<(), Box<dyn error::Error>> {
    verify_web_assets_if_enabled();
    emit_vergen_instructions()?;
    Ok(())
}

/// Returns the workspace root (iggy/), two levels up from core/server.
fn workspace_root() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap())
        .parent()
        .and_then(|p| p.parent())
        .expect("server crate must be at core/server within workspace")
        .to_path_buf()
}

fn emit_vergen_instructions() -> Result<(), Box<dyn error::Error>> {
    if option_env!("IGGY_CI_BUILD") != Some("true") {
        println!("cargo:info=Skipping vergen because IGGY_CI_BUILD is not set to 'true'");
        return Ok(());
    }

    Emitter::default()
        .add_instructions(&BuildBuilder::all_build()?)?
        .add_instructions(&CargoBuilder::all_cargo()?)?
        .add_instructions(&Git2Builder::all_git()?)?
        .add_instructions(&RustcBuilder::all_rustc()?)?
        .add_instructions(&SysinfoBuilder::all_sysinfo()?)?
        .emit()?;

    let configs_path = workspace_root()
        .join("core/configs")
        .canonicalize()
        .unwrap_or_else(|e| panic!("Failed to canonicalize configs path: {e}"));

    println!("cargo:rerun-if-changed={}", configs_path.display());
    Ok(())
}

fn verify_web_assets_if_enabled() {
    if env::var("CARGO_FEATURE_IGGY_WEB").is_err() {
        return;
    }

    let assets_dir = workspace_root().join(WEB_ASSETS_PATH);
    let index_file = workspace_root().join(WEB_INDEX_FILE);

    println!("cargo:rerun-if-changed={}", assets_dir.display());

    if !assets_dir.exists() || !index_file.exists() {
        println!(
            "cargo:warning=Web UI assets not found at {}. \
             To build them, run: npm --prefix web ci && npm --prefix web run build:static",
            assets_dir.display()
        );
        return;
    }

    println!(
        "cargo:info=Web UI assets verified at {}",
        assets_dir.display()
    );
}
