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

use std::error;
use std::path::PathBuf;
use vergen_git2::{BuildBuilder, CargoBuilder, Emitter, Git2Builder, RustcBuilder, SysinfoBuilder};

fn main() -> Result<(), Box<dyn error::Error>> {
    if option_env!("IGGY_CI_BUILD") == Some("true") {
        Emitter::default()
            .add_instructions(&BuildBuilder::all_build()?)?
            .add_instructions(&CargoBuilder::all_cargo()?)?
            .add_instructions(&Git2Builder::all_git()?)?
            .add_instructions(&RustcBuilder::all_rustc()?)?
            .add_instructions(&SysinfoBuilder::all_sysinfo()?)?
            .emit()?;

        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..");

        println!(
            "cargo:rerun-if-changed={}",
            workspace_root
                .join("configs")
                .canonicalize()
                .unwrap_or_else(|e| panic!("Failed to canonicalize path, error: {e}"))
                .display()
        );
    } else {
        println!(
            "cargo:info=Skipping build script because CI environment variable IGGY_CI_BUILD is not set to 'true'"
        );
    }

    Ok(())
}
