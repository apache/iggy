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

use clap::{Parser, Subcommand};
use std::fs;
use std::path::Path;

const DMI_ID_DIR: &str = "/sys/class/dmi/id";
const DMI_SYS_VENDOR_FILE: &str = "sys_vendor";
const DMI_PRODUCT_NAME_FILE: &str = "product_name";
const EC2_SYS_VENDOR: &str = "Amazon EC2";
const EC2_IDENTIFIER_PREFIX: &str = "aws_";
const UNKNOWN_IDENTIFIER: &str = "unknown";

#[derive(Subcommand, Debug, Clone)]
pub enum BenchmarkOutputCommand {
    /// Output results to a directory subcommand
    Output(BenchmarkOutputArgs),
}

#[derive(Parser, Debug, Clone)]
pub struct BenchmarkOutputArgs {
    /// Output directory path for storing benchmark results
    #[arg(long, short = 'o', default_value = "performance_results")]
    pub output_dir: String,

    /// Machine label of the run. Defaults to `aws_` plus the EC2 instance type with `_` for
    /// each dot (e.g. `aws_i4i_4xlarge`) on EC2, else to the hostname
    #[arg(long, default_value_t = default_identifier())]
    pub identifier: String,

    /// Additional remark for the benchmark (e.g., no-cache)
    #[arg(long)]
    pub remark: Option<String>,

    /// Extra information
    #[arg(long)]
    pub extra_info: Option<String>,

    /// Git reference (commit hash, branch or tag) used for note in the benchmark results
    #[arg(long)]
    pub gitref: Option<String>,

    /// Git reference date used for note in the benchmark results, preferably merge date of the commit
    #[arg(long)]
    pub gitref_date: Option<String>,

    /// Open generated charts in browser after benchmark is finished
    #[arg(long, short = 'c', default_value_t = false)]
    pub open_charts: bool,

    /// Suite label stored in the report (e.g. `i4i-single`)
    #[arg(long)]
    pub suite: Option<String>,

    /// Workload label stored in the report, one per workload inside a suite
    /// (e.g. `producer-20p-persisted@800MB`)
    #[arg(long)]
    pub workload: Option<String>,
}

fn default_identifier() -> String {
    ec2_identifier(Path::new(DMI_ID_DIR)).unwrap_or_else(|| {
        hostname::get().map_or_else(
            |_| UNKNOWN_IDENTIFIER.to_owned(),
            |hostname| hostname.to_string_lossy().into_owned(),
        )
    })
}

/// The EC2 instance type, spelled the way cloud-bench spells machine labels, so runs on one
/// instance type share a label instead of carrying per-host names.
fn ec2_identifier(dmi_dir: &Path) -> Option<String> {
    let vendor = fs::read_to_string(dmi_dir.join(DMI_SYS_VENDOR_FILE)).ok()?;
    if vendor.trim() != EC2_SYS_VENDOR {
        return None;
    }
    let instance_type = fs::read_to_string(dmi_dir.join(DMI_PRODUCT_NAME_FILE)).ok()?;
    let instance_type = instance_type.trim();
    (!instance_type.is_empty())
        .then(|| format!("{EC2_IDENTIFIER_PREFIX}{}", instance_type.replace('.', "_")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn dmi_dir(files: &[(&str, &str)]) -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, content) in files {
            fs::write(dir.path().join(name), content).unwrap();
        }
        dir
    }

    #[test]
    fn given_ec2_dmi_when_reading_identifier_should_return_instance_type_label() {
        let dir = dmi_dir(&[
            (DMI_SYS_VENDOR_FILE, "Amazon EC2\n"),
            (DMI_PRODUCT_NAME_FILE, "i4i.4xlarge\n"),
        ]);

        assert_eq!(
            ec2_identifier(dir.path()).as_deref(),
            Some("aws_i4i_4xlarge")
        );
    }

    #[test]
    fn given_other_vendor_when_reading_identifier_should_return_none() {
        let dir = dmi_dir(&[
            (DMI_SYS_VENDOR_FILE, "Dell Inc.\n"),
            (DMI_PRODUCT_NAME_FILE, "PowerEdge R650\n"),
        ]);

        assert_eq!(ec2_identifier(dir.path()), None);
    }

    #[test]
    fn given_missing_dmi_files_when_reading_identifier_should_return_none() {
        assert_eq!(ec2_identifier(dmi_dir(&[]).path()), None);

        let vendor_only = dmi_dir(&[(DMI_SYS_VENDOR_FILE, "Amazon EC2\n")]);
        assert_eq!(ec2_identifier(vendor_only.path()), None);
    }
}
