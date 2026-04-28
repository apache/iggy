/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    author = "Apache Iggy (Incubating)",
    version,
    about = "Apache Iggy server-ng",
    long_about = "Apache Iggy server-ng\n\nUse --replica-id <N> together with a shared cluster config to run one binary per cluster node."
)]
pub struct Args {
    /// Identifies this node within `cluster.nodes` by its replica ID.
    ///
    /// Required when `cluster.enabled = true`. The value must match exactly
    /// one `cluster.nodes[*].replica_id` entry in the loaded configuration.
    #[arg(long, verbatim_doc_comment)]
    pub replica_id: Option<u8>,
}
