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

use clap::{Args, Subcommand};

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum GeoAction {
    /// Retire a geo peer whose datacenter never returns
    ///
    /// The cluster then follows nothing of the origin again. Its shadow
    /// topics serve what they hold until their retention removes it, and a
    /// user may delete them. The retirement cannot be undone: a datacenter
    /// that comes back needs another origin name.
    ///
    /// Examples
    ///  iggy geo retire dc-a
    #[clap(verbatim_doc_comment, visible_alias = "r")]
    Retire(GeoRetireArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct GeoRetireArgs {
    /// Origin name of the peer, as its `[geo] origin` names it
    pub(crate) origin: String,
}
