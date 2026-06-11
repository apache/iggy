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

use configs::ConfigEnv;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub name: String,
    /// Full roster of cluster members. Intended to be byte-identical across
    /// every node so operators ship one config. The running node's identity
    /// is supplied out-of-band via the `--replica-id` CLI flag, which
    /// selects the entry in this list that describes the current node.
    //
    // TODO(hubcio): IGGY-155 `register-replica` CLI (a validated roster
    // append) is deferred - it is convenience only over a manual TOML edit,
    // and `ClusterConfig::validate` already rejects a malformed roster at
    // boot. Add it only if scripted/automated roster edits become a need.
    #[serde(default)]
    pub nodes: Vec<ClusterNodeConfig>,
    /// Replica-to-replica authentication settings (PSK + BLAKE3 handshake).
    #[serde(default)]
    pub auth: ClusterAuthConfig,
}

/// Replica-to-replica authentication for the consensus (`tcp_replica`) port.
#[derive(Debug, Default, Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct ClusterAuthConfig {
    /// When true, every replica peer must complete the authenticated handshake
    /// or be rejected, and [`Self::shared_secret`] is mandatory. When false
    /// (default) the replica handshake stays in legacy unauthenticated mode and
    /// `shared_secret` is not used for authentication. A configured non-empty
    /// `shared_secret` must still meet the 32-byte minimum whenever the cluster
    /// is enabled (a short value fails boot even with auth off).
    ///
    /// Enabling auth is a coordinated-restart change, and not the only one: the
    /// consensus `cluster_id` is derived from `ClusterConfig::name`
    /// unconditionally, so a mixed-version roster fails to connect regardless of
    /// this flag. Flip every node in one restart.
    #[serde(default)]
    pub enabled: bool,
    /// Cluster-wide pre-shared key for replica-to-replica authentication.
    ///
    /// At least 32 bytes of CSPRNG output, byte-identical across every node.
    /// Provisioned out-of-band, normally via `IGGY_CLUSTER_AUTH_SHARED_SECRET`
    /// rather than the on-disk config.
    // skip_serializing keeps the PSK out of the runtime `current_config.toml`
    // (and the `ServerConfig` diagnostic snapshot that cats it). The live
    // secret is read from env / on-disk config at boot, never from the
    // snapshot, so it must never be persisted there. Deserialize is retained.
    #[serde(default, skip_serializing)]
    #[config_env(secret)]
    pub shared_secret: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct ClusterNodeConfig {
    pub name: String,
    pub ip: String,
    /// Numeric replica ID for VSR consensus (0-based).
    ///
    /// Must be unique across [`ClusterConfig::nodes`] and strictly less than
    /// `nodes.len()`. Validated by [`ClusterConfig::validate`].
    pub replica_id: u8,
    pub ports: TransportPorts,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, ConfigEnv)]
pub struct TransportPorts {
    pub tcp: Option<u16>,
    pub quic: Option<u16>,
    pub http: Option<u16>,
    pub websocket: Option<u16>,
    /// Dedicated port for replica-to-replica consensus traffic.
    pub tcp_replica: Option<u16>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_secret_is_never_serialized() {
        // Regression guard: the runtime current_config.toml (and the
        // ServerConfig diagnostic snapshot that cats it) are produced by
        // serializing this struct, so the PSK must not survive serialize.
        // skip_serializing is format-agnostic, so a JSON dump proves the toml
        // path too.
        let config = ClusterConfig {
            enabled: true,
            name: "iggy-cluster".to_owned(),
            nodes: Vec::new(),
            auth: ClusterAuthConfig {
                enabled: true,
                shared_secret: "current-psk-MUST-NOT-be-persisted".to_owned(),
            },
        };
        let serialized = serde_json::to_string(&config).expect("serialize cluster config");
        assert!(
            !serialized.contains("MUST-NOT-be-persisted"),
            "PSK leaked into serialized config: {serialized}"
        );
        assert!(
            !serialized.contains("shared_secret"),
            "shared_secret field present in serialized config: {serialized}"
        );
    }
}
