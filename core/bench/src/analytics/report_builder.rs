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

use std::{collections::HashMap, thread};

use super::metrics::group::{from_individual_metrics, from_producers_and_consumers_statistics};
use crate::utils::get_server_stats;
use bench_report::{
    actor_kind::ActorKind,
    benchmark_kind::BenchmarkKind,
    cluster::{BenchmarkClusterInfo, BenchmarkClusterNode},
    hardware::BenchmarkHardware,
    individual_metrics::BenchmarkIndividualMetrics,
    params::BenchmarkParams,
    report::BenchmarkReport,
    server_stats::{BenchmarkCacheMetrics, BenchmarkCacheMetricsKey, BenchmarkServerStats},
};
use chrono::{DateTime, Utc};
use iggy::prelude::{
    CacheMetrics, CacheMetricsKey, ClusterClient, ClusterMetadata, IggyClient, IggyTimestamp, Stats,
};
use tracing::warn;

/// Both the legacy server and server-ng synthesize exactly this cluster name
/// for a non-clustered instance, so it is the single-node sentinel.
const SINGLE_NODE_CLUSTER_NAME: &str = "single-node";

pub struct BenchmarkReportBuilder;

impl BenchmarkReportBuilder {
    #[allow(clippy::cast_possible_wrap)]
    pub async fn build(
        hardware: BenchmarkHardware,
        mut params: BenchmarkParams,
        mut individual_metrics: Vec<BenchmarkIndividualMetrics>,
        moving_average_window: u32,
        admin_client: &IggyClient,
    ) -> BenchmarkReport {
        let uuid = uuid::Uuid::new_v4();

        let timestamp =
            DateTime::<Utc>::from_timestamp_micros(IggyTimestamp::now().as_micros() as i64)
                .map_or_else(|| String::from("unknown"), |dt| dt.to_rfc3339());

        let server_stats = get_server_stats(admin_client)
            .await
            .expect("Failed to get server stats");

        if params.gitref.is_none() {
            params.gitref = Some(server_stats.iggy_server_version.clone());
        }

        if params.gitref_date.is_none() {
            params.gitref_date = Some(timestamp.clone());
        }

        // Old servers predate the cluster-metadata command; tolerate the error
        // and treat the target as a single node rather than failing the run.
        let cluster = match admin_client.get_cluster_metadata().await {
            Ok(metadata) => cluster_info_from_metadata(metadata),
            Err(error) => {
                warn!("cluster metadata unavailable, assuming single node: {error}");
                None
            }
        };

        // params_identifier is the dashboard trend grouping key; fork cluster
        // runs into their own series so they never mix with single-node runs.
        params
            .params_identifier
            .push_str(&cluster_suffix(cluster.as_ref()));

        let mut group_metrics = Vec::new();

        individual_metrics.sort_by_key(|m| (m.summary.actor_kind, m.summary.actor_id));

        let producer_metrics: Vec<BenchmarkIndividualMetrics> = individual_metrics
            .iter()
            .filter(|m| m.summary.actor_kind == ActorKind::Producer)
            .cloned()
            .collect();
        let consumer_metrics: Vec<BenchmarkIndividualMetrics> = individual_metrics
            .iter()
            .filter(|m| m.summary.actor_kind == ActorKind::Consumer)
            .cloned()
            .collect();
        let producing_consumers_metrics: Vec<BenchmarkIndividualMetrics> = individual_metrics
            .iter()
            .filter(|m| m.summary.actor_kind == ActorKind::ProducingConsumer)
            .cloned()
            .collect();

        let mut join_handles = Vec::new();

        for individual_metric in [
            &producer_metrics,
            &consumer_metrics,
            &producing_consumers_metrics,
        ] {
            if !individual_metric.is_empty() {
                let individual_metric_copy = individual_metric.clone();

                join_handles.push(thread::spawn(move || {
                    if let Some(metric) =
                        from_individual_metrics(&individual_metric_copy, moving_average_window)
                    {
                        return Some(metric);
                    }
                    None
                }));
            }
        }

        if matches!(
            params.benchmark_kind,
            BenchmarkKind::PinnedProducerAndConsumer
                | BenchmarkKind::BalancedProducerAndConsumerGroup
        ) && !producer_metrics.is_empty()
            && !consumer_metrics.is_empty()
        {
            join_handles.push(thread::spawn(move || {
                if let Some(metric) = from_producers_and_consumers_statistics(
                    &producer_metrics,
                    &consumer_metrics,
                    moving_average_window,
                ) {
                    return Some(metric);
                }
                None
            }));
        }

        for handle in join_handles {
            if let Some(metric) = handle.join().expect("Should have computed group metric") {
                group_metrics.push(metric);
            }
        }

        BenchmarkReport {
            uuid,
            server_stats: stats_to_benchmark_server_stats(server_stats),
            timestamp,
            hardware,
            params,
            cluster,
            group_metrics,
            individual_metrics,
        }
    }
}

/// Classifies raw server metadata into cluster topology, returning `None` for a
/// single node so callers can keep single-node reports untouched.
fn cluster_info_from_metadata(metadata: ClusterMetadata) -> Option<BenchmarkClusterInfo> {
    let is_real_cluster = metadata.nodes.len() > 1 || metadata.name != SINGLE_NODE_CLUSTER_NAME;
    if !is_real_cluster {
        return None;
    }

    let nodes = metadata
        .nodes
        .into_iter()
        .map(|node| BenchmarkClusterNode {
            name: node.name,
            role: node.role.to_string(),
            status: node.status.to_string(),
        })
        .collect();

    Some(BenchmarkClusterInfo {
        name: metadata.name,
        nodes,
    })
}

/// Trend/directory suffix that forks a cluster run away from the single-node
/// run sharing the same benchmark params. Empty for single node so existing
/// identifiers and directory names stay byte-identical.
pub fn cluster_suffix(cluster: Option<&BenchmarkClusterInfo>) -> String {
    cluster.map_or_else(String::new, |cluster| {
        format!("_cluster{}", cluster.nodes.len())
    })
}

/// This function is a workaround.
/// See `server_stats.rs` in `bench_report` crate for more details.
fn stats_to_benchmark_server_stats(stats: Stats) -> BenchmarkServerStats {
    BenchmarkServerStats {
        process_id: stats.process_id,
        cpu_usage: stats.cpu_usage,
        total_cpu_usage: stats.total_cpu_usage,
        memory_usage: stats.memory_usage.as_bytes_u64(),
        total_memory: stats.total_memory.as_bytes_u64(),
        available_memory: stats.available_memory.as_bytes_u64(),
        run_time: stats.run_time.into(),
        start_time: stats.start_time.into(),
        read_bytes: stats.read_bytes.as_bytes_u64(),
        written_bytes: stats.written_bytes.as_bytes_u64(),
        messages_size_bytes: stats.messages_size_bytes.as_bytes_u64(),
        streams_count: stats.streams_count,
        topics_count: stats.topics_count,
        partitions_count: stats.partitions_count,
        segments_count: stats.segments_count,
        messages_count: stats.messages_count,
        clients_count: stats.clients_count,
        consumer_groups_count: stats.consumer_groups_count,
        hostname: stats.hostname,
        os_name: stats.os_name,
        os_version: stats.os_version,
        kernel_version: stats.kernel_version,
        iggy_server_version: stats.iggy_server_version,
        iggy_server_semver: stats.iggy_server_semver,
        cache_metrics: cache_metrics_to_benchmark_cache_metrics(stats.cache_metrics),
    }
}

/// This function is a workaround.
/// See `server_stats.rs` in `bench_report` crate for more details.
fn cache_metrics_to_benchmark_cache_metrics(
    cache_metrics: HashMap<CacheMetricsKey, CacheMetrics>,
) -> HashMap<BenchmarkCacheMetricsKey, BenchmarkCacheMetrics> {
    cache_metrics
        .into_iter()
        .map(|(k, v)| {
            (
                BenchmarkCacheMetricsKey {
                    stream_id: k.stream_id,
                    topic_id: k.topic_id,
                    partition_id: k.partition_id,
                },
                BenchmarkCacheMetrics {
                    hits: v.hits,
                    misses: v.misses,
                    hit_ratio: v.hit_ratio,
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy::prelude::{ClusterNode, ClusterNodeRole, ClusterNodeStatus, TransportEndpoints};

    fn metadata_node(name: &str, role: ClusterNodeRole) -> ClusterNode {
        ClusterNode {
            name: name.to_string(),
            ip: "127.0.0.1".to_string(),
            endpoints: TransportEndpoints::new(0, 0, 0, 0),
            role,
            status: ClusterNodeStatus::Healthy,
        }
    }

    fn benchmark_cluster(node_count: usize) -> BenchmarkClusterInfo {
        BenchmarkClusterInfo {
            name: "vsr".to_string(),
            nodes: (0..node_count)
                .map(|idx| BenchmarkClusterNode {
                    name: format!("node-{idx}"),
                    role: "follower".to_string(),
                    status: "healthy".to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn given_single_node_metadata_when_classified_should_return_none() {
        let metadata = ClusterMetadata {
            name: SINGLE_NODE_CLUSTER_NAME.to_string(),
            nodes: vec![metadata_node("iggy-node", ClusterNodeRole::Leader)],
        };

        assert!(cluster_info_from_metadata(metadata).is_none());
    }

    #[test]
    fn given_multi_node_metadata_when_classified_should_map_lowercase_roles_and_keep_name() {
        let metadata = ClusterMetadata {
            name: "vsr".to_string(),
            nodes: vec![
                metadata_node("node-1", ClusterNodeRole::Leader),
                metadata_node("node-2", ClusterNodeRole::Follower),
                metadata_node("node-3", ClusterNodeRole::Follower),
            ],
        };

        let info = cluster_info_from_metadata(metadata).expect("real cluster");
        assert_eq!(info.name, "vsr");
        assert_eq!(
            info.nodes
                .iter()
                .map(|node| node.role.as_str())
                .collect::<Vec<_>>(),
            vec!["leader", "follower", "follower"]
        );
        assert!(info.nodes.iter().all(|node| node.status == "healthy"));
    }

    #[test]
    fn given_single_node_with_custom_name_when_classified_should_return_some() {
        let metadata = ClusterMetadata {
            name: "vsr".to_string(),
            nodes: vec![metadata_node("node-1", ClusterNodeRole::Leader)],
        };

        let info = cluster_info_from_metadata(metadata).expect("named roster counts as cluster");
        assert_eq!(info.nodes.len(), 1);
    }

    #[test]
    fn given_cluster_when_suffixing_identifier_should_append_node_count() {
        let mut identifier = "pinned_producer_tcp".to_string();
        identifier.push_str(&cluster_suffix(Some(&benchmark_cluster(3))));
        assert_eq!(identifier, "pinned_producer_tcp_cluster3");
    }

    #[test]
    fn given_no_cluster_when_suffixing_identifier_should_leave_it_unchanged() {
        let mut identifier = "pinned_producer_tcp".to_string();
        identifier.push_str(&cluster_suffix(None));
        assert_eq!(identifier, "pinned_producer_tcp");
    }
}
