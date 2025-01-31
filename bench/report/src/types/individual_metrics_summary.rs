use super::actor_kind::ActorKind;
use crate::benchmark_kind::BenchmarkKind;
use crate::utils::round_float;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, PartialEq, Deserialize)]
pub struct BenchmarkIndividualMetricsSummary {
    pub benchmark_kind: BenchmarkKind,
    pub actor_kind: ActorKind,
    pub actor_id: u32,
    #[serde(serialize_with = "round_float")]
    pub total_time_secs: f64,
    pub total_user_data_bytes: u64,
    pub total_bytes: u64,
    pub total_messages: u64,
    #[serde(serialize_with = "round_float")]
    pub throughput_megabytes_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub throughput_messages_per_second: f64,
    #[serde(serialize_with = "round_float")]
    pub p50_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p90_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p95_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p99_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p999_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub p9999_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub avg_latency_ms: f64,
    #[serde(serialize_with = "round_float")]
    pub median_latency_ms: f64,
}
