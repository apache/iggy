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

//! The state of a serving gateway that each scrape reads, and its encoding as metrics.
//!
//! Read at the scrape, not kept up to date by the request path: what a pool holds is a semaphore
//! count away, and the groups sit behind the coordinator's lock.

use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::ConstCounter;
use prometheus_client::metrics::gauge::ConstGauge;
use prometheus_client::registry::Unit;

use crate::listener::SecurityProtocol;
use crate::observability::health::ReadyState;

/// What a scrape reads of a serving gateway.
#[derive(Debug, Clone, Default)]
pub struct Census {
    pub listeners: Vec<ListenerCensus>,
    pub pools: Vec<PoolCensus>,
    /// Bytes of decoded Produce partitions held now, and the most the gateway holds at once.
    pub produce_memory: Option<(usize, usize)>,
    /// `None` without a bridge.
    pub produce_sessions: Option<ProduceSessionCensus>,
    pub groups: GroupCensus,
    /// `None` without the coordinator log.
    pub coordinator: Option<CoordinatorCensus>,
    /// Produce sends that may have landed although the client got an error. `None` without a
    /// bridge.
    pub sends_in_doubt: Option<u64>,
    /// Whether the bridge reaches Iggy. `None` without a bridge.
    pub iggy_reachable: Option<bool>,
}

/// The connections of one listener.
#[derive(Debug, Clone, Copy)]
pub struct ListenerCensus {
    pub protocol: SecurityProtocol,
    pub open: usize,
    pub limit: usize,
}

/// A pool of slots that requests wait for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pool {
    /// Fetch read slots (`IGGY_KAFKA_FETCH_SLOTS`).
    Fetch,
    /// Slots of large Produce requests.
    ProduceLarge,
    /// SASL credential checks (`IGGY_KAFKA_MAX_CONCURRENT_AUTHENTICATIONS`).
    Authentication,
    /// Lookups of the Iggy user a client certificate names.
    CertificateLookup,
}

impl Pool {
    const fn label(self) -> &'static str {
        match self {
            Self::Fetch => "fetch",
            Self::ProduceLarge => "produce_large",
            Self::Authentication => "authentication",
            Self::CertificateLookup => "certificate_lookup",
        }
    }
}

/// The slots of one pool in use, and how many it has.
#[derive(Debug, Clone, Copy)]
pub struct PoolCensus {
    pub pool: Pool,
    pub used: usize,
    pub capacity: usize,
}

/// The Iggy sessions of Produce.
#[derive(Debug, Clone, Copy)]
pub struct ProduceSessionCensus {
    /// Producers with a session of their own.
    pub own: usize,
    /// `IGGY_KAFKA_PRODUCE_SESSIONS`.
    pub limit: usize,
    /// Producers that found every session taken, and share one client.
    pub shared: usize,
}

/// The consumer groups this gateway coordinates now.
#[derive(Debug, Clone, Default)]
pub struct GroupCensus {
    /// Groups per Kafka state, as `ListGroups` reports them.
    pub states: Vec<(&'static str, usize)>,
    pub members: usize,
}

/// This gateway's part of the coordinator log.
#[derive(Debug, Clone, Default)]
pub struct CoordinatorCensus {
    /// The coordinator log partitions this gateway serves.
    pub served: Vec<u32>,
    /// Every partition of the log.
    pub partitions: u32,
    /// Time since this gateway last read its share of the partitions from Iggy. At 10 s it stops
    /// serving them.
    pub lease_age: Duration,
}

/// Answers the census of a serving gateway.
#[async_trait]
pub trait CensusSource: Send + Sync {
    async fn census(&self) -> Census;
}

/// The census and the readiness of the last scrape, encoded as metrics.
#[derive(Debug, Default)]
pub(super) struct CensusCollector {
    latest: Mutex<Published>,
}

#[derive(Debug, Default)]
struct Published {
    census: Census,
    ready: ReadyState,
}

impl CensusCollector {
    pub(super) fn publish(&self, census: Census, ready: ReadyState) {
        *self.latest.lock().unwrap_or_else(PoisonError::into_inner) = Published { census, ready };
    }
}

impl Collector for CensusCollector {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        // A copy, so a scrape that encodes slowly does not hold up the next one's publish.
        let (census, ready) = {
            let published = self.latest.lock().unwrap_or_else(PoisonError::into_inner);
            (published.census.clone(), published.ready.clone())
        };
        encode_readiness(&mut encoder, &ready, census.iggy_reachable)?;
        encode_connections(&mut encoder, &census.listeners)?;
        encode_pools(&mut encoder, &census)?;
        encode_groups(&mut encoder, &census.groups)?;
        if let Some(coordinator) = &census.coordinator {
            encode_coordinator(&mut encoder, coordinator)?;
        }
        Ok(())
    }
}

fn encode_readiness(
    encoder: &mut DescriptorEncoder,
    ready: &ReadyState,
    iggy_reachable: Option<bool>,
) -> Result<(), std::fmt::Error> {
    gauge(
        encoder,
        "ready",
        "1 while /ready answers 200: the gateway serves and, with a bridge, reaches Iggy",
        None,
        i64::from(*ready == ReadyState::Ready),
    )?;
    if let Some(reachable) = iggy_reachable {
        gauge(
            encoder,
            "iggy_reachable",
            "1 while the bridge reaches Iggy. Two failed pings in a row, one second apart, make \
             it 0",
            None,
            i64::from(reachable),
        )?;
    }
    Ok(())
}

fn encode_connections(
    encoder: &mut DescriptorEncoder,
    listeners: &[ListenerCensus],
) -> Result<(), std::fmt::Error> {
    labeled_gauges(
        encoder,
        "connections_open",
        "Kafka connections that each listener holds now",
        listeners
            .iter()
            .map(|listener| ([("listener", listener.protocol.to_string())], listener.open)),
    )?;
    labeled_gauges(
        encoder,
        "connections_limit",
        "Kafka connections that each listener holds at most (IGGY_KAFKA_MAX_CONNECTIONS)",
        listeners.iter().map(|listener| {
            (
                [("listener", listener.protocol.to_string())],
                listener.limit,
            )
        }),
    )
}

/// The slot pools, the Produce memory and sessions, and the sends in doubt.
fn encode_pools(encoder: &mut DescriptorEncoder, census: &Census) -> Result<(), std::fmt::Error> {
    labeled_gauges(
        encoder,
        "slots_used",
        "Slots of each pool that requests hold now",
        census
            .pools
            .iter()
            .map(|pool| ([("pool", pool.pool.label().to_string())], pool.used)),
    )?;
    labeled_gauges(
        encoder,
        "slots",
        "Slots of each pool",
        census
            .pools
            .iter()
            .map(|pool| ([("pool", pool.pool.label().to_string())], pool.capacity)),
    )?;
    if let Some((used, capacity)) = census.produce_memory {
        unit_gauge(
            encoder,
            "produce_memory_used",
            "Bytes of decoded Produce partitions that requests hold now",
            &Unit::Bytes,
            used,
        )?;
        unit_gauge(
            encoder,
            "produce_memory_capacity",
            "Bytes of decoded Produce partitions that the gateway holds at most",
            &Unit::Bytes,
            capacity,
        )?;
    }
    if let Some(sessions) = census.produce_sessions {
        gauge(
            encoder,
            "produce_sessions",
            "Producers with an Iggy session of their own",
            None,
            to_i64(sessions.own),
        )?;
        gauge(
            encoder,
            "produce_sessions_limit",
            "Producers that get an Iggy session of their own at most \
             (IGGY_KAFKA_PRODUCE_SESSIONS)",
            None,
            to_i64(sessions.limit),
        )?;
        gauge(
            encoder,
            "produce_shared_producers",
            "Producers that found every Produce session taken, and send one at a time on one \
             shared Iggy client",
            None,
            to_i64(sessions.shared),
        )?;
    }
    if let Some(in_doubt) = census.sends_in_doubt {
        let counter = ConstCounter::new(in_doubt);
        let metric = encoder.encode_descriptor(
            "produce_sends_in_doubt",
            "Produce sends to Iggy whose outcome is unknown: the client got 7 \
             (REQUEST_TIMED_OUT), and the records may still have landed",
            None,
            counter.metric_type(),
        )?;
        counter.encode(metric)?;
    }
    Ok(())
}

fn encode_groups(
    encoder: &mut DescriptorEncoder,
    groups: &GroupCensus,
) -> Result<(), std::fmt::Error> {
    labeled_gauges(
        encoder,
        "groups",
        "Consumer groups that this gateway coordinates, per Kafka state",
        groups
            .states
            .iter()
            .map(|(state, count)| ([("state", (*state).to_string())], *count)),
    )?;
    gauge(
        encoder,
        "group_members",
        "Members of the consumer groups that this gateway coordinates",
        None,
        to_i64(groups.members),
    )
}

fn encode_coordinator(
    encoder: &mut DescriptorEncoder,
    coordinator: &CoordinatorCensus,
) -> Result<(), std::fmt::Error> {
    gauge(
        encoder,
        "coordinator_partitions_served",
        "Coordinator log partitions that this gateway serves",
        None,
        to_i64(coordinator.served.len()),
    )?;
    labeled_gauges(
        encoder,
        "coordinator_partition_served",
        "1 for each coordinator log partition that this gateway serves, else 0",
        (0..coordinator.partitions).map(|partition| {
            (
                [("partition", partition.to_string())],
                usize::from(coordinator.served.contains(&partition)),
            )
        }),
    )?;
    let age = ConstGauge::new(coordinator.lease_age.as_secs_f64());
    let metric = encoder.encode_descriptor(
        "coordinator_lease_age",
        "Seconds since this gateway last read its share of the coordinator log partitions from \
         Iggy. At 10 it stops serving them",
        Some(&Unit::Seconds),
        age.metric_type(),
    )?;
    age.encode(metric)
}

fn gauge(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    unit: Option<&Unit>,
    value: i64,
) -> Result<(), std::fmt::Error> {
    let gauge = ConstGauge::new(value);
    let metric = encoder.encode_descriptor(name, help, unit, gauge.metric_type())?;
    gauge.encode(metric)
}

fn unit_gauge(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    unit: &Unit,
    value: usize,
) -> Result<(), std::fmt::Error> {
    gauge(encoder, name, help, Some(unit), to_i64(value))
}

/// One gauge per label set. Writes nothing when `values` is empty, so a family with no member
/// leaves no empty descriptor behind.
fn labeled_gauges(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    values: impl Iterator<Item = ([(&'static str, String); 1], usize)>,
) -> Result<(), std::fmt::Error> {
    let mut values = values.peekable();
    if values.peek().is_none() {
        return Ok(());
    }
    let mut metric = encoder.encode_descriptor(name, help, None, MetricType::Gauge)?;
    for (labels, value) in values {
        let family = metric.encode_family(&labels)?;
        ConstGauge::new(to_i64(value)).encode(family)?;
    }
    Ok(())
}

fn to_i64(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
