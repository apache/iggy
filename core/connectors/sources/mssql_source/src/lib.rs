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

mod client;
mod encode;
mod error;
mod lsn;
mod table;

use async_trait::async_trait;
use client::{CaptureInstance, CdcClient, ChangeKey};
use encode::{EncodedEvent, cut, to_events};
use error::MssqlError;
use humantime::Duration as HumanDuration;
use iggy_common::MAX_PAYLOAD_SIZE;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::{BatchPolicy, SourceBatchResult},
    source_connector,
};
use lsn::{Lsn, Position};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use table::TableName;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

source_connector!(MssqlSource);

const CONNECTOR_NAME: &str = "MSSQL source";
const DEFAULT_POLL_INTERVAL: &str = "1s";
const DEFAULT_QUERY_TIMEOUT: &str = "30s";
const DEFAULT_BATCH_SIZE: usize = 1000;
const STUCK_LOG_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Deserialize)]
pub struct MssqlSourceConfig {
    /// ADO.NET connection string, including `Database=`. Never logged.
    pub connection_string: SecretString,
    /// Tables to stream, as unquoted `schema.table`. Each must already be enabled for CDC.
    pub tables: Vec<String>,
    /// Delay between polls. Also the reconnect backoff.
    pub poll_interval: Option<String>,
    /// Target number of change rows in one batch. A batch can exceed it to finish a change.
    pub batch_size: Option<usize>,
    /// Upper bound for one query. The connection is dropped when it expires.
    pub query_timeout: Option<String>,
    /// Where to start when there is no saved checkpoint.
    #[serde(default)]
    pub start_position: StartPosition,
    /// Upgrades per-batch logs to `info!`.
    pub verbose_logging: Option<bool>,
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    /// Read every change still retained by CDC.
    #[default]
    Earliest,
    /// Skip the retained history and read changes committed after the first start.
    Latest,
}

pub struct MssqlSource {
    id: u32,
    config: MssqlSourceConfig,
    validated: Option<ValidatedConfig>,
    /// A saved state was present but could not be decoded. `open()` refuses to start.
    state_unreadable: bool,
    // The client is moved out for I/O so no lock is held across a network await.
    client: Mutex<Option<CdcClient>>,
    instances: Vec<CaptureInstance>,
    state: Mutex<State>,
    /// `open()` changed the state. Stage it on every poll until an ACK.
    open_changes_pending: AtomicBool,
    candidate: Mutex<Option<Candidate>>,
    rows_emitted: AtomicU64,
    /// The last ACKed batch left changes behind, so the next poll starts without waiting.
    skip_next_sleep: AtomicBool,
    /// The stuck position last logged at `error`, and when.
    last_stuck_log: Mutex<Option<(Position, Instant)>>,
    /// While a change is stuck, it is read again only after this. Change-table rows never change.
    retry_stuck_at: Mutex<Option<Instant>>,
}

impl fmt::Debug for MssqlSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MssqlSource")
            .field("id", &self.id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct ValidatedConfig {
    tiberius_config: tiberius::Config,
    poll_interval: Duration,
    query_timeout: Duration,
    batch_size: usize,
    tables: Vec<TableName>,
    verbose_logging: bool,
}

// Fields are append-only with #[serde(default)]: rmp_serde writes structs as positional arrays.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct State {
    database: Option<DatabaseIdentity>,
    /// Last emitted change, or an idle watermark `(max_lsn, Lsn::MAX)`.
    checkpoint: Option<Position>,
    /// Keyed by the canonical `schema.table`.
    tables: BTreeMap<String, TableState>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct DatabaseIdentity {
    name: String,
    recovery_fork_guid: Uuid,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TableState {
    capture_instance: String,
    /// The instance minimum when the table was first read. No rows are expected below it.
    floor: Lsn,
}

#[derive(Debug)]
enum Candidate {
    Batch {
        state: Option<State>,
        messages: u64,
        behind: bool,
    },
    Stop(MssqlError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Choice {
    Use(usize),
    Gap { min_lsn: Lsn },
    NoUsable,
}

impl MssqlSource {
    pub fn new(id: u32, config: MssqlSourceConfig, state: Option<ConnectorState>) -> Self {
        let (state, state_unreadable) = match state {
            None => (State::default(), false),
            Some(saved) => match saved.deserialize::<State>(CONNECTOR_NAME, id) {
                Some(state) => (state, false),
                None => (State::default(), true),
            },
        };
        Self {
            id,
            config,
            validated: None,
            state_unreadable,
            client: Mutex::new(None),
            instances: Vec::new(),
            state: Mutex::new(state),
            open_changes_pending: AtomicBool::new(false),
            candidate: Mutex::new(None),
            rows_emitted: AtomicU64::new(0),
            skip_next_sleep: AtomicBool::new(false),
            last_stuck_log: Mutex::new(None),
            retry_stuck_at: Mutex::new(None),
        }
    }

    async fn open_with(&mut self, validated: &ValidatedConfig) -> Result<(), MssqlError> {
        let mut client =
            CdcClient::connect(&validated.tiberius_config, validated.query_timeout).await?;
        let mut state = self.state.lock().await.clone();
        let original = state.clone();

        if !client.has_time_zone_support() {
            warn!(
                "{CONNECTOR_NAME} connector with ID {}: SQL Server is older than 2022, so commit_time uses today's UTC offset for every change",
                self.id
            );
        }
        let (identity, max_lsn) = read_identity(&mut client, &state).await?;
        let database = identity.name.clone();
        state.database = Some(identity);

        let mut resolved_tables = Vec::with_capacity(validated.tables.len());
        let mut canonical: BTreeMap<TableName, Vec<String>> = BTreeMap::new();
        for table in &validated.tables {
            let resolved = client.resolve_table(table).await?;
            canonical
                .entry(resolved.clone())
                .or_default()
                .push(table.to_string());
            resolved_tables.push(resolved);
        }
        if let Some((table, configured)) = canonical.iter().find(|(_, names)| names.len() > 1) {
            return Err(MssqlError::DuplicateTable {
                table: table.to_string(),
                configured: configured.clone(),
            });
        }
        let configured: HashSet<String> = canonical.keys().map(TableName::to_string).collect();
        state.tables.retain(|table, _| configured.contains(table));

        let mut instances = Vec::with_capacity(resolved_tables.len());
        for resolved in resolved_tables {
            let key = resolved.to_string();
            let discovered = client.capture_instances(&resolved).await?;
            if discovered.is_empty() {
                return Err(MssqlError::TableNotCaptured { table: key });
            }
            let mut minimums = Vec::with_capacity(discovered.len());
            for instance in &discovered {
                minimums.push((instance.name.clone(), client.min_lsn(&instance.name).await?));
            }
            let recorded = state.tables.get(&key);
            let needed = recorded.map(|entry| next_needed(state.checkpoint, entry.floor));
            let choice = choose_instance(
                &minimums,
                needed.map(|position| position.start_lsn),
                recorded.map(|entry| entry.capture_instance.as_str()),
            );
            let index = match choice {
                Choice::Use(index) => index,
                Choice::Gap { min_lsn } => {
                    return Err(MssqlError::LsnGap {
                        table: key,
                        next_needed: needed.map_or(Lsn::ZERO, |position| position.start_lsn),
                        min_lsn,
                    });
                }
                Choice::NoUsable => {
                    return Err(MssqlError::NoUsableInstance {
                        table: key,
                        instances: minimums,
                        next_needed: needed.map_or(Lsn::ZERO, |position| position.start_lsn),
                    });
                }
            };
            let (name, min_lsn) = minimums[index].clone();
            match state.tables.get_mut(&key) {
                Some(entry) if entry.capture_instance != name => {
                    info!(
                        "{CONNECTOR_NAME} connector with ID {} switches {key} from capture instance {} to {name}",
                        self.id, entry.capture_instance
                    );
                    entry.capture_instance = name.clone();
                }
                Some(_) => {}
                None => {
                    state.tables.insert(
                        key.clone(),
                        TableState {
                            capture_instance: name.clone(),
                            floor: min_lsn,
                        },
                    );
                }
            }
            let chosen = discovered
                .into_iter()
                .nth(index)
                .ok_or(MssqlError::MissingMetadata("chosen capture instance"))?;
            instances.push(CaptureInstance::new(chosen, resolved)?);
        }

        if state.checkpoint.is_none() && self.config.start_position == StartPosition::Latest {
            state.checkpoint = Some(latest_checkpoint(max_lsn));
        }

        if state != original {
            self.open_changes_pending.store(true, Ordering::SeqCst);
        }
        info!(
            "Opened {CONNECTOR_NAME} connector with ID {} on database {}, instances [{}], checkpoint {}",
            self.id,
            database,
            instances
                .iter()
                .map(|instance| format!("{} ({})", instance.table, instance.name))
                .collect::<Vec<_>>()
                .join(", "),
            describe_checkpoint(state.checkpoint),
        );
        self.instances = instances;
        *self.state.lock().await = state;
        *self.client.lock().await = Some(client);
        Ok(())
    }

    async fn read_batch(
        &self,
        client: &mut CdcClient,
        validated: &ValidatedConfig,
    ) -> Result<(Vec<ProducedMessage>, Candidate), MssqlError> {
        let state = self.state.lock().await.clone();
        let database = state
            .database
            .as_ref()
            .map_or("", |identity| identity.name.as_str());
        // A disabled database has no cdc.lsn_time_mapping, so the maximum fails with error 208.
        let max_lsn = match client.max_lsn().await {
            Err(error) if error.is_missing_object() => {
                return match client.check_cdc_enabled(database).await {
                    Err(disabled @ MssqlError::CdcNotEnabled { .. }) => {
                        Ok((Vec::new(), Candidate::Stop(disabled)))
                    }
                    Err(other) => Err(other),
                    Ok(()) => Err(error),
                };
            }
            result => result?,
        };
        let Some(to) = max_lsn else {
            return Ok((
                Vec::new(),
                self.batch_candidate(&state, state.clone(), 0, false),
            ));
        };
        if let Err(error) = check_checkpoint(state.checkpoint, to) {
            return Ok((Vec::new(), Candidate::Stop(error)));
        }
        // At the idle watermark nothing new can exist, so only the minimums below are read.
        let idle = state.checkpoint == Some(Position::new(to, Lsn::MAX));
        let readable: &[CaptureInstance] = if idle { &[] } else { &self.instances };

        let needed: Vec<Position> = self
            .instances
            .iter()
            .map(|instance| {
                let floor = state
                    .tables
                    .get(&instance.key)
                    .map_or(Lsn::ZERO, |entry| entry.floor);
                next_needed(state.checkpoint, floor)
            })
            .collect();

        let mut keys: Vec<ChangeKey> = Vec::new();
        let mut any_full = false;
        let mut query_error = None;
        for (index, instance) in readable.iter().enumerate() {
            match client
                .change_keys(instance, index, needed[index], to, validated.batch_size)
                .await
            {
                Ok(instance_keys) => {
                    any_full |= instance_keys.len() >= validated.batch_size;
                    keys.extend(instance_keys);
                }
                Err(error) if error.is_lsn_out_of_range() || error.is_missing_object() => {
                    query_error = Some(error);
                    break;
                }
                Err(error) => return Err(error),
            }
        }

        let batch = cut(keys, validated.batch_size);
        let mut rows = Vec::new();
        if query_error.is_none()
            && let Some(last) = batch.last
        {
            for &index in &batch.instances {
                match client
                    .change_rows(&self.instances[index], index, needed[index], last, to)
                    .await
                {
                    Ok(instance_rows) => rows.extend(instance_rows),
                    Err(error) if error.is_lsn_out_of_range() || error.is_missing_object() => {
                        query_error = Some(error);
                        break;
                    }
                    Err(error @ MssqlError::Conversion { .. }) => {
                        return Err(self.log_stuck(client, error).await);
                    }
                    // The row stream stopped mid-read, so poll() drops this client and logs without it.
                    Err(MssqlError::Decode(message)) => {
                        return Err(MssqlError::Undecodable {
                            table: self.instances[index].table.to_string(),
                            after: needed[index],
                            message,
                        });
                    }
                    Err(error) => return Err(error),
                }
            }
        }

        // Read after the change queries so a cleanup during the scan is caught too.
        for (index, instance) in self.instances.iter().enumerate() {
            let min_lsn = client.min_lsn(&instance.name).await?;
            if let Some(error) = check_minimum(instance, needed[index], min_lsn) {
                return Ok((Vec::new(), Candidate::Stop(error)));
            }
        }
        if let Some(error) = query_error {
            return Err(error);
        }

        let events = match to_events(rows, &self.instances, database) {
            Ok(events) => events,
            Err(error) => return Err(self.log_stuck(client, error).await),
        };
        let mut messages = Vec::with_capacity(events.len());
        for event in &events {
            let message =
                produced_message(event).map_err(|error| MssqlError::Encoding(error.to_string()))?;
            if let Err(error) =
                check_payload_size(event.table, event.position, message.payload.len())
            {
                return Err(self.log_stuck(client, error).await);
            }
            messages.push(message);
        }

        let behind = any_full || batch.left_behind;
        let mut candidate = state.clone();
        candidate.checkpoint = if behind {
            batch.last.or(state.checkpoint)
        } else {
            Some(Position::new(to, Lsn::MAX))
        };
        let count = messages.len() as u64;
        Ok((
            messages,
            self.batch_candidate(&state, candidate, count, behind),
        ))
    }

    /// Carries the new state only when it differs from the committed one or `open()` changes are pending.
    fn batch_candidate(
        &self,
        state: &State,
        candidate: State,
        messages: u64,
        behind: bool,
    ) -> Candidate {
        let changed = candidate != *state || self.open_changes_pending.load(Ordering::SeqCst);
        Candidate::Batch {
            state: changed.then_some(candidate),
            messages,
            behind,
        }
    }

    /// Logs a change that blocks progress, with its age when the connection can still read it.
    async fn log_stuck(&self, client: &mut CdcClient, error: MssqlError) -> MssqlError {
        let Some(position) = error.stuck_position() else {
            return error;
        };
        let age = match client.lsn_age_seconds(position.start_lsn).await {
            Ok(age) => age,
            Err(age_error) if age_error.breaks_connection() => {
                self.report_stuck(position, None, &error).await;
                return age_error;
            }
            Err(age_error) => {
                warn!(
                    "{CONNECTOR_NAME} connector with ID {} cannot read the commit time of start_lsn {}: {age_error}",
                    self.id, position.start_lsn
                );
                None
            }
        };
        self.report_stuck(position, age, &error).await;
        error
    }

    /// Logs at `error` once a minute per stuck position, and at `debug` in between.
    async fn report_stuck(&self, position: Position, age_seconds: Option<i64>, error: &MssqlError) {
        let age =
            age_seconds.map_or_else(|| "unknown".to_string(), |seconds| format!("{seconds}s"));
        let mut last = self.last_stuck_log.lock().await;
        let now = Instant::now();
        if should_log_stuck(*last, position, now) {
            error!(
                "{CONNECTOR_NAME} connector with ID {} is stuck at start_lsn {} (committed {age} ago). Rows are lost once CDC retention passes it. {error}",
                self.id, position.start_lsn
            );
            *last = Some((position, now));
        } else {
            debug!(
                "{CONNECTOR_NAME} connector with ID {} is still stuck at start_lsn {}: {error}",
                self.id, position.start_lsn
            );
        }
    }

    async fn reconnect(&self, validated: &ValidatedConfig) -> Result<CdcClient, MssqlError> {
        let mut client =
            CdcClient::connect(&validated.tiberius_config, validated.query_timeout).await?;
        let state = self.state.lock().await.clone();
        let (identity, _) = read_identity(&mut client, &state).await?;
        // debug, not info: a stuck undecodable change reconnects on every poll.
        debug!(
            "{CONNECTOR_NAME} connector with ID {} reconnected to database {}",
            self.id, identity.name
        );
        Ok(client)
    }

    async fn stage_stop(&self, error: MssqlError) -> ProducedMessages {
        error!(
            "{CONNECTOR_NAME} connector with ID {} will stop: {error}",
            self.id
        );
        *self.candidate.lock().await = Some(Candidate::Stop(error));
        ProducedMessages {
            schema: Schema::Json,
            messages: Vec::new(),
            state: None,
        }
    }

    /// An empty batch while a change is stuck. It still carries pending open() changes, such as the `latest` start.
    async fn stuck_batch(&self) -> Result<ProducedMessages, Error> {
        let committed = self.state.lock().await.clone();
        let candidate = self.batch_candidate(&committed, committed.clone(), 0, false);
        let serialized = match &candidate {
            Candidate::Batch {
                state: Some(state), ..
            } => Some(self.serialize_state(state)?),
            _ => None,
        };
        *self.candidate.lock().await = Some(candidate);
        Ok(ProducedMessages {
            schema: Schema::Json,
            messages: Vec::new(),
            state: serialized,
        })
    }

    fn serialize_state(&self, state: &State) -> Result<ConnectorState, Error> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
            .ok_or_else(|| Error::Serialization("cannot serialize connector state".to_string()))
    }
}

#[async_trait]
impl Source for MssqlSource {
    async fn open(&mut self) -> Result<(), Error> {
        if self.state_unreadable {
            error!(
                "{CONNECTOR_NAME} connector with ID {} cannot decode its saved state",
                self.id
            );
            return Err(Error::InitError(
                "saved state cannot be decoded. Reset the connector's saved state to start over"
                    .to_string(),
            ));
        }
        let validated = validate_config(&self.config).inspect_err(|error| {
            error!(
                "Failed to validate {CONNECTOR_NAME} connector with ID {}: {error}",
                self.id
            );
        })?;
        if let Err(error) = self.open_with(&validated).await {
            error!(
                "Failed to open {CONNECTOR_NAME} connector with ID {}: {error}",
                self.id
            );
            return Err(Error::InitError(error.to_string()));
        }
        self.validated = Some(validated);
        Ok(())
    }

    fn batch_policy(&self) -> BatchPolicy {
        // CDC rows can be read again, so an Iggy outage must not stop the source.
        BatchPolicy::default().with_max_consecutive_nacks(None)
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let Some(validated) = self.validated.as_ref() else {
            // Unreachable after a successful open(). The sleep keeps an SDK retry from spinning.
            sleep(Duration::from_secs(1)).await;
            return Err(Error::InvalidState);
        };
        if !self.skip_next_sleep.swap(false, Ordering::SeqCst) {
            sleep(validated.poll_interval).await;
        }
        if !stuck_retry_due(*self.retry_stuck_at.lock().await, Instant::now()) {
            return self.stuck_batch().await;
        }

        let existing = self.client.lock().await.take();
        let mut client = match existing {
            Some(client) => client,
            None => match self.reconnect(validated).await {
                Ok(client) => client,
                Err(
                    error @ (MssqlError::DatabaseChanged { .. } | MssqlError::CdcNotEnabled { .. }),
                ) => {
                    return Ok(self.stage_stop(error).await);
                }
                // The SDK logs the returned error, so it is not logged here as well.
                Err(error) => return Err(error.into()),
            },
        };

        let outcome = self.read_batch(&mut client, validated).await;
        // A broken client is dropped here. The SDK logs the returned error, so it is not logged twice.
        if !matches!(&outcome, Err(error) if error.breaks_connection()) {
            *self.client.lock().await = Some(client);
        }

        // A stuck change is logged once a minute. An Err would make the SDK log it again on every poll.
        if let Err(error) = &outcome
            && let Some(position) = error.stuck_position()
        {
            if matches!(error, MssqlError::Undecodable { .. }) {
                self.report_stuck(position, None, error).await;
            }
            *self.retry_stuck_at.lock().await = Some(Instant::now() + STUCK_LOG_INTERVAL);
            return self.stuck_batch().await;
        }
        let (messages, candidate) = outcome?;
        self.retry_stuck_at.lock().await.take();
        match candidate {
            Candidate::Stop(error) => Ok(self.stage_stop(error).await),
            Candidate::Batch { ref state, .. } => {
                let count = messages.len() as u64;
                let serialized = state
                    .as_ref()
                    .map(|state| self.serialize_state(state))
                    .transpose()?;
                if count > 0 {
                    if validated.verbose_logging {
                        info!(
                            "{CONNECTOR_NAME} connector with ID {} polled {count} changes",
                            self.id
                        );
                    } else {
                        debug!(
                            "{CONNECTOR_NAME} connector with ID {} polled {count} changes",
                            self.id
                        );
                    }
                }
                *self.candidate.lock().await = Some(candidate);
                Ok(ProducedMessages {
                    schema: Schema::Json,
                    messages,
                    state: serialized,
                })
            }
        }
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let candidate = self.candidate.lock().await.take();
        match (candidate, result) {
            // stage_stop already logged the cause.
            (Some(Candidate::Stop(error)), _) => Err(error.into()),
            (
                Some(Candidate::Batch {
                    state,
                    messages,
                    behind,
                }),
                SourceBatchResult::Ack,
            ) => {
                if let Some(state) = state {
                    *self.state.lock().await = state;
                    self.open_changes_pending.store(false, Ordering::SeqCst);
                }
                self.rows_emitted.fetch_add(messages, Ordering::Relaxed);
                self.skip_next_sleep.store(behind, Ordering::SeqCst);
                Ok(())
            }
            (Some(Candidate::Batch { .. }), SourceBatchResult::Nack) => {
                warn!(
                    "{CONNECTOR_NAME} connector with ID {} batch was not acknowledged. It is read again from the last checkpoint",
                    self.id
                );
                Ok(())
            }
            (None, _) => Ok(()),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.candidate.lock().await.take();
        self.client.lock().await.take();
        info!(
            "Closed {CONNECTOR_NAME} connector with ID {} after emitting {} changes",
            self.id,
            self.rows_emitted.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

fn validate_config(config: &MssqlSourceConfig) -> Result<ValidatedConfig, Error> {
    if config.tables.is_empty() {
        return Err(Error::InvalidConfigValue(
            "tables must list at least one schema.table".to_string(),
        ));
    }
    let mut seen = HashSet::with_capacity(config.tables.len());
    let mut tables = Vec::with_capacity(config.tables.len());
    for name in &config.tables {
        let table = TableName::from_str(name)
            .map_err(|error| Error::InvalidConfigValue(format!("tables: '{name}': {error}")))?;
        if !seen.insert(name.as_str()) {
            return Err(Error::InvalidConfigValue(format!(
                "tables: '{name}' is listed twice"
            )));
        }
        tables.push(table);
    }
    let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
    if batch_size == 0 {
        return Err(Error::InvalidConfigValue(
            "batch_size must be greater than 0".to_string(),
        ));
    }
    let poll_interval = parse_duration(
        config.poll_interval.as_deref(),
        DEFAULT_POLL_INTERVAL,
        "poll_interval",
    )?;
    let query_timeout = parse_duration(
        config.query_timeout.as_deref(),
        DEFAULT_QUERY_TIMEOUT,
        "query_timeout",
    )?;
    if names_instance_without_port(config.connection_string.expose_secret()) {
        return Err(Error::InvalidConfigValue(
            "connection_string names an instance (host\\instance) without a port. SQL Browser lookup is not \
             supported, so add the instance's port: Server=tcp:host,port"
                .to_string(),
        ));
    }
    // The parse error can quote the string, so it is not passed on.
    let tiberius_config = tiberius::Config::from_ado_string(
        config.connection_string.expose_secret(),
    )
    .map_err(|_| {
        Error::InvalidConfigValue(
            "connection_string is not a valid ADO.NET connection string".to_string(),
        )
    })?;
    Ok(ValidatedConfig {
        tiberius_config,
        poll_interval,
        query_timeout,
        batch_size,
        tables,
        verbose_logging: config.verbose_logging.unwrap_or(false),
    })
}

/// A `Server=host\instance` value without `,port` makes the driver ask SQL Browser on UDP 1434.
fn names_instance_without_port(connection_string: &str) -> bool {
    connection_string
        .split(';')
        .filter_map(|pair| pair.split_once('='))
        .filter(|(key, _)| {
            matches!(
                key.trim().to_ascii_lowercase().as_str(),
                "server" | "data source" | "address" | "addr" | "network address"
            )
        })
        .any(|(_, value)| {
            let value = value.trim();
            value.contains('\\') && !value.contains(',')
        })
}

fn parse_duration(value: Option<&str>, default: &str, field: &str) -> Result<Duration, Error> {
    let duration = HumanDuration::from_str(value.unwrap_or(default))
        .map(|duration| *duration)
        .map_err(|error| Error::InvalidConfigValue(format!("{field}: {error}")))?;
    if duration.is_zero() {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must be greater than 0"
        )));
    }
    Ok(duration)
}

/// Reads the database identity and maximum LSN, and fails when they no longer match `state`.
async fn read_identity(
    client: &mut CdcClient,
    state: &State,
) -> Result<(DatabaseIdentity, Option<Lsn>), MssqlError> {
    let (name, recovery_fork_guid) = client.database().await?;
    client.check_cdc_enabled(&name).await?;
    let max_lsn = client.max_lsn().await?;
    let identity = DatabaseIdentity {
        name,
        recovery_fork_guid,
    };
    check_identity(state, &identity, max_lsn)?;
    Ok((identity, max_lsn))
}

fn check_identity(
    state: &State,
    identity: &DatabaseIdentity,
    max_lsn: Option<Lsn>,
) -> Result<(), MssqlError> {
    if let Some(saved) = &state.database
        && saved != identity
    {
        return Err(MssqlError::DatabaseChanged {
            reason: format!(
                "the checkpoint belongs to database {} (fork {}), but the connection reaches {} (fork {}). It was restored, re-created or replaced",
                saved.name, saved.recovery_fork_guid, identity.name, identity.recovery_fork_guid
            ),
        });
    }
    match max_lsn {
        Some(max_lsn) => check_checkpoint(state.checkpoint, max_lsn),
        None => Ok(()),
    }
}

/// A checkpoint above the newest LSN means the database was restored or replaced under the connector.
fn check_checkpoint(checkpoint: Option<Position>, max_lsn: Lsn) -> Result<(), MssqlError> {
    match checkpoint {
        Some(checkpoint) if checkpoint.start_lsn > max_lsn => Err(MssqlError::DatabaseChanged {
            reason: format!(
                "checkpoint {} is above the maximum LSN {max_lsn}",
                checkpoint.start_lsn
            ),
        }),
        _ => Ok(()),
    }
}

/// The checkpoint when it is at or above the table's floor, else the floor itself.
fn next_needed(checkpoint: Option<Position>, floor: Lsn) -> Position {
    match checkpoint {
        Some(checkpoint) if checkpoint.start_lsn >= floor => checkpoint,
        _ => Position::new(floor, Lsn::ZERO),
    }
}

/// The newest instance whose minimum is at or below `next_needed`. Zero minimums are unusable.
fn choose_instance(
    instances: &[(String, Lsn)],
    next_needed: Option<Lsn>,
    recorded: Option<&str>,
) -> Choice {
    let usable = |min_lsn: &Lsn| !min_lsn.is_zero();
    let Some(next_needed) = next_needed else {
        return instances
            .iter()
            .position(|(_, min_lsn)| usable(min_lsn))
            .map_or(Choice::NoUsable, Choice::Use);
    };
    if let Some(index) = instances
        .iter()
        .position(|(_, min_lsn)| usable(min_lsn) && *min_lsn <= next_needed)
    {
        return Choice::Use(index);
    }
    match instances
        .iter()
        .find(|(name, min_lsn)| Some(name.as_str()) == recorded && usable(min_lsn))
    {
        Some((_, min_lsn)) => Choice::Gap { min_lsn: *min_lsn },
        None => Choice::NoUsable,
    }
}

/// A gap when cleanup moved the minimum past the next needed LSN, or a missing instance when it is zero.
fn check_minimum(
    instance: &CaptureInstance,
    next_needed: Position,
    min_lsn: Lsn,
) -> Option<MssqlError> {
    if min_lsn.is_zero() {
        return Some(MssqlError::InstanceMissing {
            table: instance.key.clone(),
            capture_instance: instance.name.clone(),
        });
    }
    (min_lsn > next_needed.start_lsn).then(|| MssqlError::LsnGap {
        table: instance.key.clone(),
        next_needed: next_needed.start_lsn,
        min_lsn,
    })
}

/// No checkpoint yet under `latest`. A NULL maximum means no change exists, so every table reads from its floor.
fn latest_checkpoint(max_lsn: Option<Lsn>) -> Position {
    match max_lsn {
        Some(max_lsn) => Position::new(max_lsn, Lsn::MAX),
        None => Position::new(Lsn::ZERO, Lsn::ZERO),
    }
}

/// True when no change is stuck, or the wait before reading a stuck change again is over.
fn stuck_retry_due(retry_at: Option<Instant>, now: Instant) -> bool {
    retry_at.is_none_or(|at| now >= at)
}

/// A new stuck position logs at once. The same position logs again once `STUCK_LOG_INTERVAL` passed.
fn should_log_stuck(last: Option<(Position, Instant)>, position: Position, now: Instant) -> bool {
    !matches!(last, Some((logged, at)) if logged == position && now.duration_since(at) < STUCK_LOG_INTERVAL)
}

/// Iggy rejects a larger message, and the batch would be NACKed forever.
fn check_payload_size(
    table: &TableName,
    position: Position,
    bytes: usize,
) -> Result<(), MssqlError> {
    if bytes > MAX_PAYLOAD_SIZE as usize {
        return Err(MssqlError::PayloadTooLarge {
            table: table.to_string(),
            position,
            bytes,
            limit: MAX_PAYLOAD_SIZE,
        });
    }
    Ok(())
}

fn produced_message(event: &EncodedEvent<'_>) -> Result<ProducedMessage, serde_json::Error> {
    Ok(ProducedMessage {
        id: Some(event.id),
        checksum: None,
        timestamp: None,
        origin_timestamp: event
            .commit_time
            .and_then(|time| u64::try_from(time.timestamp_micros()).ok()),
        headers: None,
        payload: serde_json::to_vec(&event.event)?,
    })
}

fn describe_checkpoint(checkpoint: Option<Position>) -> String {
    checkpoint.map_or_else(|| "none".to_string(), |position| position.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use client::{CapturedColumn, DiscoveredInstance};
    use lsn::test_support::{lsn, position};

    const CONNECTION_STRING: &str = "Server=tcp:localhost,1433;Database=shop;User Id=sa;Password=Secret_Passw0rd;TrustServerCertificate=true";

    fn test_config() -> MssqlSourceConfig {
        MssqlSourceConfig {
            connection_string: SecretString::from(CONNECTION_STRING),
            tables: vec!["dbo.users".to_string()],
            poll_interval: None,
            batch_size: None,
            query_timeout: None,
            start_position: StartPosition::default(),
            verbose_logging: None,
        }
    }

    fn identity(fork: u128) -> DatabaseIdentity {
        DatabaseIdentity {
            name: "shop".to_string(),
            recovery_fork_guid: Uuid::from_u128(fork),
        }
    }

    fn saved_state() -> State {
        State {
            database: Some(identity(1)),
            checkpoint: Some(position(5, 2)),
            tables: BTreeMap::from([(
                "dbo.users".to_string(),
                TableState {
                    capture_instance: "dbo_users".to_string(),
                    floor: lsn(1),
                },
            )]),
        }
    }

    fn users_instance() -> CaptureInstance {
        let discovered = DiscoveredInstance {
            name: "dbo_users".to_string(),
            columns: vec![CapturedColumn::new(
                "id".to_string(),
                1,
                "int".to_string(),
                Some(4),
            )],
        };
        CaptureInstance::new(discovered, TableName::new("dbo", "users")).expect("supported")
    }

    fn instances(minimums: &[(&str, u8)]) -> Vec<(String, Lsn)> {
        minimums
            .iter()
            .map(|(name, min_lsn)| (name.to_string(), lsn(*min_lsn)))
            .collect()
    }

    fn stage_batch(source: &MssqlSource, behind: bool) {
        *source.candidate.blocking_lock() = Some(Candidate::Batch {
            state: Some(saved_state()),
            messages: 3,
            behind,
        });
    }

    fn block_on<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(future)
    }

    fn rejected(config: MssqlSourceConfig) -> String {
        match validate_config(&config) {
            Err(Error::InvalidConfigValue(message)) => message,
            other => panic!("expected InvalidConfigValue, got {other:?}"),
        }
    }

    #[test]
    fn given_empty_tables_should_reject_configuration() {
        let config = MssqlSourceConfig {
            tables: Vec::new(),
            ..test_config()
        };

        assert!(rejected(config).contains("tables"));
    }

    #[test]
    fn given_table_without_schema_should_reject_configuration() {
        let config = MssqlSourceConfig {
            tables: vec!["users".to_string()],
            ..test_config()
        };

        assert!(rejected(config).contains("'users'"));
    }

    #[test]
    fn given_bracketed_table_should_reject_configuration() {
        let config = MssqlSourceConfig {
            tables: vec!["[dbo].[users]".to_string()],
            ..test_config()
        };

        assert!(rejected(config).contains("brackets"));
    }

    #[test]
    fn given_duplicate_table_should_reject_configuration() {
        let config = MssqlSourceConfig {
            tables: vec!["dbo.users".to_string(), "dbo.users".to_string()],
            ..test_config()
        };

        assert!(rejected(config).contains("twice"));
    }

    #[test]
    fn given_zero_batch_size_should_reject_configuration() {
        let config = MssqlSourceConfig {
            batch_size: Some(0),
            ..test_config()
        };

        assert!(rejected(config).contains("batch_size"));
    }

    #[test]
    fn given_zero_poll_interval_should_reject_configuration() {
        let config = MssqlSourceConfig {
            poll_interval: Some("0s".to_string()),
            ..test_config()
        };

        assert!(rejected(config).contains("poll_interval"));
    }

    #[test]
    fn given_invalid_query_timeout_should_reject_configuration() {
        let config = MssqlSourceConfig {
            query_timeout: Some("soon".to_string()),
            ..test_config()
        };

        assert!(rejected(config).contains("query_timeout"));
    }

    #[test]
    fn given_invalid_connection_string_should_reject_configuration_without_echoing_it() {
        let config = MssqlSourceConfig {
            connection_string: SecretString::from("Server=tcp:localhost,notaport;Password=hunter2"),
            ..test_config()
        };

        let message = rejected(config);

        assert!(message.contains("connection_string"));
        assert!(!message.contains("hunter2"));
    }

    #[test]
    fn given_missing_optional_fields_should_use_defaults() {
        let validated = validate_config(&test_config()).expect("valid");

        assert_eq!(validated.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(validated.poll_interval, Duration::from_secs(1));
        assert_eq!(validated.query_timeout, Duration::from_secs(30));
        assert!(!validated.verbose_logging);
        assert_eq!(validated.tables, vec![TableName::new("dbo", "users")]);
    }

    #[test]
    fn given_missing_start_position_should_default_to_earliest() {
        let config: MssqlSourceConfig = serde_json::from_value(serde_json::json!({
            "connection_string": CONNECTION_STRING,
            "tables": ["dbo.users"],
        }))
        .expect("config");

        assert_eq!(config.start_position, StartPosition::Earliest);
    }

    #[test]
    fn given_latest_start_position_should_deserialize() {
        let config: MssqlSourceConfig = serde_json::from_value(serde_json::json!({
            "connection_string": CONNECTION_STRING,
            "tables": ["dbo.users"],
            "start_position": "latest",
        }))
        .expect("config");

        assert_eq!(config.start_position, StartPosition::Latest);
    }

    #[test]
    fn given_config_debug_should_not_contain_password() {
        let source = MssqlSource::new(1, test_config(), None);

        assert!(!format!("{source:?}").contains("Secret_Passw0rd"));
    }

    // The parsed config holds tiberius::Config, whose Debug includes the login.
    #[test]
    fn given_validated_config_debug_should_not_contain_password() {
        let validated = validate_config(&test_config()).expect("valid");

        assert!(!format!("{validated:?}").contains("Secret_Passw0rd"));
    }

    #[test]
    fn given_persisted_state_should_restore_it() {
        let saved = saved_state();
        let connector_state = ConnectorState(rmp_serde::to_vec(&saved).expect("serialize"));

        let source = MssqlSource::new(1, test_config(), Some(connector_state));

        assert!(!source.state_unreadable);
        assert_eq!(*source.state.blocking_lock(), saved);
    }

    #[test]
    fn given_no_state_should_start_fresh() {
        let source = MssqlSource::new(1, test_config(), None);

        assert!(!source.state_unreadable);
        assert_eq!(*source.state.blocking_lock(), State::default());
    }

    // Starting fresh would lose the restore check and, with `latest`, skip changes.
    #[test]
    fn given_invalid_state_should_refuse_to_open() {
        let invalid = ConnectorState(b"not valid msgpack".to_vec());
        let mut source = MssqlSource::new(1, test_config(), Some(invalid));

        let result = block_on(source.open());

        assert!(
            matches!(&result, Err(Error::InitError(message)) if message.contains("cannot be decoded")),
            "{result:?}"
        );
    }

    #[test]
    fn given_state_with_appended_field_should_keep_saved_values() {
        #[derive(Debug, Deserialize)]
        struct NextState {
            database: Option<DatabaseIdentity>,
            checkpoint: Option<Position>,
            tables: BTreeMap<String, TableState>,
            #[serde(default)]
            added: Option<u32>,
        }
        let saved = saved_state();

        let next: NextState = rmp_serde::from_slice(&rmp_serde::to_vec(&saved).expect("serialize"))
            .expect("deserialize");

        assert_eq!(next.database, saved.database);
        assert_eq!(next.checkpoint, saved.checkpoint);
        assert_eq!(next.tables, saved.tables);
        assert_eq!(next.added, None);
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = saved_state();

        let bytes = rmp_serde::to_vec(&original).expect("serialize");
        let decoded: State = rmp_serde::from_slice(&bytes).expect("deserialize");

        assert_eq!(decoded, original);
    }

    // Saved checkpoints must keep decoding after an upgrade. Change these bytes only with a migration.
    #[test]
    fn given_state_bytes_from_this_version_should_decode() {
        const SAVED: &str = "9392a473686f70c41000000000000000000000000000000001929a000000000000000000059a0000000000000000000281a964626f2e757365727392a964626f5f75736572739a00000000000000000001";
        let bytes: Vec<u8> = (0..SAVED.len())
            .step_by(2)
            .map(|index| u8::from_str_radix(&SAVED[index..index + 2], 16).expect("hex"))
            .collect();

        let decoded: State = rmp_serde::from_slice(&bytes).expect("deserialize");

        assert_eq!(decoded, saved_state());
    }

    #[test]
    fn given_named_instance_without_port_should_reject_configuration() {
        let config = MssqlSourceConfig {
            connection_string: SecretString::from(
                "Server=tcp:sql01\\SALES;Database=shop;User Id=sa;Password=x",
            ),
            ..test_config()
        };

        assert!(rejected(config).contains("without a port"));
    }

    #[test]
    fn given_named_instance_with_port_should_accept_configuration() {
        let config = MssqlSourceConfig {
            connection_string: SecretString::from(
                "Data Source=sql01\\SALES,1433;Database=shop;User Id=sa;Password=x",
            ),
            ..test_config()
        };

        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn given_checkpoint_above_floor_should_resume_from_checkpoint() {
        assert_eq!(next_needed(Some(position(5, 2)), lsn(3)), position(5, 2));
    }

    #[test]
    fn given_floor_above_checkpoint_should_start_at_floor() {
        assert_eq!(next_needed(Some(position(5, 2)), lsn(8)), position(8, 0));
        assert_eq!(next_needed(None, lsn(8)), position(8, 0));
    }

    #[test]
    fn given_no_table_state_should_choose_newest_usable_instance() {
        let minimums = instances(&[("dbo_users_v2", 0), ("dbo_users", 3)]);

        assert_eq!(choose_instance(&minimums, None, None), Choice::Use(1));
    }

    #[test]
    fn given_recorded_instance_only_should_keep_it() {
        let minimums = instances(&[("dbo_users", 3)]);

        assert_eq!(
            choose_instance(&minimums, Some(lsn(5)), Some("dbo_users")),
            Choice::Use(0)
        );
    }

    #[test]
    fn given_newer_instance_at_or_below_next_needed_should_switch_to_it() {
        let minimums = instances(&[("dbo_users_v2", 5), ("dbo_users", 3)]);

        assert_eq!(
            choose_instance(&minimums, Some(lsn(5)), Some("dbo_users")),
            Choice::Use(0)
        );
    }

    #[test]
    fn given_newer_instance_above_next_needed_should_keep_recorded_instance() {
        let minimums = instances(&[("dbo_users_v2", 9), ("dbo_users", 3)]);

        assert_eq!(
            choose_instance(&minimums, Some(lsn(5)), Some("dbo_users")),
            Choice::Use(1)
        );
    }

    #[test]
    fn given_recorded_minimum_above_next_needed_should_report_gap() {
        let minimums = instances(&[("dbo_users", 7)]);

        assert_eq!(
            choose_instance(&minimums, Some(lsn(5)), Some("dbo_users")),
            Choice::Gap { min_lsn: lsn(7) }
        );
    }

    #[test]
    fn given_recorded_instance_gone_and_newer_above_should_have_no_usable_instance() {
        let minimums = instances(&[("dbo_users_v2", 9)]);

        assert_eq!(
            choose_instance(&minimums, Some(lsn(5)), Some("dbo_users")),
            Choice::NoUsable
        );
    }

    #[test]
    fn given_zero_minimum_should_skip_instance() {
        let minimums = instances(&[("dbo_users_v2", 0), ("dbo_users", 3)]);

        assert_eq!(
            choose_instance(&minimums, Some(lsn(5)), Some("dbo_users")),
            Choice::Use(1)
        );
    }

    #[test]
    fn given_minimum_above_next_needed_should_report_gap() {
        let error = check_minimum(&users_instance(), position(5, 2), lsn(6));

        assert!(matches!(error, Some(MssqlError::LsnGap { .. })));
    }

    #[test]
    fn given_new_table_with_floor_at_minimum_should_have_no_gap() {
        assert!(check_minimum(&users_instance(), position(3, 0), lsn(3)).is_none());
    }

    #[test]
    fn given_idle_watermark_above_minimum_should_have_no_gap() {
        let watermark = Position::new(lsn(9), Lsn::MAX);

        assert!(check_minimum(&users_instance(), watermark, lsn(4)).is_none());
    }

    #[test]
    fn given_zero_minimum_should_report_missing_instance() {
        let error = check_minimum(&users_instance(), position(5, 2), Lsn::ZERO);

        assert!(matches!(error, Some(MssqlError::InstanceMissing { .. })));
    }

    #[test]
    fn given_different_recovery_fork_should_report_database_changed() {
        let error = check_identity(&saved_state(), &identity(2), Some(lsn(9)));

        assert!(matches!(error, Err(MssqlError::DatabaseChanged { .. })));
    }

    #[test]
    fn given_checkpoint_above_max_lsn_should_report_database_changed() {
        let error = check_identity(&saved_state(), &identity(1), Some(lsn(4)));

        assert!(matches!(error, Err(MssqlError::DatabaseChanged { .. })));
    }

    #[test]
    fn given_renamed_database_with_same_fork_should_report_database_changed() {
        let renamed = DatabaseIdentity {
            name: "shop_archive".to_string(),
            recovery_fork_guid: Uuid::from_u128(1),
        };

        let error = check_identity(&saved_state(), &renamed, Some(lsn(9)));

        assert!(matches!(error, Err(MssqlError::DatabaseChanged { .. })));
    }

    #[test]
    fn given_stuck_change_before_first_ack_should_stage_open_changes() {
        let source = MssqlSource::new(1, test_config(), None);
        *source.state.blocking_lock() = saved_state();
        source.open_changes_pending.store(true, Ordering::SeqCst);

        let batch = block_on(source.stuck_batch()).expect("empty batch");

        assert!(batch.messages.is_empty());
        assert!(
            batch.state.is_some(),
            "the open() changes must be saved even while stuck"
        );
    }

    #[test]
    fn given_stuck_change_after_first_ack_should_stage_nothing() {
        let source = MssqlSource::new(1, test_config(), None);
        *source.state.blocking_lock() = saved_state();

        let batch = block_on(source.stuck_batch()).expect("empty batch");

        assert!(batch.state.is_none());
        assert!(matches!(
            *source.candidate.blocking_lock(),
            Some(Candidate::Batch { state: None, .. })
        ));
    }

    #[test]
    fn given_matching_identity_should_pass() {
        assert!(check_identity(&saved_state(), &identity(1), Some(lsn(5))).is_ok());
        assert!(check_identity(&State::default(), &identity(1), None).is_ok());
    }

    #[test]
    fn given_batch_policy_should_have_no_nack_limit() {
        let source = MssqlSource::new(1, test_config(), None);

        assert_eq!(source.batch_policy().max_consecutive_nacks(), None);
    }

    #[test]
    fn given_unchanged_state_should_stage_nothing() {
        let source = MssqlSource::new(1, test_config(), None);
        let state = saved_state();

        let candidate = source.batch_candidate(&state, state.clone(), 0, false);

        assert!(matches!(candidate, Candidate::Batch { state: None, .. }));
    }

    #[test]
    fn given_open_changes_pending_should_stage_unchanged_state() {
        let source = MssqlSource::new(1, test_config(), None);
        source.open_changes_pending.store(true, Ordering::SeqCst);
        let state = saved_state();

        let candidate = source.batch_candidate(&state, state.clone(), 0, false);

        assert!(
            matches!(candidate, Candidate::Batch { state: Some(staged), .. } if staged == saved_state())
        );
    }

    #[test]
    fn given_payload_at_iggy_limit_should_be_accepted() {
        let table = TableName::new("dbo", "orders");

        assert!(check_payload_size(&table, position(5, 1), MAX_PAYLOAD_SIZE as usize).is_ok());
    }

    #[test]
    fn given_payload_one_byte_over_iggy_limit_should_hold_progress() {
        let table = TableName::new("dbo", "orders");

        let error = check_payload_size(&table, position(5, 1), MAX_PAYLOAD_SIZE as usize + 1)
            .expect_err("over the limit");

        assert!(matches!(error, MssqlError::PayloadTooLarge { .. }));
        assert_eq!(error.stuck_position(), Some(position(5, 1)));
    }

    #[test]
    fn given_commit_time_should_set_origin_timestamp_in_microseconds() {
        let instances = [users_instance()];
        let commit_time =
            chrono::DateTime::parse_from_rfc3339("2026-10-20T22:42:52.530+00:00").expect("time");
        let rows = vec![encode::ChangeRow {
            position: position(5, 1),
            operation: encode::OPERATION_INSERT,
            update_mask: Vec::new(),
            commit_time: Some(commit_time),
            values: vec![serde_json::json!(1)],
            instance: 0,
        }];
        let events = to_events(rows, &instances, "shop").expect("events");

        let message = produced_message(&events[0]).expect("message");

        assert_eq!(message.origin_timestamp, Some(1_792_536_172_530_000));
    }

    #[test]
    fn given_latest_with_changes_should_start_after_the_newest_lsn() {
        assert_eq!(
            latest_checkpoint(Some(lsn(9))),
            Position::new(lsn(9), Lsn::MAX)
        );
    }

    // Leaving the checkpoint empty would apply `latest` again on the next start and skip changes.
    #[test]
    fn given_latest_with_no_changes_should_save_a_checkpoint_below_every_floor() {
        let checkpoint = latest_checkpoint(None);

        assert_eq!(next_needed(Some(checkpoint), lsn(3)), position(3, 0));
    }

    #[test]
    fn given_stuck_change_should_wait_before_reading_it_again() {
        let now = Instant::now();

        assert!(stuck_retry_due(None, now));
        assert!(!stuck_retry_due(Some(now + Duration::from_secs(30)), now));
        assert!(stuck_retry_due(Some(now), now));
    }

    #[test]
    fn given_new_stuck_position_should_log_at_error() {
        let now = Instant::now();

        assert!(should_log_stuck(None, position(5, 1), now));
        assert!(should_log_stuck(
            Some((position(4, 1), now)),
            position(5, 1),
            now
        ));
    }

    #[test]
    fn given_same_stuck_position_should_log_at_error_once_a_minute() {
        let now = Instant::now();
        let logged = Some((position(5, 1), now));

        assert!(!should_log_stuck(
            logged,
            position(5, 1),
            now + Duration::from_secs(30)
        ));
        assert!(should_log_stuck(
            logged,
            position(5, 1),
            now + STUCK_LOG_INTERVAL
        ));
    }

    #[test]
    fn given_ack_should_commit_candidate() {
        let source = MssqlSource::new(1, test_config(), None);
        source.open_changes_pending.store(true, Ordering::SeqCst);
        stage_batch(&source, false);

        block_on(source.on_batch_result(SourceBatchResult::Ack)).expect("ack");

        assert_eq!(*source.state.blocking_lock(), saved_state());
        assert!(!source.open_changes_pending.load(Ordering::SeqCst));
        assert_eq!(source.rows_emitted.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn given_nack_should_drop_candidate_and_keep_open_changes_pending() {
        let source = MssqlSource::new(1, test_config(), None);
        source.open_changes_pending.store(true, Ordering::SeqCst);
        stage_batch(&source, true);

        block_on(source.on_batch_result(SourceBatchResult::Nack)).expect("nack");

        assert_eq!(*source.state.blocking_lock(), State::default());
        assert!(source.open_changes_pending.load(Ordering::SeqCst));
        assert!(source.candidate.blocking_lock().is_none());
        assert_eq!(source.rows_emitted.load(Ordering::Relaxed), 0);
        assert!(!source.skip_next_sleep.load(Ordering::SeqCst));
    }

    #[test]
    fn given_ack_of_batch_left_behind_should_skip_next_sleep() {
        let source = MssqlSource::new(1, test_config(), None);
        stage_batch(&source, true);

        block_on(source.on_batch_result(SourceBatchResult::Ack)).expect("ack");

        assert!(source.skip_next_sleep.load(Ordering::SeqCst));
    }

    #[test]
    fn given_ack_of_caught_up_batch_should_sleep_before_next_poll() {
        let source = MssqlSource::new(1, test_config(), None);
        source.skip_next_sleep.store(true, Ordering::SeqCst);
        stage_batch(&source, false);

        block_on(source.on_batch_result(SourceBatchResult::Ack)).expect("ack");

        assert!(!source.skip_next_sleep.load(Ordering::SeqCst));
    }

    #[test]
    fn given_staged_stop_should_fail_batch_result_on_ack_and_nack() {
        for result in [SourceBatchResult::Ack, SourceBatchResult::Nack] {
            let source = MssqlSource::new(1, test_config(), None);
            block_on(source.stage_stop(MssqlError::LsnGap {
                table: "dbo.users".to_string(),
                next_needed: lsn(5),
                min_lsn: lsn(6),
            }));

            assert!(block_on(source.on_batch_result(result)).is_err());
        }
    }

    #[test]
    fn given_staged_stop_should_return_empty_json_batch_without_state() {
        let source = MssqlSource::new(1, test_config(), None);

        let batch = block_on(source.stage_stop(MssqlError::DatabaseChanged {
            reason: "restored".to_string(),
        }));

        assert!(batch.messages.is_empty());
        assert!(batch.state.is_none());
        assert_eq!(batch.schema, Schema::Json);
    }
}
