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

pub mod config;
pub mod driver;
pub mod message;

pub mod batch {
    use iggy_connector_sdk::{Error, ProducedMessage, ProducedMessages, Schema};
    use lapin::{Acker, options::BasicAckOptions};

    pub struct PendingBatch {
        schema: Schema,
        deliveries: Vec<(ProducedMessage, Acker)>,
        in_flight: bool,
    }

    impl PendingBatch {
        pub fn new(schema: Schema, deliveries: Vec<(ProducedMessage, Acker)>) -> Self {
            Self {
                schema,
                deliveries,
                in_flight: false,
            }
        }

        pub fn next_attempt(&mut self) -> Option<ProducedMessages> {
            if self.in_flight {
                return None;
            }
            self.in_flight = true;
            // Keep the originals and their ackers for replay if the runtime returns a NACK.
            Some(ProducedMessages {
                schema: self.schema,
                messages: self
                    .deliveries
                    .iter()
                    .map(|(message, _)| ProducedMessage {
                        id: message.id,
                        checksum: message.checksum,
                        timestamp: message.timestamp,
                        origin_timestamp: message.origin_timestamp,
                        headers: message.headers.clone(),
                        payload: message.payload.clone(),
                    })
                    .collect(),
                state: None,
            })
        }

        pub fn nack(&mut self) {
            self.in_flight = false;
        }

        pub async fn acknowledge(&self) -> Result<(), Error> {
            for (_, acker) in &self.deliveries {
                match acker.ack(BasicAckOptions::default()).await {
                    Ok(true) => {}
                    Ok(false) => {
                        return Err(Error::Connection(
                            "RabbitMQ acknowledgement was refused".into(),
                        ));
                    }
                    Err(_) => {
                        return Err(Error::Connection("RabbitMQ acknowledgement failed".into()));
                    }
                }
            }
            Ok(())
        }
    }
}

mod tls {
    use std::{io::Cursor, sync::Arc};

    use iggy_connector_sdk::Error;
    use lapin::tcp::{OwnedIdentity, OwnedTLSConfig};
    use rustls::{ClientConfig, RootCertStore};

    use crate::config::RabbitMqTlsConfig;

    pub fn build(config: Option<&RabbitMqTlsConfig>) -> Result<OwnedTLSConfig, Error> {
        let Some(config) = config else {
            return Ok(OwnedTLSConfig::default());
        };
        let mut tls = OwnedTLSConfig::default();
        if let Some(path) = &config.ca_file {
            let data = std::fs::read(path).map_err(|error| {
                Error::InvalidConfigValue(format!("cannot read tls.ca_file: {error}"))
            })?;
            let certificates = rustls_pemfile::certs(&mut Cursor::new(&data))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    Error::InvalidConfigValue(format!("invalid CA certificates: {error}"))
                })?;
            if certificates.is_empty() {
                return Err(Error::InvalidConfigValue(
                    "tls.ca_file contains no certificates".into(),
                ));
            }
            let mut roots = RootCertStore::empty();
            for certificate in certificates {
                roots.add(certificate).map_err(|error| {
                    Error::InvalidConfigValue(format!("invalid CA certificate: {error}"))
                })?;
            }
            tls.cert_chain = Some(
                String::from_utf8(data)
                    .map_err(|_| Error::InvalidConfigValue("CA file is not UTF-8 PEM".into()))?,
            );
        }
        if let (Some(cert_path), Some(key_path)) =
            (&config.client_cert_file, &config.client_key_file)
        {
            let pem = std::fs::read(cert_path).map_err(|error| {
                Error::InvalidConfigValue(format!("cannot read client certificate: {error}"))
            })?;
            let key = std::fs::read(key_path).map_err(|error| {
                Error::InvalidConfigValue(format!("cannot read client key: {error}"))
            })?;
            let certificates = rustls_pemfile::certs(&mut Cursor::new(&pem))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    Error::InvalidConfigValue(format!("invalid client certificate: {error}"))
                })?;
            if certificates.is_empty() {
                return Err(Error::InvalidConfigValue(
                    "client certificate file contains no certificates".into(),
                ));
            }
            let private_key = rustls_pemfile::private_key(&mut Cursor::new(&key))
                .map_err(|error| Error::InvalidConfigValue(format!("invalid client key: {error}")))?
                .ok_or_else(|| {
                    Error::InvalidConfigValue("client key file contains no private key".into())
                })?;
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|error| {
                    Error::InvalidConfigValue(format!("invalid TLS versions: {error}"))
                })?
                .with_root_certificates(RootCertStore::empty())
                .with_client_auth_cert(certificates, private_key)
                .map_err(|error| {
                    Error::InvalidConfigValue(format!(
                        "client certificate and key do not match: {error}"
                    ))
                })?;
            tls.identity = Some(OwnedIdentity::PKCS8 { pem, key });
        }
        Ok(tls)
    }

    #[cfg(test)]
    mod tests {
        use std::path::PathBuf;

        use super::build;
        use crate::config::RabbitMqTlsConfig;

        #[test]
        fn given_matching_certificates_should_build_tls_configuration() {
            let certs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../certs");
            let config = RabbitMqTlsConfig {
                ca_file: Some(
                    certs
                        .join("iggy_ca_cert.pem")
                        .to_string_lossy()
                        .into_owned(),
                ),
                client_cert_file: Some(certs.join("iggy_cert.pem").to_string_lossy().into_owned()),
                client_key_file: Some(certs.join("iggy_key.pem").to_string_lossy().into_owned()),
            };
            let tls = build(Some(&config)).expect("matching certificate and key must load");
            assert!(tls.cert_chain.is_some());
            assert!(tls.identity.is_some());
        }

        #[test]
        fn given_non_pem_ca_should_reject_tls_configuration() {
            let config = RabbitMqTlsConfig {
                ca_file: Some(
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("Cargo.toml")
                        .to_string_lossy()
                        .into_owned(),
                ),
                client_cert_file: None,
                client_key_file: None,
            };
            assert!(build(Some(&config)).is_err());
        }

        #[test]
        fn given_mismatched_certificate_and_key_should_reject_tls_configuration() {
            let certs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../certs");
            let config = RabbitMqTlsConfig {
                ca_file: None,
                client_cert_file: Some(
                    certs
                        .join("iggy_ca_cert.pem")
                        .to_string_lossy()
                        .into_owned(),
                ),
                client_key_file: Some(certs.join("iggy_key.pem").to_string_lossy().into_owned()),
            };
            assert!(build(Some(&config)).is_err());
        }
    }
}

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessages, Schema, Source, source::SourceBatchResult,
    source_connector,
};
use lapin::{
    Connection, ConnectionProperties,
    options::{BasicAckOptions, BasicRejectOptions},
    tcp::OwnedTLSConfig,
};
use secrecy::ExposeSecret;
use tokio::{
    sync::Mutex,
    time::{Instant, sleep, timeout},
};
use tracing::{debug, info, warn};

use crate::{
    batch::PendingBatch, config::RabbitMqSourceConfig, driver::RabbitMqDriver,
    message::prepare_delivery,
};

source_connector!(RabbitMqSource);

pub struct RabbitMqSource {
    id: u32,
    config: RabbitMqSourceConfig,
    batch_timeout: Duration,
    tls: OwnedTLSConfig,
    driver: Mutex<Option<RabbitMqDriver>>,
    pending: Mutex<Option<PendingBatch>>,
    deferred_failure: Mutex<Option<Failure>>,
    retry_attempts: AtomicU32,
    last_warning: Mutex<BTreeMap<(&'static str, Option<String>), Instant>>,
}

struct Failure {
    operation: &'static str,
    reply_code: Option<String>,
    error: Error,
}

impl Failure {
    fn simple(operation: &'static str, message: &str) -> Self {
        Self {
            operation,
            reply_code: None,
            error: Error::Connection(message.into()),
        }
    }

    fn lapin(operation: &'static str, error: lapin::Error) -> Self {
        // Lapin errors can contain connection details or credentials, so retain only safe details.
        let (reply_code, detail) = match error.kind() {
            lapin::ErrorKind::ProtocolError(error) => (
                Some(error.get_id().to_string()),
                format!("{} (reply code {})", error.kind(), error.get_id()),
            ),
            lapin::ErrorKind::IOError(error) => (None, format!("I/O error: {:?}", error.kind())),
            _ => (None, "client error".to_string()),
        };
        Self {
            operation,
            reply_code,
            error: Error::Connection(format!("RabbitMQ {operation} failed: {detail}")),
        }
    }
}

impl std::fmt::Debug for RabbitMqSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RabbitMqSource")
            .field("id", &self.id)
            .field("queue", &self.config.queue)
            .finish_non_exhaustive()
    }
}

impl RabbitMqSource {
    pub fn new(id: u32, config: RabbitMqSourceConfig, _state: Option<ConnectorState>) -> Self {
        Self {
            id,
            config,
            batch_timeout: Duration::ZERO,
            tls: OwnedTLSConfig::default(),
            driver: Mutex::new(None),
            pending: Mutex::new(None),
            deferred_failure: Mutex::new(None),
            retry_attempts: AtomicU32::new(0),
            last_warning: Mutex::new(BTreeMap::new()),
        }
    }

    fn schema(&self) -> Schema {
        if self.config.include_metadata {
            Schema::Json
        } else {
            Schema::Raw
        }
    }

    async fn connect(&self) -> Result<RabbitMqDriver, Failure> {
        let duration = Duration::from_secs(self.config.timeout_secs);
        let runtime =
            lapin::runtime::default_runtime().map_err(|error| Failure::lapin("runtime", error))?;
        let connection = timeout(
            duration,
            Connection::connect_with_config(
                self.config.amqp_url.expose_secret(),
                ConnectionProperties::default(),
                self.tls.clone(),
                runtime,
            ),
        )
        .await
        .map_err(|_| Failure::simple("connect", "RabbitMQ connection timed out"))?
        .map_err(|error| Failure::lapin("connect", error))?;
        timeout(
            duration,
            RabbitMqDriver::consume(connection, &self.config.queue, self.config.batch_size),
        )
        .await
        .map_err(|_| Failure::simple("consumer_setup", "RabbitMQ consumer setup timed out"))?
        .map_err(|error| Failure::lapin(error.operation, error.error))
    }

    async fn wait_before_retry(&self, failure: &Failure) {
        let attempts = self.retry_attempts.fetch_add(1, Ordering::Relaxed).min(3);
        let maximum_ms = (1000u64 << attempts).min(5000);
        let delay_ms = 1000 + rand::random::<u64>() % (maximum_ms - 999);
        let mut last = self.last_warning.lock().await;
        let key = (failure.operation, failure.reply_code.clone());
        if last
            .get(&key)
            .is_none_or(|instant| instant.elapsed() >= Duration::from_secs(60))
        {
            warn!(connector_id = self.id, queue = %self.config.queue, operation = failure.operation, reply_code = failure.reply_code.as_deref().unwrap_or("none"), error = %failure.error, "RabbitMQ source operation failed");
            last.insert(key, Instant::now());
        }
        drop(last);
        sleep(Duration::from_millis(delay_ms)).await;
    }

    async fn close_driver(&self, driver: RabbitMqDriver) {
        match timeout(
            Duration::from_secs(self.config.timeout_secs),
            driver.close(),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                warn!(connector_id = self.id, queue = %self.config.queue, "Failed to close RabbitMQ connection")
            }
            Err(_) => {
                warn!(connector_id = self.id, queue = %self.config.queue, "RabbitMQ connection close timed out")
            }
        }
    }

    fn empty_batch(&self) -> ProducedMessages {
        ProducedMessages {
            schema: self.schema(),
            messages: Vec::new(),
            state: None,
        }
    }
}

#[async_trait]
impl Source for RabbitMqSource {
    async fn open(&mut self) -> Result<(), Error> {
        self.batch_timeout = self.config.validate()?;
        self.tls = tls::build(self.config.tls.as_ref())?;
        let driver = self.connect().await.map_err(|failure| failure.error)?;
        *self.driver.get_mut() = Some(driver);
        info!(connector_id = self.id, queue = %self.config.queue, "Opened RabbitMQ source");
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        if let Some(batch) = self.pending.lock().await.as_mut() {
            return batch.next_attempt().ok_or(Error::InvalidState);
        }
        let current_driver = self.driver.lock().await.take();
        let mut driver = match current_driver {
            Some(driver) => driver,
            None => match self.connect().await {
                Ok(driver) => {
                    self.retry_attempts.store(0, Ordering::Relaxed);
                    info!(connector_id = self.id, queue = %self.config.queue, "RabbitMQ source recovered");
                    driver
                }
                Err(failure) => {
                    self.wait_before_retry(&failure).await;
                    return Err(failure.error);
                }
            },
        };
        let mut deliveries = Vec::with_capacity(usize::from(self.config.batch_size));
        let mut deadline = None;
        loop {
            let wait = deadline.map_or(Duration::from_secs(1), |deadline: Instant| {
                deadline.saturating_duration_since(Instant::now())
            });
            if wait.is_zero() {
                break;
            }
            let mut delivery = match timeout(wait, driver.next()).await {
                Err(_) => break,
                Ok(Some(Ok(delivery))) => delivery,
                Ok(Some(Err(error))) => {
                    let failure = Failure::lapin("consume", error);
                    self.close_driver(driver).await;
                    self.wait_before_retry(&failure).await;
                    return Err(failure.error);
                }
                Ok(None) => {
                    let failure = Failure::simple("consume", "RabbitMQ consumer cancelled");
                    self.close_driver(driver).await;
                    self.wait_before_retry(&failure).await;
                    return Err(failure.error);
                }
            };
            let received_at = Instant::now();
            let message = match prepare_delivery(
                &mut delivery,
                self.config.include_metadata,
                self.id,
            ) {
                Ok(message) => message,
                Err(error) => {
                    warn!(connector_id = self.id, queue = %self.config.queue, %error, "Rejecting RabbitMQ delivery after preparation failed");
                    let result = timeout(
                        Duration::from_secs(self.config.timeout_secs),
                        delivery.acker.reject(BasicRejectOptions { requeue: false }),
                    )
                    .await;
                    let failure = match result {
                        Ok(Ok(true)) => continue,
                        Ok(Err(error)) => Failure::lapin("poison_reject", error),
                        _ => Failure::simple("poison_reject", "RabbitMQ poison rejection failed"),
                    };
                    if !deliveries.is_empty() {
                        // The valid deliveries need their original channel until they are acknowledged.
                        *self.deferred_failure.lock().await = Some(failure);
                        break;
                    }
                    self.close_driver(driver).await;
                    self.wait_before_retry(&failure).await;
                    return Err(failure.error);
                }
            };
            if let Some(message) = message {
                deliveries.push((message, delivery.acker));
                deadline.get_or_insert(received_at + self.batch_timeout);
                if deliveries.len() >= usize::from(self.config.batch_size) {
                    break;
                }
            } else {
                warn!(connector_id = self.id, queue = %self.config.queue, "Dropping unsupported RabbitMQ delivery");
                // Unsupported payloads cannot be forwarded, so redelivery would repeat the failure.
                match timeout(
                    Duration::from_secs(self.config.timeout_secs),
                    delivery.acker.ack(BasicAckOptions::default()),
                )
                .await
                {
                    Ok(Ok(true)) => {}
                    result => {
                        let failure = match result {
                            Ok(Err(error)) => Failure::lapin("poison_ack", error),
                            _ => Failure::simple(
                                "poison_ack",
                                "RabbitMQ poison acknowledgement failed",
                            ),
                        };
                        self.close_driver(driver).await;
                        self.wait_before_retry(&failure).await;
                        return Err(failure.error);
                    }
                }
            }
        }
        *self.driver.lock().await = Some(driver);
        if deliveries.is_empty() {
            return Ok(self.empty_batch());
        }
        if self.config.verbose_logging {
            info!(connector_id = self.id, queue = %self.config.queue, batch_size = deliveries.len(), "Prepared RabbitMQ source batch");
        } else {
            debug!(connector_id = self.id, queue = %self.config.queue, batch_size = deliveries.len(), "Prepared RabbitMQ source batch");
        }
        let mut batch = PendingBatch::new(self.schema(), deliveries);
        let messages = batch.next_attempt().ok_or(Error::InvalidState)?;
        *self.pending.lock().await = Some(batch);
        Ok(messages)
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let mut pending = self.pending.lock().await;
        if result == SourceBatchResult::Nack {
            if let Some(batch) = pending.as_mut() {
                batch.nack();
            }
            return Ok(());
        }
        let Some(batch) = pending.take() else {
            return Ok(());
        };
        drop(pending);
        let result = timeout(
            Duration::from_secs(self.config.timeout_secs),
            batch.acknowledge(),
        )
        .await
        .unwrap_or_else(|_| {
            Err(Error::Connection(
                "RabbitMQ acknowledgement timed out".into(),
            ))
        });
        if result.is_err() {
            /* An acker is single-use and the broker outcome is uncertain. Closing the
             * connection lets RabbitMQ requeue unsettled deliveries, possibly causing duplicates. */
            let driver = self.driver.lock().await.take();
            if let Some(driver) = driver {
                self.close_driver(driver).await;
            }
        } else {
            let failure = self.deferred_failure.lock().await.take();
            if let Some(failure) = failure {
                let driver = self.driver.lock().await.take();
                if let Some(driver) = driver {
                    self.close_driver(driver).await;
                }
                self.wait_before_retry(&failure).await;
            }
        }
        result
    }

    async fn close(&mut self) -> Result<(), Error> {
        self.pending.get_mut().take();
        self.deferred_failure.get_mut().take();
        if let Some(driver) = self.driver.get_mut().take() {
            self.close_driver(driver).await;
        }
        Ok(())
    }
}

#[cfg(test)]
mod source_tests {
    use super::{Failure, RabbitMqSource};
    use crate::{batch::PendingBatch, config::RabbitMqSourceConfig};
    use iggy_connector_sdk::{ProducedMessage, Schema, Source, source::SourceBatchResult};
    use lapin::message::Delivery;

    fn source() -> RabbitMqSource {
        let config: RabbitMqSourceConfig =
            serde_json::from_str(r#"{"amqp_url":"amqp://localhost","queue":"events"}"#)
                .expect("configuration must deserialize");
        RabbitMqSource::new(1, config, None)
    }

    fn message(payload: Vec<u8>) -> ProducedMessage {
        ProducedMessage {
            id: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload,
        }
    }

    #[test]
    fn given_lapin_error_with_url_secrets_should_not_expose_error_details() {
        let secret_url = "amqp://user:password@broker.example/private?token=secret";
        let error = std::io::Error::other(secret_url);
        let failure = Failure::lapin("consumer_setup", error.into());

        assert_eq!(failure.operation, "consumer_setup");
        let message = failure.error.to_string();
        assert!(message.contains("consumer_setup"));
        for secret in [
            "user",
            "password",
            "broker.example",
            "private",
            "token",
            "secret",
        ] {
            assert!(!message.contains(secret));
        }
    }

    #[test]
    fn given_broker_protocol_error_should_preserve_safe_details() {
        for (code, description) in [(403, "ACCESS-REFUSED"), (404, "NOT-FOUND")] {
            let broker_error = lapin::protocol::AMQPError::from_id(
                code,
                "amqp://user:password@broker?token=secret".into(),
            )
            .unwrap();
            let error = lapin::ErrorKind::ProtocolError(broker_error).into();
            let failure = Failure::lapin("basic_consume", error);
            let message = failure.error.to_string();

            assert_eq!(failure.reply_code, Some(code.to_string()));
            assert!(message.contains(&code.to_string()));
            assert!(message.to_uppercase().contains(description));
            assert!(!message.contains("password"));
            assert!(!message.contains("secret"));
        }
    }

    #[test]
    fn given_invalid_url_with_credentials_should_not_expose_credentials() {
        let config: RabbitMqSourceConfig = serde_json::from_str(
            r#"{"amqp_url":"amqp://user:password@[invalid/private?token=secret","queue":"events"}"#,
        )
        .expect("configuration must deserialize");
        let error = config
            .validate()
            .expect_err("URL must be invalid")
            .to_string();
        for secret in ["user", "password", "private", "token", "secret"] {
            assert!(!error.contains(secret));
        }
    }

    #[test]
    fn given_pending_batch_when_nacked_should_make_it_available_for_replay() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let source = source();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            let mut batch =
                PendingBatch::new(Schema::Raw, vec![(message(delivery.data), delivery.acker)]);
            let first = batch.next_attempt().expect("first batch must exist");
            assert!(first.state.is_none());
            *source.pending.lock().await = Some(batch);
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("Nack must succeed");
            let mut pending = source.pending.lock().await;
            let replay = pending
                .as_mut()
                .expect("batch must remain pending")
                .next_attempt()
                .expect("batch must be replayable");
            assert_eq!(replay.messages[0].payload, first.messages[0].payload);
        });
    }

    #[test]
    fn given_supplied_state_should_start_without_persisted_state() {
        let config: RabbitMqSourceConfig =
            serde_json::from_str(r#"{"amqp_url":"amqp://localhost","queue":"events"}"#)
                .expect("configuration must deserialize");
        let source = RabbitMqSource::new(
            1,
            config,
            Some(iggy_connector_sdk::ConnectorState(b"unused".to_vec())),
        );
        assert!(source.empty_batch().state.is_none());
    }

    #[test]
    fn given_pending_batch_when_closed_twice_should_not_acknowledge() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let mut source = source();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            let acker = delivery.acker.clone();
            *source.pending.lock().await = Some(PendingBatch::new(
                Schema::Raw,
                vec![(message(delivery.data), delivery.acker)],
            ));
            source.close().await.expect("first close must succeed");
            source.close().await.expect("second close must succeed");
            assert!(source.pending.lock().await.is_none());
            assert!(
                acker
                    .ack(Default::default())
                    .await
                    .expect("acknowledgement must succeed")
            );
        });
    }

    #[test]
    fn given_used_acknowledgement_when_batch_is_accepted_should_stop_with_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let source = source();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            assert!(
                delivery
                    .acker
                    .ack(Default::default())
                    .await
                    .expect("first acknowledgement must succeed")
            );
            *source.pending.lock().await = Some(PendingBatch::new(
                Schema::Raw,
                vec![(message(delivery.data), delivery.acker)],
            ));
            assert!(
                source
                    .on_batch_result(SourceBatchResult::Ack)
                    .await
                    .is_err()
            );
            assert!(source.pending.lock().await.is_none());
        });
    }

    #[test]
    fn given_pending_delivery_when_nacked_then_acked_should_acknowledge_once() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let source = source();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            let acker = delivery.acker.clone();
            let mut batch =
                PendingBatch::new(Schema::Raw, vec![(message(delivery.data), delivery.acker)]);
            batch.next_attempt().expect("batch must be available");
            *source.pending.lock().await = Some(batch);
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("Nack must succeed");
            assert!(acker.usable());
            source
                .pending
                .lock()
                .await
                .as_mut()
                .expect("batch must remain pending")
                .next_attempt()
                .expect("batch must be replayable");
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("Ack must succeed");
            assert!(!acker.usable());
            assert!(source.pending.lock().await.is_none());
        });
    }

    #[test]
    fn given_deferred_preparation_failure_when_batch_is_retried_should_wait_for_ack() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let source = source();
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            let acker = delivery.acker.clone();
            let mut batch =
                PendingBatch::new(Schema::Raw, vec![(message(delivery.data), delivery.acker)]);
            batch.next_attempt().expect("batch must be available");
            *source.pending.lock().await = Some(batch);
            *source.deferred_failure.lock().await = Some(Failure::simple("prepare", "failed"));

            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("Nack must succeed");
            assert!(source.deferred_failure.lock().await.is_some());
            assert!(acker.usable());
            source
                .pending
                .lock()
                .await
                .as_mut()
                .expect("batch must remain pending")
                .next_attempt()
                .expect("batch must be replayable");
            source
                .on_batch_result(SourceBatchResult::Ack)
                .await
                .expect("Ack must succeed");
            assert!(!acker.usable());
            assert!(source.deferred_failure.lock().await.is_none());
        });
    }
}

#[cfg(test)]
mod tests {
    use super::batch::PendingBatch;
    use super::config::RabbitMqSourceConfig;
    use iggy_connector_sdk::{ProducedMessage, Schema};
    use lapin::message::Delivery;

    #[test]
    fn given_minimal_configuration_should_apply_defaults() {
        let config: RabbitMqSourceConfig = serde_json::from_str(
            r#"{"amqp_url":"amqp://guest:guest@localhost:5672","queue":"events"}"#,
        )
        .expect("configuration must deserialize");
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.batch_timeout, "10ms");
        assert!(!config.include_metadata);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(
            config.validate().unwrap(),
            std::time::Duration::from_millis(10)
        );
    }

    #[test]
    fn given_zero_batch_size_should_reject_configuration() {
        let config: RabbitMqSourceConfig = serde_json::from_str(
            r#"{"amqp_url":"amqp://localhost","queue":"events","batch_size":0}"#,
        )
        .expect("configuration must deserialize");
        assert!(config.validate().is_err());
    }

    #[test]
    fn given_invalid_configuration_should_reject_it() {
        let cases = [
            serde_json::json!({"amqp_url":"http://localhost","queue":"events"}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":""}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"events","batch_timeout":"0s"}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"events","timeout_secs":0}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"events","timeout_secs":u64::MAX}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"events","tls":{}}),
            serde_json::json!({"amqp_url":"amqps://localhost","queue":"events","tls":{"client_cert_file":"cert.pem"}}),
            serde_json::json!({"amqp_url":"amqp://localhost","queue":"x".repeat(256)}),
        ];
        for value in cases {
            let config: RabbitMqSourceConfig =
                serde_json::from_value(value).expect("configuration must deserialize");
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn given_nack_should_replay_the_same_pending_delivery() {
        let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1, 2]);
        let message = ProducedMessage {
            id: Some(42),
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: None,
            payload: delivery.data.clone(),
        };
        let mut batch = PendingBatch::new(Schema::Raw, vec![(message, delivery.acker)]);
        let first = batch.next_attempt().expect("first batch must be available");
        assert!(batch.next_attempt().is_none());
        batch.nack();
        let replay = batch.next_attempt().expect("Nack must allow replay");
        assert_eq!(replay.schema, first.schema);
        assert_eq!(replay.messages[0].id, first.messages[0].id);
        assert_eq!(replay.messages[0].payload, first.messages[0].payload);
    }

    #[test]
    fn given_used_acknowledgement_when_acknowledged_should_return_error() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime must start");
        runtime.block_on(async {
            let delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![1]);
            assert!(
                delivery
                    .acker
                    .ack(Default::default())
                    .await
                    .expect("first acknowledgement must succeed")
            );
            let message = ProducedMessage {
                id: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                headers: None,
                payload: delivery.data,
            };
            let batch = PendingBatch::new(Schema::Raw, vec![(message, delivery.acker)]);
            assert!(batch.acknowledge().await.is_err());
        });
    }
}
