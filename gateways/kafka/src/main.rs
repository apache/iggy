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

use std::fmt::Display;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use tokio::signal;
use tokio::sync::{Semaphore, broadcast};
use tracing::{info, warn};

use iggy_gateway_kafka::auth::IggyAuthenticator;
use iggy_gateway_kafka::bridge::{
    DEFAULT_TOPIC_PARTITIONS, IggyBridge, IggyBridgeConfig, IggyTls, MAX_TOPIC_PARTITIONS,
};
use iggy_gateway_kafka::env::parse_bool;
use iggy_gateway_kafka::listener::{
    ListenerConfig, SecurityProtocol, apply_advertised_listeners, parse_listeners,
};
use iggy_gateway_kafka::protocol::auto_topic_creation::AutoTopicCreationConfig;
use iggy_gateway_kafka::server::{bind_listeners, init_tracing};
use iggy_gateway_kafka::tls::{ClientAuth, TlsConfig};
use iggy_gateway_kafka::{GatewayConfig, KafkaGateway};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Held for the lifetime of main: dropping it stops the non-blocking log writer's worker
    // thread, and any buffered-but-unflushed log lines are lost. Binding to `_` instead of a
    // named variable would drop it immediately.
    let _tracing_guard = init_tracing();

    let config = load_config()?;
    let sasl_enabled = config.sasl_enabled();
    let plaintext_sasl = config
        .listeners
        .iter()
        .any(|listener| listener.protocol == SecurityProtocol::SaslPlaintext);
    let bridge = connect_bridge(sasl_enabled).await?;
    // Loaded before the listeners bind, so no client waits in the backlog behind Iggy.
    let cluster_id = match &bridge {
        Some(bridge) => Some(bridge.load_cluster_id().await.map_err(|e| e.to_string())?),
        None => None,
    };

    let listeners = bind_listeners(&config).map_err(|e| e.to_string())?;
    let mut server = KafkaGateway::new(config).with_bridge(bridge.clone());
    if let Some(cluster_id) = cluster_id {
        server = server.with_cluster_id(cluster_id);
    }
    if sasl_enabled {
        let authenticator = IggyAuthenticator::from_env()?;
        if plaintext_sasl {
            // This is the hop the Kafka client's password actually crosses. The warning is tied
            // to the listener that exposes it, not to the Iggy hop, so encrypting the Iggy side
            // does not silence the only notice about this one.
            warn!(
                "SASL/PLAIN on the SASL_PLAINTEXT listener sends the password in the clear; use \
                 a SASL_SSL listener outside a trusted network"
            );
        }
        if !authenticator.is_tls_enabled() {
            warn!(
                "the credential check's link to Iggy is unencrypted; set \
                 IGGY_KAFKA_IGGY_TLS_ENABLED=true to encrypt it"
            );
        }
        info!("SASL/PLAIN enabled; credentials verified against {authenticator}");
        server = server.with_authenticator(Arc::new(authenticator));
    }

    let (tx, rx) = broadcast::channel(1);
    let mut server_task = tokio::spawn(async move { server.run(listeners, rx).await });

    let stopped_alone = tokio::select! {
        result = &mut server_task => Some(result),
        () = shutdown_signal() => None,
    };
    let outcome = if let Some(outcome) = stopped_alone {
        outcome
    } else {
        let _ = tx.send(());
        server_task.await
    };
    close_bridge(bridge.as_deref()).await;
    Ok(outcome??)
}

/// How long shutdown waits for the Iggy clients to sign out. The connection drain before it can
/// take 25 s of a 30 s grace period.
const BRIDGE_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

/// Signs the bridge's Iggy clients out and shuts them down. Without it Iggy keeps each client's
/// session until its lease runs out.
async fn close_bridge(bridge: Option<&IggyBridge>) {
    let Some(bridge) = bridge else {
        return;
    };
    match tokio::time::timeout(BRIDGE_CLOSE_TIMEOUT, bridge.close()).await {
        Ok(Ok(())) => info!("Iggy bridge closed"),
        Ok(Err(error)) => warn!("the Iggy bridge did not close cleanly: {error}"),
        Err(_elapsed) => warn!(
            "the Iggy bridge did not close within {BRIDGE_CLOSE_TIMEOUT:?}: Iggy ends the \
             remaining sessions when their leases run out"
        ),
    }
}

/// The `IGGY_KAFKA_*` vars `load_config` itself reads. `IGGY_KAFKA_` is a
/// `SERVER_ALLOWED_ENV_PREFIXES` entry in `core/configs`, which trades away the server's
/// unknown-variable check for this whole namespace - a misspelled key here would otherwise
/// silently no-op instead of surfacing anywhere. `reject_unknown_kafka_env_vars` is this crate's
/// own replacement for that lost check.
///
/// Deliberately excludes `IggyBridgeConfig::KNOWN_ENV_VARS`: `reject_unknown_kafka_env_vars`
/// checks both lists rather than one merged copy, so a bridge var rename can't silently desync
/// this array from the one `bridge::config` actually reads.
const KNOWN_KAFKA_ENV_VARS: &[&str] = &[
    "IGGY_KAFKA_BIND_ADDR",
    "IGGY_KAFKA_ADVERTISED_HOST",
    "IGGY_KAFKA_ADVERTISED_PORT",
    "IGGY_KAFKA_LISTENERS",
    "IGGY_KAFKA_ADVERTISED_LISTENERS",
    "IGGY_KAFKA_TLS_CERT_FILE",
    "IGGY_KAFKA_TLS_KEY_FILE",
    "IGGY_KAFKA_TLS_CLIENT_AUTH",
    "IGGY_KAFKA_TLS_CLIENT_CA_FILE",
    "IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES",
    "IGGY_KAFKA_TLS_RELOAD_INTERVAL_SECS",
    "IGGY_KAFKA_MAX_CONNECTIONS",
    "IGGY_KAFKA_MAX_FRAME_SIZE",
    "IGGY_KAFKA_IDLE_TIMEOUT_SECS",
    "IGGY_KAFKA_READ_TIMEOUT_SECS",
    "IGGY_KAFKA_WRITE_TIMEOUT_SECS",
    "IGGY_KAFKA_SHUTDOWN_DRAIN_TIMEOUT_SECS",
    "IGGY_KAFKA_BRIDGE_ENABLED",
    "IGGY_KAFKA_INSTANCE_ID",
    "IGGY_KAFKA_SASL_ENABLED",
    "IGGY_KAFKA_PRE_AUTH_TIMEOUT_SECS",
    "IGGY_KAFKA_MAX_CONCURRENT_AUTHENTICATIONS",
    "IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED",
    "IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS",
];

/// The variables of the single-listener form, which `IGGY_KAFKA_LISTENERS` replaces as a whole.
const SINGLE_LISTENER_ENV_VARS: &[&str] = &[
    "IGGY_KAFKA_BIND_ADDR",
    "IGGY_KAFKA_ADVERTISED_HOST",
    "IGGY_KAFKA_ADVERTISED_PORT",
    "IGGY_KAFKA_SASL_ENABLED",
];

/// The variables only a TLS listener reads.
const TLS_ENV_VARS: &[&str] = &[
    "IGGY_KAFKA_TLS_CERT_FILE",
    "IGGY_KAFKA_TLS_KEY_FILE",
    "IGGY_KAFKA_TLS_CLIENT_AUTH",
    "IGGY_KAFKA_TLS_CLIENT_CA_FILE",
    "IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES",
    "IGGY_KAFKA_TLS_RELOAD_INTERVAL_SECS",
];

/// Rejects any `IGGY_KAFKA_*` env var not in [`KNOWN_KAFKA_ENV_VARS`] or
/// [`IggyBridgeConfig::KNOWN_ENV_VARS`] - a typo (e.g. `IGGY_KAFKA_BIN_ADDR`) would otherwise be
/// silently ignored: the server's check in `core/configs` skips the whole `IGGY_KAFKA_` prefix, and
/// this crate's own `env_var()` only ever looks up exact known names, so nothing reads the
/// misspelled var and nothing warns either. Checking both lists is deliberate: a user who
/// exports a bridge var while `IGGY_KAFKA_BRIDGE_ENABLED` is off must not see a spurious
/// "unknown env var" rejection for a name this crate already recognizes.
fn reject_unknown_kafka_env_vars() -> Result<(), String> {
    for (key, _) in std::env::vars() {
        if key.starts_with("IGGY_KAFKA_")
            && !KNOWN_KAFKA_ENV_VARS.contains(&key.as_str())
            && !IggyBridgeConfig::KNOWN_ENV_VARS.contains(&key.as_str())
            && !IggyAuthenticator::KNOWN_ENV_VARS.contains(&key.as_str())
        {
            return Err(format!(
                "unknown Kafka gateway env var '{key}' (not in the recognized IGGY_KAFKA_* set - \
                 check for a typo; it would otherwise be silently ignored)"
            ));
        }
    }
    Ok(())
}

/// Connects the Iggy bridge when `IGGY_KAFKA_BRIDGE_ENABLED` is true, otherwise returns `None`
/// and every handler keeps answering with its stub.
///
/// Off by default, so wiring one API at a time does not change what an operator sees. A failed
/// connect is fatal: an operator who turned the bridge on must learn it is unreachable at
/// startup, not from `NOT_LEADER_OR_FOLLOWER` on every produce.
async fn connect_bridge(sasl_enabled: bool) -> Result<Option<Arc<IggyBridge>>, String> {
    if !bridge_enabled()? {
        let set: Vec<&str> = IggyBridgeConfig::KNOWN_ENV_VARS
            .iter()
            .copied()
            // With SASL on, the address and the TLS switches are the bridge variables that are
            // genuinely read, since they name and secure the server credentials are verified
            // against. Warning that nothing reads them would be false, and suppressing the whole
            // list instead would hide a set credential that really does go nowhere.
            .filter(|var| {
                !(sasl_enabled
                    && (*var == "IGGY_KAFKA_IGGY_ADDR" || IggyTls::KNOWN_ENV_VARS.contains(var)))
            })
            .filter(|var| std::env::var(var).is_ok())
            .collect();
        if !set.is_empty() {
            warn!(
                "{} set but IGGY_KAFKA_BRIDGE_ENABLED is not true: the gateway is answering with \
                 stub responses and reads none of them",
                set.join(", ")
            );
        }
        return Ok(None);
    }
    let config = IggyBridgeConfig::from_env().map_err(|e| format!("invalid bridge config: {e}"))?;
    if !config.tls.enabled {
        warn!(
            "the bridge's connection to Iggy is unencrypted and carries IGGY_KAFKA_IGGY_PASSWORD \
             in the clear; set IGGY_KAFKA_IGGY_TLS_ENABLED=true to encrypt it"
        );
    }
    let bridge = IggyBridge::connect(config)
        .await
        .map_err(|e| format!("failed to connect the Iggy bridge: {e}"))?;
    info!("Iggy bridge connected");
    Ok(Some(Arc::new(bridge)))
}

/// `IGGY_KAFKA_BRIDGE_ENABLED`, rejecting anything that is neither `true` nor `false`.
///
/// A typo otherwise reads as "off", which leaves stub responses and nothing in the log.
fn bridge_enabled() -> Result<bool, String> {
    env_var("IGGY_KAFKA_BRIDGE_ENABLED").map_or(Ok(false), |raw| {
        raw.parse()
            .map_err(|e| format!("invalid IGGY_KAFKA_BRIDGE_ENABLED `{raw}`: {e}"))
    })
}

/// Build [`GatewayConfig`] from `IGGY_KAFKA_*` env vars, rejecting values that would silently
/// break the listener (a zero connection cap serves nothing, a zero timeout drops every
/// connection, a semaphore cap above `Semaphore::MAX_PERMITS` panics at startup).
fn load_config() -> Result<GatewayConfig, String> {
    reject_unknown_kafka_env_vars()?;
    let mut config = GatewayConfig::default();

    config.listeners = load_listeners()?;
    config.tls = load_tls(&config.listeners)?;
    if let Some(raw) = env_var("IGGY_KAFKA_MAX_CONNECTIONS") {
        let max_connections: usize = parse_positive("IGGY_KAFKA_MAX_CONNECTIONS", &raw)?;
        if max_connections > Semaphore::MAX_PERMITS {
            return Err(format!(
                "IGGY_KAFKA_MAX_CONNECTIONS {max_connections} exceeds maximum {}",
                Semaphore::MAX_PERMITS
            ));
        }
        config.max_connections = max_connections;
    }
    if let Some(raw) = env_var("IGGY_KAFKA_MAX_FRAME_SIZE") {
        config.max_frame_size = parse_positive("IGGY_KAFKA_MAX_FRAME_SIZE", &raw)?;
    }
    if let Some(raw) = env_var("IGGY_KAFKA_IDLE_TIMEOUT_SECS") {
        config.idle_timeout =
            Duration::from_secs(parse_positive("IGGY_KAFKA_IDLE_TIMEOUT_SECS", &raw)?);
    }
    if let Some(raw) = env_var("IGGY_KAFKA_READ_TIMEOUT_SECS") {
        config.read_timeout =
            Duration::from_secs(parse_positive("IGGY_KAFKA_READ_TIMEOUT_SECS", &raw)?);
    }
    if let Some(raw) = env_var("IGGY_KAFKA_WRITE_TIMEOUT_SECS") {
        config.write_timeout =
            Duration::from_secs(parse_positive("IGGY_KAFKA_WRITE_TIMEOUT_SECS", &raw)?);
    }
    if let Some(raw) = env_var("IGGY_KAFKA_PRE_AUTH_TIMEOUT_SECS") {
        config.pre_auth_timeout =
            Duration::from_secs(parse_positive("IGGY_KAFKA_PRE_AUTH_TIMEOUT_SECS", &raw)?);
    }
    if let Some(raw) = env_var("IGGY_KAFKA_MAX_CONCURRENT_AUTHENTICATIONS") {
        // Rejecting zero matters more here than elsewhere: a zero-permit semaphore never yields,
        // so every authentication would wait out its budget and no client could ever log in.
        let max_concurrent_authentications: usize =
            parse_positive("IGGY_KAFKA_MAX_CONCURRENT_AUTHENTICATIONS", &raw)?;
        // Both caps build a `Semaphore`, so both need the same ceiling: above `MAX_PERMITS`,
        // `Semaphore::new` panics and the gateway dies on a startup value instead of rejecting it.
        if max_concurrent_authentications > Semaphore::MAX_PERMITS {
            return Err(format!(
                "IGGY_KAFKA_MAX_CONCURRENT_AUTHENTICATIONS {max_concurrent_authentications} \
                 exceeds maximum {}",
                Semaphore::MAX_PERMITS
            ));
        }
        config.max_concurrent_authentications = max_concurrent_authentications;
    }
    // Drain of 0 is valid: abandon in-flight connections immediately on shutdown.
    if let Some(raw) = env_var("IGGY_KAFKA_SHUTDOWN_DRAIN_TIMEOUT_SECS") {
        let secs: u64 = raw
            .parse()
            .map_err(|e| format!("invalid IGGY_KAFKA_SHUTDOWN_DRAIN_TIMEOUT_SECS `{raw}`: {e}"))?;
        config.shutdown_drain_timeout = Duration::from_secs(secs);
    }
    // Not `parse_positive`: 0 is the default, and the right value for a single gateway.
    if let Some(raw) = env_var("IGGY_KAFKA_INSTANCE_ID") {
        config.instance_id = raw
            .parse()
            .map_err(|e| format!("invalid IGGY_KAFKA_INSTANCE_ID `{raw}`: {e}"))?;
    } else {
        // A second gateway left on the default hands out the same producer ids as the first,
        // and nothing in the cluster can detect it, so an unset value is surfaced at startup.
        warn!(
            "IGGY_KAFKA_INSTANCE_ID is not set, defaulting to 0: every gateway fronting the same \
             Iggy cluster needs its own value, or they hand out colliding producer ids"
        );
    }
    config.auto_topic_creation = load_auto_topic_creation(bridge_enabled()?)?;
    if config.auto_topic_creation.enabled {
        info!(
            partitions = config.auto_topic_creation.partitions,
            "auto topic creation is on: a Metadata lookup of a missing topic creates it"
        );
    }
    reject_unread_iggy_tls(
        config.sasl_enabled(),
        bridge_enabled()?,
        env_var("IGGY_KAFKA_LISTENERS").is_some(),
    )?;

    Ok(config)
}

/// The listeners, from `IGGY_KAFKA_LISTENERS` or from the single-listener variables.
///
/// With a listener list, `IGGY_KAFKA_BIND_ADDR` is ignored with a warning rather than refused:
/// the gateway's own image bakes `IGGY_KAFKA_BIND_ADDR=0.0.0.0:9093` in, and refusing it would
/// make `IGGY_KAFKA_LISTENERS` unusable there. Every other single-listener variable is refused
/// beside a list, since ignoring it would advertise or authenticate differently from what its
/// operator set.
fn load_listeners() -> Result<Vec<ListenerConfig>, String> {
    let advertised = env_var("IGGY_KAFKA_ADVERTISED_LISTENERS");
    if let Some(raw) = env_var("IGGY_KAFKA_LISTENERS") {
        let refused: Vec<&str> = SINGLE_LISTENER_ENV_VARS
            .iter()
            .copied()
            .filter(|var| *var != "IGGY_KAFKA_BIND_ADDR" && env_var(var).is_some())
            .collect();
        if !refused.is_empty() {
            return Err(format!(
                "IGGY_KAFKA_LISTENERS is set together with {}, which belong to the \
                 single-listener form; advertise through IGGY_KAFKA_ADVERTISED_LISTENERS, and \
                 turn SASL on with a SASL_PLAINTEXT or SASL_SSL listener",
                refused.join(", ")
            ));
        }
        if env_var("IGGY_KAFKA_BIND_ADDR").is_some() {
            warn!(
                "IGGY_KAFKA_BIND_ADDR set but IGGY_KAFKA_LISTENERS takes precedence; it is \
                 ignored (bind through IGGY_KAFKA_LISTENERS)"
            );
        }
        let mut listeners = parse_listeners(&raw).map_err(|e| e.to_string())?;
        if let Some(advertised) = advertised {
            apply_advertised_listeners(&mut listeners, &advertised).map_err(|e| e.to_string())?;
        }
        return Ok(listeners);
    }
    if advertised.is_some() {
        return Err(
            "IGGY_KAFKA_ADVERTISED_LISTENERS is set but IGGY_KAFKA_LISTENERS is not; the \
             single-listener form advertises through IGGY_KAFKA_ADVERTISED_HOST and \
             IGGY_KAFKA_ADVERTISED_PORT"
                .to_string(),
        );
    }
    let sasl_enabled = match env_var("IGGY_KAFKA_SASL_ENABLED") {
        Some(raw) => parse_bool("IGGY_KAFKA_SASL_ENABLED", &raw)?,
        None => false,
    };
    let protocol = if sasl_enabled {
        SecurityProtocol::SaslPlaintext
    } else {
        SecurityProtocol::Plaintext
    };
    let bind_addr = env_var("IGGY_KAFKA_BIND_ADDR")
        .unwrap_or_else(|| GatewayConfig::default().listeners[0].bind_addr.clone());
    let mut listener = ListenerConfig::new(protocol, bind_addr);
    listener.advertised_host = env_var("IGGY_KAFKA_ADVERTISED_HOST");
    if let Some(raw) = env_var("IGGY_KAFKA_ADVERTISED_PORT") {
        listener.advertised_port = Some(parse_positive("IGGY_KAFKA_ADVERTISED_PORT", &raw)?);
    }
    Ok(vec![listener])
}

/// The TLS material, required exactly when a listener speaks TLS.
///
/// Every combination in which something set would go unread is refused, the way the SASL and
/// Iggy-hop switches are: material without a TLS listener, a client CA without client
/// authentication, mapping rules without an `SSL` listener to apply them on. Each reads to whoever
/// set it as a protection that is on.
fn load_tls(listeners: &[ListenerConfig]) -> Result<Option<TlsConfig>, String> {
    let set: Vec<&str> = TLS_ENV_VARS
        .iter()
        .copied()
        .filter(|var| env_var(var).is_some())
        .collect();
    if !listeners
        .iter()
        .any(|listener| listener.protocol.uses_tls())
    {
        if set.is_empty() {
            return Ok(None);
        }
        return Err(format!(
            "{} set but no listener uses TLS; add an SSL or SASL_SSL listener to \
             IGGY_KAFKA_LISTENERS, or unset them",
            set.join(", ")
        ));
    }
    let cert_file = env_var("IGGY_KAFKA_TLS_CERT_FILE").ok_or(
        "IGGY_KAFKA_TLS_CERT_FILE must name a PEM certificate for an SSL or SASL_SSL listener",
    )?;
    let key_file = env_var("IGGY_KAFKA_TLS_KEY_FILE").ok_or(
        "IGGY_KAFKA_TLS_KEY_FILE must name a PEM private key for an SSL or SASL_SSL listener",
    )?;
    let mut tls = TlsConfig::new(cert_file, key_file);

    let client_auth = match env_var("IGGY_KAFKA_TLS_CLIENT_AUTH") {
        Some(raw) => raw
            .parse::<ClientAuth>()
            .map_err(|e| format!("invalid IGGY_KAFKA_TLS_CLIENT_AUTH `{raw}`: {e}"))?,
        None => ClientAuth::None,
    };
    let has_ssl_listener = listeners
        .iter()
        .any(|listener| listener.protocol.authenticates_by_certificate());
    if client_auth.is_enabled() && !has_ssl_listener {
        return Err(format!(
            "IGGY_KAFKA_TLS_CLIENT_AUTH is {client_auth} but no SSL listener is configured; it \
             applies to SSL listeners only, and a SASL_SSL listener asks for no client \
             certificate"
        ));
    }
    match (
        client_auth.is_enabled(),
        env_var("IGGY_KAFKA_TLS_CLIENT_CA_FILE"),
    ) {
        (true, Some(ca_file)) => tls = tls.with_client_auth(client_auth, ca_file),
        (true, None) => {
            return Err(format!(
                "IGGY_KAFKA_TLS_CLIENT_AUTH is {client_auth} but IGGY_KAFKA_TLS_CLIENT_CA_FILE is \
                 not set; nothing could verify a client certificate"
            ));
        }
        (false, Some(_)) => {
            return Err(
                "IGGY_KAFKA_TLS_CLIENT_CA_FILE is set but IGGY_KAFKA_TLS_CLIENT_AUTH is not \
                 requested or required; no client would be asked for a certificate"
                    .to_string(),
            );
        }
        (false, None) => {}
    }
    if let Some(raw) = env_var("IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES") {
        if !client_auth.is_enabled() {
            return Err(
                "IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES is set but no SSL listener verifies \
                 client certificates; the rules apply to an SSL listener with \
                 IGGY_KAFKA_TLS_CLIENT_AUTH requested or required, and on SASL_SSL the \
                 principal comes from SASL"
                    .to_string(),
            );
        }
        let rules = raw
            .parse()
            .map_err(|e| format!("invalid IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES: {e}"))?;
        tls = tls.with_principal_mapping(rules);
    }
    // Zero is valid: never re-read the files.
    if let Some(raw) = env_var("IGGY_KAFKA_TLS_RELOAD_INTERVAL_SECS") {
        let secs: u64 = raw
            .parse()
            .map_err(|e| format!("invalid IGGY_KAFKA_TLS_RELOAD_INTERVAL_SECS `{raw}`: {e}"))?;
        tls = tls.with_reload_interval(Duration::from_secs(secs));
    }
    Ok(Some(tls))
}

/// Refuses `IGGY_KAFKA_IGGY_TLS_*` while nothing would read them.
///
/// The bridge and the SASL credential check are the two things that dial Iggy. With both off,
/// accepting the switches lets an operator believe the link to Iggy is encrypted when there is no
/// link, the same mistake `IggyTls::from_env` refuses to start for.
fn reject_unread_iggy_tls(
    sasl_enabled: bool,
    bridge_enabled: bool,
    listener_list: bool,
) -> Result<(), String> {
    if sasl_enabled || bridge_enabled {
        return Ok(());
    }
    let set: Vec<&str> = IggyTls::KNOWN_ENV_VARS
        .iter()
        .copied()
        .filter(|var| std::env::var(var).is_ok())
        .collect();
    if set.is_empty() {
        return Ok(());
    }
    let sasl_switch = if listener_list {
        "no listener in IGGY_KAFKA_LISTENERS runs SASL"
    } else {
        "IGGY_KAFKA_SASL_ENABLED is not true"
    };
    Err(format!(
        "{} set but {sasl_switch} and IGGY_KAFKA_BRIDGE_ENABLED is not true; only the SASL \
         credential check and the bridge dial Iggy, so nothing would be encrypted",
        set.join(", ")
    ))
}

fn env_var(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// Parse a strictly-positive value, rejecting `0` (which for connection caps and timeouts would
/// silently disable the listener) and unparsable input.
fn parse_positive<T>(key: &str, raw: &str) -> Result<T, String>
where
    T: FromStr + Default + PartialEq,
    T::Err: Display,
{
    let value: T = raw
        .parse()
        .map_err(|e| format!("invalid {key} `{raw}`: {e}"))?;
    if value == T::default() {
        return Err(format!("{key} must be greater than 0"));
    }
    Ok(value)
}

/// `IGGY_KAFKA_AUTO_CREATE_TOPICS_*`. A partition count with creation off, or creation without a
/// bridge to create through, is refused rather than silently ignored.
fn load_auto_topic_creation(bridge_enabled: bool) -> Result<AutoTopicCreationConfig, String> {
    let enabled = match env_var("IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED") {
        Some(raw) => parse_bool("IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED", &raw)?,
        None => false,
    };
    let partitions = env_var("IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS");
    if !enabled {
        if partitions.is_some() {
            return Err("IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS is set but \
                 IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED is not true"
                .to_string());
        }
        return Ok(AutoTopicCreationConfig::default());
    }
    if !bridge_enabled {
        return Err(
            "IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED needs IGGY_KAFKA_BRIDGE_ENABLED=true"
                .to_string(),
        );
    }
    let partitions = match partitions {
        Some(raw) => {
            let count: u32 = parse_positive("IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS", &raw)?;
            if count > MAX_TOPIC_PARTITIONS {
                return Err(format!(
                    "IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS {count} exceeds Iggy's per-topic \
                     maximum {MAX_TOPIC_PARTITIONS}"
                ));
            }
            count
        }
        None => DEFAULT_TOPIC_PARTITIONS,
    };
    Ok(AutoTopicCreationConfig {
        enabled,
        partitions,
    })
}

/// Wait for Ctrl-C (SIGINT) or, on Unix, SIGTERM (`docker stop`).
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use iggy_gateway_kafka::listener::SecurityProtocol;
    use iggy_gateway_kafka::tls::ClientAuth;

    use super::{
        KNOWN_KAFKA_ENV_VARS, load_config, parse_positive, reject_unknown_kafka_env_vars,
        reject_unread_iggy_tls,
    };

    /// Leaves every variable `load_config` reads unset, so one test's value cannot decide
    /// another's result.
    ///
    /// # Safety
    /// Only sound under `#[serial]`, see the note on the first test below.
    fn clear_gateway_env() {
        for key in KNOWN_KAFKA_ENV_VARS {
            unsafe {
                std::env::remove_var(key);
            }
        }
    }

    /// Runs `load_config` with exactly `vars` set, then clears them again.
    fn load_with(vars: &[(&str, &str)]) -> Result<super::GatewayConfig, String> {
        clear_gateway_env();
        for (key, value) in vars {
            unsafe {
                std::env::set_var(key, value);
            }
        }
        let loaded = load_config();
        clear_gateway_env();
        loaded
    }

    /// Sequential (not two separate `#[test]` fns), and `#[serial]` (unkeyed - this binary's
    /// default group), shared with the instance-id and TLS tests below since all of them touch the
    /// process environment. The `#[serial]` tests in this module are the only ones compiled into
    /// *this* binary (`main.rs` -> the `iggy-gateway-kafka` bin's own test harness) - `auth`'s,
    /// `bridge::config`'s and `server`'s env-touching tests compile into the separate lib test
    /// binary, and `serial_test`'s mutex is process-local, so it does not (and does not need to)
    /// coordinate with any of those; `server.rs`'s own `#[serial]` test makes the mirror-image
    /// note.
    ///
    /// # Safety
    /// Edition 2024's `env::set_var`/`remove_var` are unsound against *any* concurrent env read
    /// or write on another thread in this process, not merely one touching this same key - env
    /// vars are process-wide C `environ` state, and the race is at that level. `#[serial]` is what
    /// makes this sound, by excluding every other `#[serial]`-tagged test in this binary while
    /// this one runs; being single-threaded within this function is necessary but not sufficient
    /// on its own.
    #[test]
    #[serial]
    fn reject_unknown_kafka_env_vars_flags_typo_but_accepts_known_keys() {
        unsafe {
            std::env::set_var("IGGY_KAFKA_BIN_ADDR", "127.0.0.1:9093"); // typo: missing D
        }
        let typo_result = reject_unknown_kafka_env_vars();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_BIN_ADDR");
        }
        assert!(
            typo_result.is_err(),
            "typo'd IGGY_KAFKA_ var must be rejected, not silently ignored"
        );

        unsafe {
            std::env::set_var("IGGY_KAFKA_BIND_ADDR", "127.0.0.1:9093");
        }
        let known_result = reject_unknown_kafka_env_vars();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_BIND_ADDR");
        }
        assert!(
            known_result.is_ok(),
            "known IGGY_KAFKA_ var must be accepted"
        );

        // A bridge-specific var (not in this module's own KNOWN_KAFKA_ENV_VARS) must also be
        // accepted - the two lists are checked separately, not merged into one.
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_ADDR", "127.0.0.1:8090");
        }
        let bridge_var_result = reject_unknown_kafka_env_vars();
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_ADDR");
        }
        assert!(
            bridge_var_result.is_ok(),
            "known bridge IGGY_KAFKA_ var must be accepted"
        );
    }

    /// `#[serial]` and `# Safety` as on
    /// `reject_unknown_kafka_env_vars_flags_typo_but_accepts_known_keys` above.
    ///
    /// Covers both halves of adding this var: it has to be in `KNOWN_KAFKA_ENV_VARS` (or setting
    /// it refuses to start the gateway) and it has to keep `0`, which `parse_positive` rejects.
    #[test]
    #[serial]
    fn given_an_instance_id_env_var_when_loading_config_should_accept_and_parse_it() {
        let seven = load_with(&[("IGGY_KAFKA_INSTANCE_ID", "7")]);
        let zero = load_with(&[("IGGY_KAFKA_INSTANCE_ID", "0")]);
        let overflow = load_with(&[("IGGY_KAFKA_INSTANCE_ID", "65536")]);

        assert_eq!(seven.expect("instance id 7 must load").instance_id, 7);
        assert_eq!(zero.expect("instance id 0 must load").instance_id, 0);
        assert!(
            overflow.is_err(),
            "an instance id above u16::MAX must be rejected, not truncated"
        );
    }

    /// A bad instance id must fail startup loudly. Silently defaulting to 0 would let two
    /// gateways mint colliding producer ids, which Kafka requires to be unique cluster-wide,
    /// and the message has to name the variable and the offending value or an operator cannot
    /// act on it.
    #[test]
    #[serial]
    fn given_an_unparsable_instance_id_when_loading_config_should_reject_and_name_it() {
        for raw in ["abc", "-1", "", " 7", "7.0"] {
            let loaded = load_with(&[("IGGY_KAFKA_INSTANCE_ID", raw)]);
            let error = loaded.err().unwrap_or_else(|| {
                panic!("instance id `{raw}` must be rejected, not silently defaulted to 0")
            });
            assert!(
                error.contains("IGGY_KAFKA_INSTANCE_ID"),
                "`{raw}` rejection must name the variable, got: {error}"
            );
            assert!(
                error.contains(raw),
                "`{raw}` rejection must quote the offending value, got: {error}"
            );
        }
    }

    #[test]
    #[serial]
    fn given_no_auto_create_vars_when_loading_config_should_leave_auto_creation_off() {
        let config = load_with(&[]).expect("defaults load");
        assert!(!config.auto_topic_creation.enabled);
    }

    #[test]
    #[serial]
    fn given_auto_creation_with_a_bridge_when_loading_config_should_read_the_partition_count() {
        let config = load_with(&[
            ("IGGY_KAFKA_BRIDGE_ENABLED", "true"),
            ("IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED", "true"),
            ("IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS", "6"),
        ])
        .expect("a valid auto-creation config loads");
        assert!(config.auto_topic_creation.enabled);
        assert_eq!(config.auto_topic_creation.partitions, 6);
    }

    #[test]
    #[serial]
    fn given_an_invalid_auto_create_setting_when_loading_config_should_refuse_and_name_it() {
        let cases: [(&[(&str, &str)], &str); 4] = [
            (
                &[("IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED", "true")],
                "IGGY_KAFKA_BRIDGE_ENABLED",
            ),
            (
                &[("IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS", "3")],
                "IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED",
            ),
            (
                &[
                    ("IGGY_KAFKA_BRIDGE_ENABLED", "true"),
                    ("IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED", "true"),
                    ("IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS", "1001"),
                ],
                "IGGY_KAFKA_AUTO_CREATE_TOPICS_PARTITIONS",
            ),
            (
                &[("IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED", "yes")],
                "IGGY_KAFKA_AUTO_CREATE_TOPICS_ENABLED",
            ),
        ];
        for (vars, named) in cases {
            let error = load_with(vars)
                .err()
                .unwrap_or_else(|| panic!("{vars:?} must be refused"));
            assert!(error.contains(named), "{vars:?}: {error}");
        }
    }

    /// `#[serial]` for the same reason as the test above: it mutates process-wide env state.
    #[test]
    #[serial]
    fn given_iggy_tls_with_nothing_dialing_iggy_should_refuse_to_start() {
        unsafe {
            std::env::set_var("IGGY_KAFKA_IGGY_TLS_ENABLED", "true");
        }
        let nothing_reads_it = reject_unread_iggy_tls(false, false, false);
        let nothing_reads_it_in_a_list = reject_unread_iggy_tls(false, false, true);
        let sasl_reads_it = reject_unread_iggy_tls(true, false, false);
        let bridge_reads_it = reject_unread_iggy_tls(false, true, false);
        unsafe {
            std::env::remove_var("IGGY_KAFKA_IGGY_TLS_ENABLED");
        }
        assert!(
            nothing_reads_it
                .expect_err(
                    "nothing reads the TLS switch with SASL and the bridge off, so accepting it \
                     hides that"
                )
                .contains("IGGY_KAFKA_SASL_ENABLED")
        );
        let in_a_list = nothing_reads_it_in_a_list.expect_err("refused with a listener list too");
        assert!(
            !in_a_list.contains("IGGY_KAFKA_SASL_ENABLED"),
            "a listener list refuses that switch, so naming it misleads, got: {in_a_list}"
        );
        assert!(sasl_reads_it.is_ok());
        assert!(bridge_reads_it.is_ok());
    }

    #[test]
    #[serial]
    fn given_the_single_listener_form_should_build_one_listener_from_its_variables() {
        let plain = load_with(&[("IGGY_KAFKA_BIND_ADDR", "0.0.0.0:19093")])
            .expect("a bind address alone is valid");
        assert_eq!(plain.listeners.len(), 1);
        assert_eq!(plain.listeners[0].protocol, SecurityProtocol::Plaintext);
        assert_eq!(plain.listeners[0].bind_addr, "0.0.0.0:19093");
        assert!(plain.tls.is_none());

        let sasl = load_with(&[
            ("IGGY_KAFKA_SASL_ENABLED", "true"),
            ("IGGY_KAFKA_ADVERTISED_HOST", "kafka.example.com"),
            ("IGGY_KAFKA_ADVERTISED_PORT", "9093"),
        ])
        .expect("the SASL switch selects the listener's protocol");
        assert_eq!(sasl.listeners[0].protocol, SecurityProtocol::SaslPlaintext);
        assert_eq!(
            sasl.listeners[0].advertised_host.as_deref(),
            Some("kafka.example.com")
        );
        assert_eq!(sasl.listeners[0].advertised_port, Some(9093));
        assert!(sasl.sasl_enabled());

        let unset = load_with(&[]).expect("an empty environment is valid");
        assert_eq!(unset.listeners[0].bind_addr, "127.0.0.1:9093");
    }

    #[test]
    #[serial]
    fn given_both_listener_forms_should_refuse_all_but_the_bind_address() {
        for var in [
            "IGGY_KAFKA_SASL_ENABLED",
            "IGGY_KAFKA_ADVERTISED_HOST",
            "IGGY_KAFKA_ADVERTISED_PORT",
        ] {
            let value = if var == "IGGY_KAFKA_SASL_ENABLED" {
                "true"
            } else {
                "9093"
            };
            let error = load_with(&[
                ("IGGY_KAFKA_LISTENERS", "PLAINTEXT://127.0.0.1:9093"),
                (var, value),
            ])
            .unwrap_err();
            assert!(error.contains("IGGY_KAFKA_LISTENERS"), "got: {error}");
            assert!(error.contains(var), "got: {error}");
            if var != "IGGY_KAFKA_SASL_ENABLED" {
                assert!(
                    error.contains("IGGY_KAFKA_ADVERTISED_LISTENERS"),
                    "got: {error}"
                );
            }
        }

        // The image bakes a bind address in, so a listener list has to win over it.
        let config = load_with(&[
            ("IGGY_KAFKA_LISTENERS", "SSL://127.0.0.1:9094"),
            ("IGGY_KAFKA_BIND_ADDR", "0.0.0.0:9093"),
            ("IGGY_KAFKA_TLS_CERT_FILE", "/etc/iggy/kafka.pem"),
            ("IGGY_KAFKA_TLS_KEY_FILE", "/etc/iggy/kafka.key"),
        ])
        .expect("the list takes precedence over the baked bind address");
        assert_eq!(config.listeners.len(), 1);
        assert_eq!(config.listeners[0].bind_addr, "127.0.0.1:9094");

        let error = load_with(&[(
            "IGGY_KAFKA_ADVERTISED_LISTENERS",
            "PLAINTEXT://kafka.example.com:9093",
        )])
        .unwrap_err();
        assert!(
            error.contains("IGGY_KAFKA_ADVERTISED_LISTENERS"),
            "got: {error}"
        );
    }

    #[test]
    #[serial]
    fn given_kafka_style_listeners_with_tls_should_require_and_carry_the_material() {
        let missing = load_with(&[("IGGY_KAFKA_LISTENERS", "SSL://0.0.0.0:9094")]).unwrap_err();
        assert!(
            missing.contains("IGGY_KAFKA_TLS_CERT_FILE"),
            "got: {missing}"
        );

        let config = load_with(&[
            (
                "IGGY_KAFKA_LISTENERS",
                "PLAINTEXT://127.0.0.1:9093,SASL_SSL://0.0.0.0:9095",
            ),
            (
                "IGGY_KAFKA_ADVERTISED_LISTENERS",
                "SASL_SSL://kafka.example.com:9095",
            ),
            ("IGGY_KAFKA_TLS_CERT_FILE", "/etc/iggy/kafka.pem"),
            ("IGGY_KAFKA_TLS_KEY_FILE", "/etc/iggy/kafka.key"),
            ("IGGY_KAFKA_TLS_RELOAD_INTERVAL_SECS", "0"),
        ])
        .expect("a TLS listener with its material is valid");
        assert_eq!(config.listeners.len(), 2);
        assert_eq!(config.listeners[1].protocol, SecurityProtocol::SaslSsl);
        assert_eq!(
            config.listeners[1].advertised_host.as_deref(),
            Some("kafka.example.com")
        );
        let tls = config.tls.as_ref().expect("material carried through");
        assert_eq!(tls.cert_file.to_str(), Some("/etc/iggy/kafka.pem"));
        assert_eq!(tls.client_auth, ClientAuth::None);
        assert!(tls.reload_interval.is_zero());
        assert!(config.sasl_enabled());
        assert!(config.tls_enabled());
        assert!(!config.maps_certificate_identities());
    }

    #[test]
    #[serial]
    fn given_tls_variables_without_a_tls_listener_should_refuse() {
        let error = load_with(&[
            ("IGGY_KAFKA_LISTENERS", "PLAINTEXT://127.0.0.1:9093"),
            ("IGGY_KAFKA_TLS_CERT_FILE", "/etc/iggy/kafka.pem"),
        ])
        .unwrap_err();
        assert!(error.contains("IGGY_KAFKA_TLS_CERT_FILE"), "got: {error}");
        assert!(error.contains("no listener uses TLS"), "got: {error}");
    }

    #[test]
    #[serial]
    fn given_client_auth_settings_should_refuse_the_half_configured_combinations() {
        let base = [
            ("IGGY_KAFKA_LISTENERS", "SSL://0.0.0.0:9094"),
            ("IGGY_KAFKA_TLS_CERT_FILE", "/etc/iggy/kafka.pem"),
            ("IGGY_KAFKA_TLS_KEY_FILE", "/etc/iggy/kafka.key"),
        ];
        let with = |extra: &[(&str, &str)]| {
            let mut vars = base.to_vec();
            vars.extend_from_slice(extra);
            load_with(&vars)
        };

        let no_ca = with(&[("IGGY_KAFKA_TLS_CLIENT_AUTH", "required")]).unwrap_err();
        assert!(
            no_ca.contains("IGGY_KAFKA_TLS_CLIENT_CA_FILE"),
            "got: {no_ca}"
        );

        let no_auth = with(&[("IGGY_KAFKA_TLS_CLIENT_CA_FILE", "/etc/iggy/ca.pem")]).unwrap_err();
        assert!(
            no_auth.contains("IGGY_KAFKA_TLS_CLIENT_AUTH"),
            "got: {no_auth}"
        );

        let auth_without_ssl = load_with(&[
            ("IGGY_KAFKA_LISTENERS", "SASL_SSL://0.0.0.0:9095"),
            ("IGGY_KAFKA_TLS_CERT_FILE", "/etc/iggy/kafka.pem"),
            ("IGGY_KAFKA_TLS_KEY_FILE", "/etc/iggy/kafka.key"),
            ("IGGY_KAFKA_TLS_CLIENT_AUTH", "required"),
            ("IGGY_KAFKA_TLS_CLIENT_CA_FILE", "/etc/iggy/ca.pem"),
        ])
        .unwrap_err();
        assert!(
            auth_without_ssl.contains("no SSL listener"),
            "client authentication applies to SSL listeners only, got: {auth_without_ssl}"
        );

        let rules_without_auth =
            with(&[("IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES", "DEFAULT")]).unwrap_err();
        assert!(
            rules_without_auth.contains("IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES"),
            "got: {rules_without_auth}"
        );

        let bad_mode = with(&[
            ("IGGY_KAFKA_TLS_CLIENT_AUTH", "Required"),
            ("IGGY_KAFKA_TLS_CLIENT_CA_FILE", "/etc/iggy/ca.pem"),
        ])
        .unwrap_err();
        assert!(
            bad_mode.contains("IGGY_KAFKA_TLS_CLIENT_AUTH"),
            "got: {bad_mode}"
        );

        let bad_rules = with(&[
            ("IGGY_KAFKA_TLS_CLIENT_AUTH", "required"),
            ("IGGY_KAFKA_TLS_CLIENT_CA_FILE", "/etc/iggy/ca.pem"),
            ("IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES", "RULE:broken"),
        ])
        .unwrap_err();
        assert!(
            bad_rules.contains("IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES"),
            "got: {bad_rules}"
        );

        let complete = with(&[
            ("IGGY_KAFKA_TLS_CLIENT_AUTH", "required"),
            ("IGGY_KAFKA_TLS_CLIENT_CA_FILE", "/etc/iggy/ca.pem"),
            (
                "IGGY_KAFKA_TLS_PRINCIPAL_MAPPING_RULES",
                "RULE:^CN=([^,]+).*$/$1/L",
            ),
        ])
        .expect("a complete mTLS configuration is valid");
        let tls = complete.tls.as_ref().expect("material carried through");
        assert_eq!(tls.client_auth, ClientAuth::Required);
        assert_eq!(
            tls.principal_mapping.map("CN=Alice,O=Example").as_deref(),
            Some("alice")
        );
        assert!(complete.maps_certificate_identities());
    }

    #[test]
    fn parse_positive_rejects_zero() {
        assert!(parse_positive::<usize>("KEY", "0").is_err());
        assert!(parse_positive::<u16>("KEY", "0").is_err());
    }

    #[test]
    fn parse_positive_rejects_non_numeric() {
        assert!(parse_positive::<usize>("KEY", "abc").is_err());
    }

    #[test]
    fn parse_positive_accepts_positive_value() {
        assert_eq!(parse_positive::<u64>("KEY", "42").unwrap(), 42);
        assert_eq!(parse_positive::<u16>("KEY", "9093").unwrap(), 9093);
    }
}
