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

//! Whether the bridge reaches Iggy, for the gateway's readiness.
//!
//! A client of its own pings Iggy each second. The answer says nothing of whether the cluster
//! takes writes, only that a node answers and the bridge's user can sign in. Without a client, a
//! check dials every known node at once, so a node that does not answer delays no other.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use iggy::prelude::{IggyClient, IggyError, SystemClient};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::{IggyBridge, connect_client_to, out_of_reach, sign_out_and_shut_down};
use crate::bridge::config::IggyBridgeConfig;
use crate::bridge::error::BridgeError;

/// How often the probe asks Iggy.
const PROBE_EVERY: Duration = Duration::from_secs(1);

/// How long one ping, or the wait for a connect, may take.
const PING_TIMEOUT: Duration = Duration::from_secs(2);

/// Failed checks in a row that make Iggy unreachable. One failure is not enough: a node that
/// restarts or a view change can cost one ping, and the client moves on.
const FAILURES_TO_UNREACHABLE: u32 = 2;

/// What the probe last found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Reach {
    /// No check has ended yet.
    #[default]
    Unknown,
    Reachable,
    /// The failure of the last check.
    Unreachable(String),
}

/// The probe task, started at the first call of [`IggyBridge::reach`]. Dropping it stops the
/// task.
#[derive(Default)]
pub(super) struct ReachProbe {
    sender: watch::Sender<Reach>,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl ReachProbe {
    /// Stops the task and waits for it to sign its client out.
    pub(super) async fn stop(&self) {
        self.cancel.cancel();
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task
            && let Err(error) = task.await
        {
            debug!(%error, "the Iggy reach probe ended with an error");
        }
    }
}

impl Drop for ReachProbe {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl IggyBridge {
    /// Whether the bridge reaches Iggy, as a ping each second finds. The first call starts the
    /// pings, on a client of their own, so a busy admin client cannot delay them. They stop at
    /// [`Self::close`].
    #[must_use]
    pub fn reach(&self) -> watch::Receiver<Reach> {
        let probe = &self.reach;
        let receiver = probe.sender.subscribe();
        let mut task = probe.task.lock().unwrap_or_else(PoisonError::into_inner);
        if task.is_none() && !probe.cancel.is_cancelled() {
            let run = probe_iggy(
                self.config.clone(),
                probe.sender.clone(),
                probe.cancel.clone(),
            );
            *task = Some(tokio::spawn(run));
        }
        drop(task);
        receiver
    }
}

/// Pings Iggy every [`PROBE_EVERY`] until `cancel`, and sends what it finds to `reach`.
async fn probe_iggy(
    config: IggyBridgeConfig,
    reach: watch::Sender<Reach>,
    cancel: CancellationToken,
) {
    let mut probe = Probe::default();
    let mut tick = interval(PROBE_EVERY);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            _ = tick.tick() => {}
        }
        let checked = tokio::select! {
            () = cancel.cancelled() => break,
            checked = probe.check(&config) => checked,
        };
        probe.report(checked, &reach);
    }
    probe.close().await;
}

/// The probe's client, and the checks that failed in a row.
#[derive(Default)]
struct Probe {
    client: Option<Arc<IggyClient>>,
    /// A connect that outlived the check that started it. It goes on in its task, so a node that
    /// does not answer still runs into the connect's own timeout, and the dial moves on to the
    /// next node.
    connecting: Option<JoinHandle<Result<IggyClient, BridgeError>>>,
    failures: u32,
}

impl Probe {
    /// Pings Iggy on the probe's client, or waits for its connect. `Err` says why Iggy did not
    /// answer.
    async fn check(&mut self, config: &IggyBridgeConfig) -> Result<(), String> {
        let Some(client) = self.client.clone() else {
            return self.connect(config).await;
        };
        let failure = match timeout(PING_TIMEOUT, client.ping()).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => format!("a ping failed: {error}"),
            Err(_) => format!("no answer to a ping within {PING_TIMEOUT:?}"),
        };
        // The next check dials again, through every known node, rather than wait for this
        // client's own reconnects to the node it knew.
        self.client = None;
        tokio::spawn(async move {
            if let Err(error) = sign_out_and_shut_down(&client).await {
                debug!(%error, "the reach probe's client did not close cleanly");
            }
        });
        Err(failure)
    }

    async fn connect(&mut self, config: &IggyBridgeConfig) -> Result<(), String> {
        let connecting = self.connecting.get_or_insert_with(|| {
            let config = config.clone();
            tokio::spawn(async move { dial_every_node(&config).await })
        });
        let Ok(joined) = timeout(PING_TIMEOUT, connecting).await else {
            return Err(format!("no connection to Iggy within {PING_TIMEOUT:?}"));
        };
        self.connecting = None;
        match joined {
            Ok(Ok(client)) => {
                self.client = Some(Arc::new(client));
                Ok(())
            }
            Ok(Err(error)) => Err(format!("cannot connect: {error}")),
            Err(error) => Err(format!("the connect ended: {error}")),
        }
    }

    /// Sends what `checked` means to `reach`, and logs a change.
    fn report(&mut self, checked: Result<(), String>, reach: &watch::Sender<Reach>) {
        let next = match checked {
            Ok(()) => {
                self.failures = 0;
                Reach::Reachable
            }
            Err(failure) => {
                self.failures += 1;
                debug!(
                    failures = self.failures,
                    "Iggy did not answer the reach probe: {failure}"
                );
                if self.failures < FAILURES_TO_UNREACHABLE {
                    return;
                }
                Reach::Unreachable(failure)
            }
        };
        let previous = reach.send_replace(next.clone());
        match (&previous, &next) {
            (Reach::Reachable, Reach::Unreachable(failure)) => {
                warn!(
                    "Iggy is unreachable, so /ready answers 503 until it answers again: {failure}"
                );
            }
            (Reach::Unreachable(_), Reach::Reachable) => info!("Iggy is reachable again"),
            _ => {}
        }
    }

    /// Signs the client out, and drops a connect still running.
    async fn close(self) {
        if let Some(connecting) = self.connecting {
            connecting.abort();
        }
        if let Some(client) = self.client
            && let Err(error) = sign_out_and_shut_down(&client).await
        {
            debug!(%error, "the reach probe's client did not close cleanly");
        }
    }
}

/// A client signed in on the first known node that answers. Dials every node at once. A node that
/// answers later is signed out in the background.
async fn dial_every_node(config: &IggyBridgeConfig) -> Result<IggyClient, BridgeError> {
    let mut dials = JoinSet::new();
    for address in config.nodes.dial_order(&config.address) {
        let config = config.clone();
        dials.spawn(async move {
            let dialed = connect_client_to(&config, &address).await;
            (address, dialed)
        });
    }
    let mut failure = BridgeError::Iggy(IggyError::CannotEstablishConnection);
    while let Some(joined) = dials.join_next().await {
        let Ok((address, dialed)) = joined else {
            continue;
        };
        match dialed {
            Ok(client) => {
                config.nodes.mark_reachable(&address);
                tokio::spawn(sign_out_the_rest(dials));
                return Ok(client);
            }
            Err(error) if out_of_reach(&error) => {
                config.nodes.mark_unreachable(&address);
                if out_of_reach(&failure) {
                    failure = error;
                }
            }
            // A refused sign-in says more than a node out of reach.
            Err(error) => failure = error,
        }
    }
    Err(failure)
}

/// Signs out each client of `dials` that still connects.
async fn sign_out_the_rest(mut dials: JoinSet<(String, Result<IggyClient, BridgeError>)>) {
    while let Some(joined) = dials.join_next().await {
        if let Ok((_, Ok(client))) = joined
            && let Err(error) = sign_out_and_shut_down(&client).await
        {
            debug!(%error, "a spare client of the reach probe did not close cleanly");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_failed_checks_should_call_iggy_unreachable_from_the_second_in_a_row() {
        let (reach, receiver) = watch::channel(Reach::Unknown);
        let mut probe = Probe::default();

        probe.report(Ok(()), &reach);
        assert_eq!(*receiver.borrow(), Reach::Reachable);

        probe.report(Err("refused".to_string()), &reach);
        assert_eq!(
            *receiver.borrow(),
            Reach::Reachable,
            "one failed check is not enough"
        );

        probe.report(Err("refused again".to_string()), &reach);
        assert_eq!(
            *receiver.borrow(),
            Reach::Unreachable("refused again".to_string())
        );

        probe.report(Ok(()), &reach);
        assert_eq!(
            *receiver.borrow(),
            Reach::Reachable,
            "one answer makes Iggy reachable again"
        );
    }

    #[tokio::test]
    async fn given_no_iggy_when_the_probe_runs_should_call_it_unreachable() {
        let bridge = IggyBridge::offline_at("127.0.0.1:1");
        let mut reach = bridge.reach();

        let found = timeout(
            Duration::from_secs(10),
            reach.wait_for(|reach| matches!(reach, Reach::Unreachable(_))),
        )
        .await
        .expect("two failed checks within a few seconds")
        .expect("the probe keeps its sender")
        .clone();
        assert!(
            matches!(&found, Reach::Unreachable(failure) if failure.contains("connect")),
            "{found:?}"
        );

        bridge.close().await.ok();
        assert!(
            bridge.reach.task.lock().unwrap().is_none(),
            "close stops the probe"
        );
    }
}
