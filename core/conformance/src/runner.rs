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

//! Runs the tests of a corpus against one agent and judges every answer.

use crate::agent::RustAgent;
use crate::agent_host::{AgentCommand, AgentHost, HostError};
use crate::api::Method;
use crate::format;
use crate::format::{
    AdminCall, ClientSpec, Credentials, EntitySpec, ErrorExpectation, LoadedFile, Login,
    LoginKeyword, Node, ORCHESTRATOR, Operation, OrchestratorCall, PROXY, ProxyCall, Requirements,
    TestCase, Topology, Transport,
};
use crate::iggy_errors;
use crate::matcher::{self, Captures, compact};
use crate::protocol::{AgentError, CreateClientArgs, EntityKind, Hello, Reply, Request};
use crate::proxy::{ClientArgs, FailFramesArgs, ProxyHub, RefuseConnectionsArgs};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};

/// The entity id of the orchestrator's own client.
const ADMIN: &str = "admin";

/// The entity id of the orchestrator's client of the cluster leader.
const CLUSTER_ADMIN: &str = "admin-cluster";

/// How long past a time bound the runner waits before it calls an answer
/// late, so that a late answer fails with its time instead of a timeout.
const LATE_GRACE: Duration = Duration::from_secs(1);

/// The pause between two reads of an `eventually` operation.
const EVENTUALLY_INTERVAL: Duration = Duration::from_millis(100);

/// How long a reset may take before the runner restarts the agent.
const RESET_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct RunConfig {
    /// The TCP address of the single node.
    pub server: String,
    /// The TCP address of a node that cannot reach quorum, if one runs.
    pub no_quorum: Option<String>,
    /// The TCP addresses of the primary and of a backup of a cluster, if one runs.
    pub cluster: Option<ClusterAddresses>,
    pub root: Credentials,
    pub transport: Transport,
    pub agent: AgentCommand,
    /// The longest wait for one answer without a time bound.
    pub call_timeout: Duration,
    pub hello_timeout: Duration,
    /// Runs only the tests whose id contains this text.
    pub filter: Option<String>,
    /// Prints every answer as a JSON line and continues past mismatches, so
    /// that a test author can start from what the agent observed.
    pub record: bool,
}

#[derive(Debug, Clone)]
pub struct ClusterAddresses {
    pub leader: String,
    pub follower: String,
    /// The host that the nodes advertise to clients. The orchestrator listens
    /// there on the port of each node, so every client of the cluster goes
    /// through the fault proxy, a redirect included.
    pub proxy_host: Option<String>,
}

/// The proxy listener of a cluster node, as `fail_frames` and `expect_frames`
/// name it in place of a client.
pub const LEADER_LISTENER: &str = "@leader";
pub const FOLLOWER_LISTENER: &str = "@follower";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "verdict", content = "reason", rename_all = "snake_case")]
pub enum Verdict {
    Passed,
    Failed(String),
    Skipped(String),
    /// The test could not run: an agent failure, or a mistake in the file.
    Errored(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestReport {
    pub id: String,
    pub file: String,
    pub description: String,
    #[serde(flatten)]
    pub verdict: Verdict,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunReport {
    pub sdk: String,
    pub sdk_version: String,
    pub transport: Transport,
    pub tests: Vec<TestReport>,
}

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("the admin client cannot start: {0}")]
    Admin(String),
    #[error(transparent)]
    Agent(#[from] HostError),
    #[error("the agent does not support the {0:?} transport of this run")]
    Transport(Transport),
}

pub struct Runner {
    config: RunConfig,
    admin: RustAgent,
    proxy: ProxyHub,
    /// The listeners of the cluster nodes, open for the whole run.
    node_proxy: ProxyHub,
    host: Option<AgentHost>,
    hello: Hello,
}

impl Runner {
    /// Connects the admin client and starts the agent.
    ///
    /// # Errors
    ///
    /// Fails when the server or the agent cannot be reached.
    pub async fn start(config: RunConfig) -> Result<Self, RunError> {
        let admin = RustAgent::default();
        let mut admins = vec![(ADMIN, config.server.clone())];
        if let Some(cluster) = &config.cluster {
            admins.push((CLUSTER_ADMIN, cluster.leader.clone()));
        }
        for (entity, address) in admins {
            let create = Request::Create {
                kind: EntityKind::Client,
                entity: entity.to_owned(),
                args: to_value(&CreateClientArgs {
                    transport: Transport::Tcp,
                    address,
                    login: Some(config.root.clone()),
                }),
            };
            match admin.handle(create).await {
                Reply::Result(_) => {}
                other => return Err(RunError::Admin(describe_reply(&other))),
            }
        }
        let mut node_proxy = ProxyHub::default();
        if let Some(cluster) = &config.cluster
            && let Some(proxy_host) = &cluster.proxy_host
        {
            for (key, upstream) in [
                (LEADER_LISTENER, &cluster.leader),
                (FOLLOWER_LISTENER, &cluster.follower),
            ] {
                let bind = advertised(proxy_host, upstream).map_err(RunError::Admin)?;
                node_proxy
                    .listen_at(key, &bind, upstream)
                    .await
                    .map_err(|error| {
                        RunError::Admin(format!("cannot listen at {bind}: {error}"))
                    })?;
            }
        }
        let host = AgentHost::spawn(&config.agent, config.hello_timeout).await?;
        let hello = host.hello().clone();
        if !hello.transports.contains(&config.transport) {
            host.stop().await;
            return Err(RunError::Transport(config.transport));
        }
        Ok(Self {
            config,
            admin,
            host: Some(host),
            proxy: ProxyHub::default(),
            node_proxy,
            hello,
        })
    }

    #[must_use]
    pub fn hello(&self) -> &Hello {
        &self.hello
    }

    /// Runs every selected test of `files` in order.
    pub async fn run(&mut self, files: &[LoadedFile]) -> RunReport {
        let mut tests = Vec::new();
        for file in files {
            for test in &file.file.tests {
                let id = file.test_id(test);
                if self
                    .config
                    .filter
                    .as_ref()
                    .is_some_and(|filter| !id.contains(filter.as_str()))
                {
                    continue;
                }
                let started = Instant::now();
                let verdict = self.run_test(file, test, &id).await;
                let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                log_verdict(&id, &verdict);
                tests.push(TestReport {
                    id,
                    file: file.name.clone(),
                    description: test.description.clone(),
                    verdict,
                    duration_ms,
                });
            }
        }
        RunReport {
            sdk: self.hello.sdk.clone(),
            sdk_version: self.hello.sdk_version.clone(),
            transport: self.config.transport,
            tests,
        }
    }

    /// Stops the agent and closes the admin client.
    pub async fn stop(mut self) {
        if let Some(host) = self.host.take() {
            host.stop().await;
        }
        self.admin.reset().await;
    }

    async fn run_test(&mut self, file: &LoadedFile, test: &TestCase, id: &str) -> Verdict {
        if self.host.is_none() {
            match AgentHost::spawn(&self.config.agent, self.config.hello_timeout).await {
                Ok(host) => self.host = Some(host),
                Err(error) => {
                    return Verdict::Errored(format!("cannot restart the agent: {error}"));
                }
            }
        }
        let verdict = match self.execute(file, test, id).await {
            Ok(()) => Verdict::Passed,
            Err(Step::Failed(reason)) => Verdict::Failed(reason),
            Err(Step::Skipped(reason)) => Verdict::Skipped(reason),
            Err(Step::Errored(reason)) => Verdict::Errored(reason),
            Err(Step::Agent(error)) => Verdict::Errored(format!("agent: {error}")),
        };
        self.reset_agent().await;
        self.proxy.close_all();
        verdict
    }

    async fn execute(&mut self, file: &LoadedFile, test: &TestCase, id: &str) -> Result<(), Step> {
        self.check_requirements(&file.file.run_on)?;
        self.check_requirements(&test.run_on)?;
        for EntitySpec { client } in &test.create_entities {
            let topology = client.node.topology();
            if !self.topologies().contains(&topology) {
                return Err(Step::Skipped(format!(
                    "no {topology:?} deployment in this run"
                )));
            }
        }
        let admin = Self::admin_of(test);
        for entity in self.admin_entities() {
            self.admin
                .purge(entity, &self.config.root.username)
                .await
                .map_err(|error| Step::Errored(format!("cannot purge {entity}: {error}")))?;
        }
        let mut state = TestState {
            test: id.to_owned(),
            admin,
            ..TestState::default()
        };
        // Tests refer to the root credentials of the run with `$$ref`.
        state.captures.insert(
            "root_username",
            Value::from(self.config.root.username.clone()),
        );
        state.captures.insert(
            "root_password",
            Value::from(self.config.root.password.clone()),
        );
        // And to the addresses of the nodes, to check where a client went.
        state
            .captures
            .insert("server_address", Value::from(self.config.server.clone()));
        if self.config.cluster.is_some() {
            for (name, node) in [
                ("leader_address", Node::Leader),
                ("follower_address", Node::Follower),
            ] {
                let address = self.node_address(node).map_err(Step::Errored)?;
                state.captures.insert(name, Value::from(address));
            }
        }
        self.node_proxy.reset();
        for call in &file.file.initial_data {
            self.admin_call(admin, call, &mut state.captures)
                .await
                .map_err(|step| step.errored(&format!("initial_data {}", call.name)))?;
        }
        for EntitySpec { client } in &test.create_entities {
            self.create_client(client, &mut state).await?;
        }
        for (index, operation) in test.operations.iter().enumerate() {
            state.operation = index;
            self.run_operation(operation, &mut state)
                .await
                .map_err(|step| {
                    step.context(&format!(
                        "operation {index} ({}.{})",
                        operation.object, operation.name
                    ))
                })?;
        }
        for expectation in &test.expect_frames {
            let selected: Vec<Value> = self
                .proxy_of(&expectation.client)
                .frames(&expectation.client)
                .map_err(Step::Errored)?
                .iter()
                .filter(|frame| expectation.include_pings || !frame.is_ping())
                .filter(|frame| {
                    expectation.commands.is_empty() || expectation.commands.contains(&frame.command)
                })
                .filter(|frame| {
                    expectation
                        .direction
                        .is_none_or(|direction| direction == frame.direction)
                })
                .map(to_value)
                .collect();
            matcher::matches(
                &expectation.frames,
                &Value::Array(selected),
                &mut state.captures,
            )
            .map_err(|mismatch| {
                Step::Failed(format!(
                    "expect_frames of {}: {mismatch}",
                    expectation.client
                ))
            })?;
        }
        for call in &test.outcome {
            self.admin_call(admin, call, &mut state.captures)
                .await
                .map_err(|step| step.context(&format!("outcome {}", call.name)))?;
        }
        Ok(())
    }

    fn topologies(&self) -> Vec<Topology> {
        let mut topologies = vec![Topology::Single];
        if self.config.no_quorum.is_some() {
            topologies.push(Topology::NoQuorum);
        }
        if self.config.cluster.is_some() {
            topologies.push(Topology::Cluster);
        }
        topologies
    }

    fn check_requirements(&self, sets: &[Requirements]) -> Result<(), Step> {
        if sets.is_empty() {
            return Ok(());
        }
        let topologies = self.topologies();
        let mut reasons = Vec::new();
        for set in sets {
            let mut unmet = Vec::new();
            if let Some(topology) = set.topology
                && !topologies.contains(&topology)
            {
                unmet.push(format!("no {topology:?} deployment"));
            }
            if !set.transports.is_empty() && !set.transports.contains(&self.config.transport) {
                unmet.push(format!(
                    "needs {:?}, the run uses {:?}",
                    set.transports, self.config.transport
                ));
            }
            let missing: Vec<&str> = set
                .capabilities
                .iter()
                .filter(|capability| !self.hello.capabilities.contains(capability))
                .map(String::as_str)
                .collect();
            if !missing.is_empty() {
                unmet.push(format!("the agent lacks {}", missing.join(", ")));
            }
            if unmet.is_empty() {
                return Ok(());
            }
            reasons.push(unmet.join(" and "));
        }
        Err(Step::Skipped(reasons.join("; ")))
    }

    async fn create_client(
        &mut self,
        spec: &ClientSpec,
        state: &mut TestState,
    ) -> Result<(), Step> {
        let upstream = match spec.node {
            Node::Default => self.config.server.clone(),
            Node::NoQuorum => self
                .config
                .no_quorum
                .clone()
                .ok_or_else(|| Step::Skipped("no no_quorum node in this run".to_owned()))?,
            Node::Leader | Node::Follower => {
                if self.config.cluster.is_none() {
                    return Err(Step::Skipped("no cluster in this run".to_owned()));
                }
                self.node_address(spec.node).map_err(Step::Errored)?
            }
        };
        let proxied_cluster = spec.node.topology() == Topology::Cluster
            && self
                .config
                .cluster
                .as_ref()
                .is_some_and(|cluster| cluster.proxy_host.is_some());
        // A client of a proxied cluster already goes through the node listeners.
        let address = if spec.through_proxy && !proxied_cluster {
            self.proxy
                .listen(&spec.id, &upstream)
                .await
                .map_err(|error| Step::Errored(format!("cannot open a proxy listener: {error}")))?
        } else {
            upstream
        };
        let login = match &spec.login {
            Login::Keyword(LoginKeyword::Root) => Some(self.config.root.clone()),
            Login::Keyword(LoginKeyword::None) => None,
            Login::Credentials(credentials) => Some(credentials.clone()),
        };
        let request = Request::Create {
            kind: EntityKind::Client,
            entity: spec.id.clone(),
            args: to_value(&CreateClientArgs {
                transport: self.config.transport,
                address,
                login,
            }),
        };
        match self.request(request, self.config.call_timeout).await? {
            Reply::Result(_) => {
                state.entities.insert(spec.id.clone(), EntityKind::Client);
                Ok(())
            }
            Reply::Unsupported(reason) => Err(Step::Skipped(reason)),
            other => Err(Step::Errored(format!(
                "cannot create {}: {}",
                spec.id,
                describe_reply(&other)
            ))),
        }
    }

    async fn run_operation(
        &mut self,
        operation: &Operation,
        state: &mut TestState,
    ) -> Result<(), Step> {
        let args =
            matcher::resolve_refs(&Value::Object(operation.arguments.clone()), &state.captures)
                .map_err(Step::Errored)?;
        match operation.object.as_str() {
            ORCHESTRATOR => self.orchestrator_call(operation, args, state).await,
            PROXY => self.proxy_call(operation, args),
            format::ADMIN => self.admin_operation(operation, args, state).await,
            entity => self.entity_call(entity, operation, args, state).await,
        }
    }

    async fn entity_call(
        &mut self,
        entity: &str,
        operation: &Operation,
        args: Value,
        state: &mut TestState,
    ) -> Result<(), Step> {
        let kind = *state
            .entities
            .get(entity)
            .ok_or_else(|| Step::Errored(format!("no entity {entity:?}")))?;
        let method = Method::from_str(&operation.name)
            .map_err(|_| Step::Errored(format!("unknown method {}", operation.name)))?;
        if !method.targets().contains(&kind) {
            return Err(Step::Errored(format!(
                "{} is a method of {:?}, and {entity} is a {kind:?}",
                operation.name,
                method.targets()
            )));
        }
        if let Some(capability) = method.capability(kind)
            && !self
                .hello
                .capabilities
                .iter()
                .any(|known| known == capability)
        {
            return Err(Step::Skipped(format!("the agent lacks {capability}")));
        }
        method
            .check_args(&args)
            .map_err(|error| Step::Errored(format!("arguments: {error}")))?;
        if let Some(task) = &operation.background {
            let started = Instant::now();
            let request = Request::Start {
                task: task.clone(),
                entity: entity.to_owned(),
                method: operation.name.clone(),
                args,
            };
            return match self.request(request, self.config.call_timeout).await? {
                Reply::Result(Value::Null) => {
                    state.tasks.insert(
                        task.clone(),
                        Task {
                            method,
                            kind,
                            started,
                        },
                    );
                    Ok(())
                }
                Reply::Unsupported(reason) => Err(Step::Skipped(reason)),
                other => Err(Step::Errored(format!("start: {}", describe_reply(&other)))),
            };
        }
        let save_as = match (method.creates(), &operation.save_result_as) {
            (Some(_), Some(id)) => Some(id.clone()),
            (Some(_), None) => {
                return Err(Step::Errored(format!(
                    "{} creates an entity and needs save_result_as",
                    operation.name
                )));
            }
            (None, Some(_)) => {
                return Err(Step::Errored(format!(
                    "{} creates no entity to save",
                    operation.name
                )));
            }
            (None, None) => None,
        };
        if operation.eventually.is_some() {
            return self
                .eventually(entity, operation, (method, kind), &args, state)
                .await;
        }
        let request = Request::Call {
            entity: entity.to_owned(),
            method: operation.name.clone(),
            args,
            save_as: save_as.clone(),
        };
        let started = Instant::now();
        let limit = operation
            .within
            .map_or(self.config.call_timeout, |within| within + LATE_GRACE);
        let reply = self.timed_request(request, limit, operation.within).await?;
        let elapsed = started.elapsed();
        let produced = matches!(reply, Reply::Result(_));
        self.judge_recorded(state, method, kind, operation, reply, elapsed)?;
        self.bounds_recorded(state, elapsed, operation)?;
        if let (Some(created), Some(id), true) = (method.creates(), save_as, produced) {
            state.entities.insert(id, created);
        }
        Ok(())
    }

    /// Repeats a read until its answer matches, or reports the last mismatch.
    async fn eventually(
        &mut self,
        entity: &str,
        operation: &Operation,
        (method, kind): (Method, EntityKind),
        args: &Value,
        state: &mut TestState,
    ) -> Result<(), Step> {
        let limit = operation.eventually.unwrap_or_default();
        let started = Instant::now();
        loop {
            let request = Request::Call {
                entity: entity.to_owned(),
                method: operation.name.clone(),
                args: args.clone(),
                save_as: None,
            };
            let reply = self.request(request, self.config.call_timeout).await?;
            match judge(
                method,
                kind,
                operation.expect_result.as_ref(),
                operation.expect_error.as_ref(),
                reply,
                &mut state.captures,
            ) {
                Err(Step::Failed(_)) if started.elapsed() < limit => {
                    tokio::time::sleep(EVENTUALLY_INTERVAL).await;
                }
                Err(Step::Failed(reason)) => {
                    return Err(Step::Failed(format!("still after {limit:?}: {reason}")));
                }
                other => return other,
            }
        }
    }

    async fn orchestrator_call(
        &mut self,
        operation: &Operation,
        args: Value,
        state: &mut TestState,
    ) -> Result<(), Step> {
        let call = OrchestratorCall::from_str(&operation.name).map_err(Step::Errored)?;
        match call {
            OrchestratorCall::Wait => {
                let args: WaitArgs = decode(args)?;
                tokio::time::sleep(Duration::from_millis(args.duration_ms)).await;
                Ok(())
            }
            OrchestratorCall::Await => {
                let args: TaskArgs = decode(args)?;
                let task = state
                    .tasks
                    .remove(&args.task)
                    .ok_or_else(|| Step::Errored(format!("no task {:?}", args.task)))?;
                let limit = operation.within.map_or(self.config.call_timeout, |within| {
                    (task.started + within + LATE_GRACE).saturating_duration_since(Instant::now())
                });
                let reply = self
                    .timed_request(Request::Await { task: args.task }, limit, operation.within)
                    .await?;
                let elapsed = task.started.elapsed();
                self.judge_recorded(state, task.method, task.kind, operation, reply, elapsed)?;
                self.bounds_recorded(state, elapsed, operation)
            }
            OrchestratorCall::Cancel => {
                let args: TaskArgs = decode(args)?;
                state
                    .tasks
                    .remove(&args.task)
                    .ok_or_else(|| Step::Errored(format!("no task {:?}", args.task)))?;
                let reply = self
                    .request(
                        Request::Cancel { task: args.task },
                        self.config.call_timeout,
                    )
                    .await?;
                expect_null(&reply, "cancel")
            }
            OrchestratorCall::Close => {
                let args: EntityArgs = decode(args)?;
                state
                    .entities
                    .remove(&args.entity)
                    .ok_or_else(|| Step::Errored(format!("no entity {:?}", args.entity)))?;
                let reply = self
                    .request(
                        Request::Close {
                            entity: args.entity,
                        },
                        self.config.call_timeout,
                    )
                    .await?;
                expect_null(&reply, "close")
            }
        }
    }

    fn proxy_call(&self, operation: &Operation, args: Value) -> Result<(), Step> {
        let call = ProxyCall::from_str(&operation.name).map_err(Step::Errored)?;
        let applied = match call {
            ProxyCall::FailFrames => {
                let args: FailFramesArgs = decode(args)?;
                self.proxy_of(&args.client).fail_frames(args)
            }
            ProxyCall::RefuseConnections => {
                let args: RefuseConnectionsArgs = decode(args)?;
                self.proxy_of(&args.client).refuse_connections(&args)
            }
            ProxyCall::ClearRules => {
                let args: ClientArgs = decode(args)?;
                self.proxy_of(&args.client).clear_rules(&args.client)
            }
        };
        applied.map_err(Step::Errored)
    }

    /// The node listeners for `@leader` and `@follower`, the per-client
    /// listeners for an entity.
    fn proxy_of(&self, client: &str) -> &ProxyHub {
        if client.starts_with('@') {
            &self.node_proxy
        } else {
            &self.proxy
        }
    }

    /// The address that a client of a cluster node dials: the advertised proxy
    /// address when the cluster runs behind the proxy.
    fn node_address(&self, node: Node) -> Result<String, String> {
        let cluster = self
            .config
            .cluster
            .as_ref()
            .ok_or_else(|| "no cluster in this run".to_owned())?;
        let address = if node == Node::Leader {
            &cluster.leader
        } else {
            &cluster.follower
        };
        match &cluster.proxy_host {
            Some(proxy_host) => advertised(proxy_host, address),
            None => Ok(address.clone()),
        }
    }

    /// A setup call with the orchestrator's own client, so that a step that a
    /// test does not check never fails on a divergence of the SDK under test.
    async fn admin_operation(
        &self,
        operation: &Operation,
        args: Value,
        state: &mut TestState,
    ) -> Result<(), Step> {
        let method = Method::from_str(&operation.name)
            .map_err(|_| Step::Errored(format!("unknown method {}", operation.name)))?;
        method
            .check_args(&args)
            .map_err(|error| Step::Errored(format!("arguments: {error}")))?;
        let reply = self
            .admin
            .call(state.admin, &operation.name, args, None)
            .await;
        judge(
            method,
            EntityKind::Client,
            operation.expect_result.as_ref(),
            operation.expect_error.as_ref(),
            reply,
            &mut state.captures,
        )
    }

    /// The admin client of the deployment that a test uses: the cluster for a
    /// test with a client of a cluster node, the single node otherwise.
    fn admin_of(test: &TestCase) -> &'static str {
        let clustered = test
            .create_entities
            .iter()
            .any(|entity| entity.client.node.topology() == Topology::Cluster);
        if clustered { CLUSTER_ADMIN } else { ADMIN }
    }

    fn admin_entities(&self) -> Vec<&'static str> {
        if self.config.cluster.is_some() {
            vec![ADMIN, CLUSTER_ADMIN]
        } else {
            vec![ADMIN]
        }
    }

    async fn admin_call(
        &self,
        admin: &str,
        call: &AdminCall,
        captures: &mut Captures,
    ) -> Result<(), Step> {
        let method = Method::from_str(&call.name)
            .map_err(|_| Step::Errored(format!("unknown method {}", call.name)))?;
        let args = matcher::resolve_refs(&Value::Object(call.arguments.clone()), captures)
            .map_err(Step::Errored)?;
        method
            .check_args(&args)
            .map_err(|error| Step::Errored(format!("arguments: {error}")))?;
        let reply = self.admin.call(admin, &call.name, args, None).await;
        judge(
            method,
            EntityKind::Client,
            call.expect_result.as_ref(),
            None,
            reply,
            captures,
        )
    }

    /// Judges an answer, and in record mode prints it and keeps going.
    fn judge_recorded(
        &self,
        state: &mut TestState,
        method: Method,
        kind: EntityKind,
        operation: &Operation,
        reply: Reply,
        elapsed: Duration,
    ) -> Result<(), Step> {
        if self.config.record {
            let line = serde_json::json!({
                "test": state.test,
                "operation": state.operation,
                "call": format!("{}.{}", operation.object, operation.name),
                "elapsed_ms": u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
                "answer": to_value(&reply.clone().into_line(0)),
            });
            println!("{line}");
        }
        let judged = judge(
            method,
            kind,
            operation.expect_result.as_ref(),
            operation.expect_error.as_ref(),
            reply,
            &mut state.captures,
        );
        self.recorded(state, judged)
    }

    fn bounds_recorded(
        &self,
        state: &TestState,
        elapsed: Duration,
        operation: &Operation,
    ) -> Result<(), Step> {
        self.recorded(
            state,
            check_bounds(elapsed, operation.within, operation.not_before),
        )
    }

    fn recorded(&self, state: &TestState, outcome: Result<(), Step>) -> Result<(), Step> {
        match outcome {
            Err(Step::Failed(reason)) if self.config.record => {
                tracing::warn!("{} operation {}: {reason}", state.test, state.operation);
                Ok(())
            }
            other => other,
        }
    }

    /// A request whose timeout is a failure of a time bound, not of the agent.
    async fn timed_request(
        &mut self,
        request: Request,
        limit: Duration,
        within: Option<Duration>,
    ) -> Result<Reply, Step> {
        match self.request(request, limit).await {
            Err(Step::Agent(HostError::Timeout(_))) if within.is_some() => Err(Step::Failed(
                format!("no answer within {:?}", within.unwrap_or_default()),
            )),
            other => other,
        }
    }

    async fn request(&mut self, request: Request, limit: Duration) -> Result<Reply, Step> {
        let host = self
            .host
            .as_ref()
            .ok_or_else(|| Step::Errored("the agent is not running".to_owned()))?;
        host.request(request, limit).await.map_err(Step::Agent)
    }

    /// Resets the agent, and restarts it if the reset fails.
    async fn reset_agent(&mut self) {
        let Some(host) = self.host.as_ref() else {
            return;
        };
        match host.request(Request::Reset, RESET_TIMEOUT).await {
            Ok(Reply::Result(_)) => {}
            outcome => {
                tracing::warn!("The agent did not reset ({outcome:?}), so it restarts");
                if let Some(host) = self.host.take() {
                    host.stop().await;
                }
            }
        }
    }
}

#[derive(Default)]
struct TestState {
    test: String,
    /// The admin client of the deployment that the test uses.
    admin: &'static str,
    operation: usize,
    captures: Captures,
    entities: HashMap<String, EntityKind>,
    tasks: HashMap<String, Task>,
}

struct Task {
    method: Method,
    kind: EntityKind,
    started: Instant,
}

/// Why a test stopped before its end.
enum Step {
    Failed(String),
    Skipped(String),
    Errored(String),
    Agent(HostError),
}

impl Step {
    fn context(self, place: &str) -> Self {
        match self {
            Self::Failed(reason) => Self::Failed(format!("{place}: {reason}")),
            Self::Errored(reason) => Self::Errored(format!("{place}: {reason}")),
            Self::Agent(error) => Self::Errored(format!("{place}: agent: {error}")),
            Self::Skipped(reason) => Self::Skipped(reason),
        }
    }

    /// A failure of a setup step is an error of the test, not a failure.
    fn errored(self, place: &str) -> Self {
        match self {
            Self::Failed(reason) | Self::Errored(reason) => {
                Self::Errored(format!("{place}: {reason}"))
            }
            other => other.context(place),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitArgs {
    duration_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TaskArgs {
    task: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityArgs {
    entity: String,
}

/// Compares an answer with the expectation of its call.
fn judge(
    method: Method,
    kind: EntityKind,
    expect_result: Option<&Value>,
    expect_error: Option<&ErrorExpectation>,
    reply: Reply,
    captures: &mut Captures,
) -> Result<(), Step> {
    match reply {
        Reply::Unsupported(reason) => Err(Step::Skipped(format!("unsupported: {reason}"))),
        Reply::Failure(reason) => Err(Step::Errored(format!("agent failure: {reason}"))),
        Reply::Result(value) => {
            method.check_result(kind, &value).map_err(|error| {
                Step::Failed(format!(
                    "the result is not the canonical form of {}: {error}: {}",
                    method.name(),
                    compact(&value)
                ))
            })?;
            if let Some(expected) = expect_error {
                return Err(Step::Failed(format!(
                    "expected the error {}, got the result {}",
                    describe_expected_error(expected),
                    compact(&value)
                )));
            }
            if let Some(expected) = expect_result {
                matcher::matches(expected, &value, captures)
                    .map_err(|mismatch| Step::Failed(format!("result: {mismatch}")))?;
            }
            Ok(())
        }
        Reply::Error(error) => {
            let Some(expected) = expect_error else {
                return Err(Step::Failed(format!(
                    "unexpected error {}",
                    describe_error(&error)
                )));
            };
            let expected_code = expected
                .code
                .or_else(|| expected.name.as_deref().and_then(iggy_errors::code_of));
            if expected_code != Some(error.code) {
                return Err(Step::Failed(format!(
                    "expected the error {}, got {}",
                    describe_expected_error(expected),
                    describe_error(&error)
                )));
            }
            if let Some(operation_id) = &expected.operation_id {
                let actual = error.operation_id.map_or(Value::Null, Value::from);
                matcher::matches(operation_id, &actual, captures)
                    .map_err(|mismatch| Step::Failed(format!("error operation_id: {mismatch}")))?;
            }
            Ok(())
        }
    }
}

fn check_bounds(
    elapsed: Duration,
    within: Option<Duration>,
    not_before: Option<Duration>,
) -> Result<(), Step> {
    if let Some(within) = within
        && elapsed > within
    {
        return Err(Step::Failed(format!(
            "answered after {elapsed:?}, later than {within:?}"
        )));
    }
    if let Some(not_before) = not_before
        && elapsed < not_before
    {
        return Err(Step::Failed(format!(
            "answered after {elapsed:?}, earlier than {not_before:?}"
        )));
    }
    Ok(())
}

/// The proxy host with the port of a node address.
fn advertised(proxy_host: &str, address: &str) -> Result<String, String> {
    let (_, port) = address
        .rsplit_once(':')
        .ok_or_else(|| format!("{address} has no port"))?;
    Ok(format!("{proxy_host}:{port}"))
}

fn expect_null(reply: &Reply, what: &str) -> Result<(), Step> {
    match reply {
        Reply::Result(Value::Null) => Ok(()),
        other => Err(Step::Errored(format!("{what}: {}", describe_reply(other)))),
    }
}

fn decode<T: DeserializeOwned>(args: Value) -> Result<T, Step> {
    serde_json::from_value(args).map_err(|error| Step::Errored(format!("arguments: {error}")))
}

fn to_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or(Value::Null)
}

fn describe_error(error: &AgentError) -> String {
    let name = iggy_errors::name_of(error.code).unwrap_or("unknown");
    let mut text = format!("{} ({name})", error.code);
    if let Some(operation_id) = error.operation_id {
        text.push_str(&format!(" of operation {operation_id}"));
    }
    if let Some(message) = &error.message {
        text.push_str(&format!(": {message}"));
    }
    text
}

fn describe_expected_error(expected: &ErrorExpectation) -> String {
    match (&expected.name, expected.code) {
        (Some(name), _) => format!(
            "{} ({name})",
            iggy_errors::code_of(name).map_or_else(|| "?".to_owned(), |code| code.to_string())
        ),
        (None, Some(code)) => format!(
            "{code} ({})",
            iggy_errors::name_of(code).unwrap_or("unknown")
        ),
        (None, None) => "of any code".to_owned(),
    }
}

fn describe_reply(reply: &Reply) -> String {
    match reply {
        Reply::Result(value) => format!("the result {}", compact(value)),
        Reply::Error(error) => format!("the error {}", describe_error(error)),
        Reply::Unsupported(reason) => format!("unsupported: {reason}"),
        Reply::Failure(reason) => format!("agent failure: {reason}"),
    }
}

fn log_verdict(id: &str, verdict: &Verdict) {
    match verdict {
        Verdict::Passed => tracing::info!("PASS {id}"),
        Verdict::Failed(reason) => tracing::error!("FAIL {id}: {reason}"),
        Verdict::Skipped(reason) => tracing::info!("SKIP {id}: {reason}"),
        Verdict::Errored(reason) => tracing::error!("ERROR {id}: {reason}"),
    }
}
