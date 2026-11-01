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

use super::client::{ConnectorEndpoint, ConnectorKind, ConnectorsApiClient};
use super::config::{
    ActivateConnectorConfigCmd, CreateConnectorConfigCmd, DeleteConnectorConfigCmd,
    GetConnectorConfigCmd, GetConnectorConfigsCmd, GetConnectorPluginConfigCmd,
};
use super::get_connector::GetConnectorCmd;
use super::get_connectors::{GetConnectorsCmd, GetConnectorsOutput};
use super::get_transforms::GetTransformsCmd;
use super::restart_connector::RestartConnectorCmd;
use super::stats::GetConnectorStatsCmd;
use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use iggy::prelude::IggyClient;
use serde_json::json;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};
use wiremock::matchers::{any, body_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

const KEY: &str = "secret";
// Self-signed CA made only for these tests, with no private key kept. It is one
// unbroken line because the spell checker skips long base64 and flags short pieces.
const TEST_CA_BODY: &str = "MIIBazCCARKgAwIBAgIUe2bB2ILhuNKOdaNp9/h6Y7rMIpkwCgYIKoZIzj0EAwIwGzEZMBcGA1UEAwwQaWdneS1jbGktdGVzdC1jYTAgFw0yNjExMDEwNjM5NTNaGA8yMTI2MTAwODA2Mzk1M1owGzEZMBcGA1UEAwwQaWdneS1jbGktdGVzdC1jYTBZMBMGByqGSM49AgEGCCqGSM49AwEHA0IABEmnLeGoD/oUMV8sLQr6V4/Xz0Yq6Peg4JeBIXUgnTLFhWbyBMIpLxV5IWh2H1Z8eNaqYg/NYDqLXO8HJrI6ddejMjAwMB0GA1UdDgQWBBRzQ40qz05dOpEsCRyhp5OfDQQeAjAPBgNVHRMBAf8EBTADAQH/MAoGCCqGSM49BAMCA0cAMEQCID4vbL30w6iHqmHFCNbzHsV+uZwhSS5QGnKKkpC29jatAiBVcPyW5Aq6E6uN8jwj/YQhVbcWCLdFDqVJhLLpzKYqMg==";

fn test_ca_pem() -> String {
    format!("-----BEGIN CERTIFICATE-----\n{TEST_CA_BODY}\n-----END CERTIFICATE-----\n")
}

/// Collects what a command prints, which goes through tracing events on `PRINT_TARGET`.
#[derive(Clone, Default)]
struct Printed(Arc<Mutex<Vec<String>>>);

struct Message(String);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl Subscriber for Printed {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target() == PRINT_TARGET
    }

    fn new_span(&self, _attributes: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut message = Message(String::new());
        event.record(&mut message);
        self.0.lock().unwrap().push(message.0);
    }

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}
}

fn endpoint(server: &MockServer) -> ConnectorEndpoint {
    ConnectorEndpoint {
        api_url: Some(server.uri()),
        api_key: Some(KEY.to_string()),
        tls_ca_file: None,
    }
}

/// Runs a command to the end and returns its result with everything it printed.
async fn run(command: &mut dyn CliCommand) -> (anyhow::Result<()>, String) {
    let printed = Printed::default();
    let _guard = tracing::subscriber::set_default(printed.clone());
    let result = command.execute_cmd(&IggyClient::default()).await;
    let output = printed.0.lock().unwrap().join("\n");
    (result, output)
}

/// Mounts one expected request that must carry the key and be hit exactly once.
async fn expect(server: &MockServer, verb: &str, route: &str, response: ResponseTemplate) {
    Mock::given(method(verb))
        .and(path(route))
        .and(header("api-key", KEY))
        .respond_with(response)
        .expect(1)
        .mount(server)
        .await;
}

fn ok(body: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(body)
}

#[tokio::test]
async fn should_list_sinks_as_table() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/sinks",
        ok(json!([{ "key": "orders-sink", "name": "Orders", "status": "running", "enabled": true }])),
    )
    .await;
    let mut command = GetConnectorsCmd::new(
        ConnectorKind::Sinks,
        GetConnectorsOutput::Table,
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert!(output.contains("Key") && output.contains("Status"));
    assert!(output.contains("orders-sink") && output.contains("running"));
}

#[tokio::test]
async fn should_list_sources_in_list_mode() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/sources",
        ok(json!([{ "key": "orders-source", "name": "Orders", "status": "stopped", "enabled": false }])),
    )
    .await;
    let mut command = GetConnectorsCmd::new(
        ConnectorKind::Sources,
        GetConnectorsOutput::List,
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert_eq!(output, "orders-source|Orders|stopped|false");
}

#[tokio::test]
async fn should_report_no_connectors_when_list_is_empty() {
    let server = MockServer::start().await;
    expect(&server, "GET", "/sinks", ok(json!([]))).await;
    let mut command = GetConnectorsCmd::new(
        ConnectorKind::Sinks,
        GetConnectorsOutput::Table,
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert_eq!(output, "No sinks found!");
}

#[tokio::test]
async fn should_show_connector_details_as_property_table() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/sinks/orders-sink",
        ok(json!({ "key": "orders-sink", "enabled": true, "last_error": null })),
    )
    .await;
    let mut command = GetConnectorCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert!(output.contains("Property") && output.contains("Value"));
    assert!(output.contains("enabled") && output.contains("true"));
}

#[tokio::test]
async fn should_show_transforms() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/sinks/orders-sink/transforms",
        ok(json!([{ "type": "add_fields", "config": { "fields": [] } }])),
    )
    .await;
    let mut command = GetTransformsCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert!(output.contains("add_fields") && output.contains(r#"{"fields":[]}"#));
}

#[tokio::test]
async fn should_report_no_transforms_when_there_are_none() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/sources/orders-source/transforms",
        ok(json!([])),
    )
    .await;
    let mut command = GetTransformsCmd::new(
        ConnectorKind::Sources,
        "orders-source".into(),
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert_eq!(output, "No transforms found for source orders-source!");
}

#[tokio::test]
async fn should_restart_connector() {
    let server = MockServer::start().await;
    expect(
        &server,
        "POST",
        "/sinks/orders-sink/restart",
        ResponseTemplate::new(204),
    )
    .await;
    let mut command = RestartConnectorCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert_eq!(output, "sink orders-sink restarted successfully");
}

#[tokio::test]
async fn should_show_runtime_and_connector_stats() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/stats",
        ok(json!({
            "connectors_runtime_version": "0.1.0",
            "sinks_total": 1,
            "sinks_running": 1,
            "connectors": [{
                "key": "orders-sink", "connector_type": "sink", "status": "running",
                "enabled": true, "messages_sent": 7, "errors": 0
            }]
        })),
    )
    .await;
    let mut command = GetConnectorStatsCmd::new(endpoint(&server));

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert!(output.contains("connectors_runtime_version") && output.contains("0.1.0"));
    assert!(output.contains("sinks_running"));
    assert!(output.contains("orders-sink") && output.contains("Errors"));
}

#[tokio::test]
async fn should_show_runtime_stats_without_connector_table() {
    let server = MockServer::start().await;
    expect(&server, "GET", "/stats", ok(json!({ "process_id": 42 }))).await;
    let mut command = GetConnectorStatsCmd::new(endpoint(&server));

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert!(output.contains("process_id") && !output.contains("Produced"));
}

#[tokio::test]
async fn should_list_config_versions() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/sinks/orders-sink/configs",
        ok(json!([
            { "version": 1, "active": false, "enabled": true, "name": "old" },
            { "version": 2, "active": true, "enabled": true, "name": "new" }
        ])),
    )
    .await;
    let mut command = GetConnectorConfigsCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert!(output.contains("Version") && output.contains("Active"));
    assert!(output.contains("old") && output.contains("new"));
}

#[tokio::test]
async fn should_report_no_configs_when_there_are_none() {
    let server = MockServer::start().await;
    expect(&server, "GET", "/sinks/orders-sink/configs", ok(json!([]))).await;
    let mut command = GetConnectorConfigsCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert_eq!(output, "No configurations found for sink orders-sink!");
}

#[tokio::test]
async fn should_get_active_config_when_no_version_is_given() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/sinks/orders-sink/configs/active",
        ok(json!({ "version": 2 })),
    )
    .await;
    let mut command = GetConnectorConfigCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        None,
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert_eq!(
        output,
        serde_json::to_string_pretty(&json!({ "version": 2 })).unwrap()
    );
}

#[tokio::test]
async fn should_get_the_requested_config_version() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/sources/orders-source/configs/3",
        ok(json!({ "version": 3 })),
    )
    .await;
    let mut command = GetConnectorConfigCmd::new(
        ConnectorKind::Sources,
        "orders-source".into(),
        Some(3),
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert!(output.contains("\"version\": 3"));
}

#[tokio::test]
async fn should_get_plugin_config_in_the_requested_format() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/sinks/orders-sink/configs/plugin"))
        .and(query_param("format", "toml"))
        .and(header("api-key", KEY))
        .respond_with(ResponseTemplate::new(200).set_body_string("url = \"db\""))
        .expect(1)
        .mount(&server)
        .await;
    let mut command = GetConnectorPluginConfigCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        Some("toml".into()),
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert_eq!(output, "url = \"db\"");
}

#[tokio::test]
async fn should_create_config_from_file() {
    let server = MockServer::start().await;
    let body = json!({ "enabled": true, "name": "Orders" });
    Mock::given(method("POST"))
        .and(path("/sinks/orders-sink/configs"))
        .and(header("api-key", KEY))
        .and(body_json(&body))
        .respond_with(ok(json!({ "version": 4 })))
        .expect(1)
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("config.json");
    std::fs::write(&file, body.to_string()).unwrap();
    let mut command = CreateConnectorConfigCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        Some(file),
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert!(output.contains("\"version\": 4"));
}

#[tokio::test]
async fn should_not_send_anything_when_config_file_is_not_json() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("config.json");
    std::fs::write(&file, "{not json").unwrap();
    let mut command = CreateConnectorConfigCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        Some(file),
        endpoint(&server),
    );

    let (result, _) = run(&mut command).await;

    assert!(format!("{:#}", result.unwrap_err()).contains("problem parsing configuration as JSON"));
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn should_fail_when_config_file_is_missing() {
    let server = MockServer::start().await;
    let mut command = CreateConnectorConfigCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        Some("/no/such/config.json".into()),
        endpoint(&server),
    );

    let (result, _) = run(&mut command).await;

    assert!(format!("{:#}", result.unwrap_err()).contains("problem reading configuration file"));
}

#[tokio::test]
async fn should_delete_the_requested_config_version() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/sinks/orders-sink/configs"))
        .and(query_param("version", "3"))
        .and(header("api-key", KEY))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let mut command = DeleteConnectorConfigCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        3,
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert_eq!(
        output,
        "configuration version 3 deleted successfully for sink orders-sink"
    );
}

#[tokio::test]
async fn should_activate_config_version() {
    let server = MockServer::start().await;
    Mock::given(method("PUT"))
        .and(path("/sources/orders-source/configs/active"))
        .and(header("api-key", KEY))
        .and(body_json(json!({ "version": 2 })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;
    let mut command = ActivateConnectorConfigCmd::new(
        ConnectorKind::Sources,
        "orders-source".into(),
        2,
        endpoint(&server),
    );

    let (result, output) = run(&mut command).await;

    result.unwrap();
    assert!(output.contains("marked active") && output.contains("keeps its configuration"));
}

#[tokio::test]
async fn should_show_server_code_and_reason_on_error() {
    let server = MockServer::start().await;
    expect(
        &server,
        "GET",
        "/sinks/missing",
        ResponseTemplate::new(404)
            .set_body_json(json!({ "code": "sink_not_found", "reason": "no sink missing" })),
    )
    .await;
    let mut command =
        GetConnectorCmd::new(ConnectorKind::Sinks, "missing".into(), endpoint(&server));

    let (result, _) = run(&mut command).await;

    let error = format!("{:#}", result.unwrap_err());
    assert!(error.contains("sink_not_found") && error.contains("no sink missing"));
}

#[tokio::test]
async fn should_show_status_and_body_when_error_is_not_json() {
    let server = MockServer::start().await;
    expect(
        &server,
        "POST",
        "/sinks/orders-sink/restart",
        ResponseTemplate::new(500).set_body_string("boom"),
    )
    .await;
    let mut command = RestartConnectorCmd::new(
        ConnectorKind::Sinks,
        "orders-sink".into(),
        endpoint(&server),
    );

    let (result, _) = run(&mut command).await;

    let error = format!("{:#}", result.unwrap_err());
    assert!(error.contains("500") && error.contains("boom"));
}

#[tokio::test]
async fn should_fail_on_wrong_key_without_asking_again() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/stats"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&server)
        .await;
    let mut command = GetConnectorStatsCmd::new(endpoint(&server));

    let (result, _) = run(&mut command).await;

    assert!(format!("{:#}", result.unwrap_err()).contains("401"));
}

#[tokio::test]
async fn should_send_no_key_header_when_none_is_configured() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/stats"))
        .respond_with(ok(json!({})))
        .expect(1)
        .mount(&server)
        .await;
    let mut command = GetConnectorStatsCmd::new(ConnectorEndpoint {
        api_url: Some(server.uri()),
        api_key: None,
        tls_ca_file: None,
    });

    let (result, _) = run(&mut command).await;

    result.unwrap();
    let requests = server.received_requests().await.unwrap();
    assert!(!requests[0].headers.contains_key("api-key"));
}

#[tokio::test]
async fn should_not_follow_redirects_with_the_key() {
    let elsewhere = MockServer::start().await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/stats"))
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", format!("{}/stats", elsewhere.uri()).as_str()),
        )
        .expect(1)
        .mount(&server)
        .await;
    let mut command = GetConnectorStatsCmd::new(endpoint(&server));

    let (result, _) = run(&mut command).await;

    assert!(format!("{:#}", result.unwrap_err()).contains("302"));
    assert!(elsewhere.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn should_fail_when_runtime_is_unreachable() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let mut command = GetConnectorStatsCmd::new(ConnectorEndpoint {
        api_url: Some(url),
        api_key: None,
        tls_ca_file: None,
    });

    let (result, _) = run(&mut command).await;

    assert!(format!("{:#}", result.unwrap_err()).contains("problem calling connectors API"));
}

#[test]
fn should_fail_when_ca_file_is_missing() {
    let error = ConnectorsApiClient::new(None, None, Some("/no/such/ca.pem".into()))
        .err()
        .unwrap();

    assert!(format!("{error:#}").contains("problem reading CA file"));
}

fn client_with_ca(content: &str) -> anyhow::Result<ConnectorsApiClient> {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("ca.pem");
    std::fs::write(&file, content).unwrap();
    ConnectorsApiClient::new(None, None, Some(file))
}

#[test]
fn should_accept_a_valid_ca_file() {
    client_with_ca(&test_ca_pem()).unwrap();
}

#[test]
fn should_accept_a_ca_bundle() {
    client_with_ca(&format!("{0}{0}", test_ca_pem())).unwrap();
}

#[test]
fn should_reject_a_ca_file_without_certificates() {
    let error = client_with_ca("not a certificate").err().unwrap();

    assert!(format!("{error:#}").contains("contains no certificate"));
}

#[test]
fn should_reject_a_malformed_ca_file() {
    let error = client_with_ca("-----BEGIN CERTIFICATE-----\n!!!\n-----END CERTIFICATE-----\n")
        .err()
        .unwrap();

    assert!(format!("{error:#}").contains("CA file"));
}

#[test]
fn should_keep_stdout_clean_only_for_raw_output_commands() {
    let endpoint = || ConnectorEndpoint {
        api_url: None,
        api_key: None,
        tls_ca_file: None,
    };
    let kind = ConnectorKind::Sinks;
    let key = || "orders-sink".to_string();

    assert!(GetConnectorConfigCmd::new(kind, key(), None, endpoint()).stdout_is_data());
    assert!(GetConnectorPluginConfigCmd::new(kind, key(), None, endpoint()).stdout_is_data());
    assert!(CreateConnectorConfigCmd::new(kind, key(), None, endpoint()).stdout_is_data());
    assert!(!GetConnectorConfigsCmd::new(kind, key(), endpoint()).stdout_is_data());
    assert!(!DeleteConnectorConfigCmd::new(kind, key(), 1, endpoint()).stdout_is_data());
    assert!(!GetConnectorStatsCmd::new(endpoint()).stdout_is_data());
}

/// Every connector command, in the order of the CLI, all pointing at one endpoint.
fn all_commands(server: &MockServer, config_file: PathBuf) -> Vec<Box<dyn CliCommand>> {
    let kind = ConnectorKind::Sinks;
    let key = || "orders-sink".to_string();
    vec![
        Box::new(GetConnectorsCmd::new(
            kind,
            GetConnectorsOutput::Table,
            endpoint(server),
        )),
        Box::new(GetConnectorCmd::new(kind, key(), endpoint(server))),
        Box::new(GetTransformsCmd::new(kind, key(), endpoint(server))),
        Box::new(RestartConnectorCmd::new(kind, key(), endpoint(server))),
        Box::new(GetConnectorStatsCmd::new(endpoint(server))),
        Box::new(GetConnectorConfigsCmd::new(kind, key(), endpoint(server))),
        Box::new(GetConnectorConfigCmd::new(
            kind,
            key(),
            None,
            endpoint(server),
        )),
        Box::new(GetConnectorPluginConfigCmd::new(
            kind,
            key(),
            None,
            endpoint(server),
        )),
        Box::new(CreateConnectorConfigCmd::new(
            kind,
            key(),
            Some(config_file),
            endpoint(server),
        )),
        Box::new(DeleteConnectorConfigCmd::new(
            kind,
            key(),
            1,
            endpoint(server),
        )),
        Box::new(ActivateConnectorConfigCmd::new(
            kind,
            key(),
            1,
            endpoint(server),
        )),
    ]
}

#[tokio::test]
async fn should_not_need_the_iggy_server_for_any_connector_command() {
    let server = MockServer::start().await;

    for command in all_commands(&server, PathBuf::new()) {
        assert!(!command.login_required(), "{}", command.explain());
        assert!(!command.connection_required(), "{}", command.explain());
    }
}

#[tokio::test]
async fn should_say_which_command_failed_when_the_runtime_errors() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("config.json");
    std::fs::write(&file, "{}").unwrap();

    for mut command in all_commands(&server, file) {
        let explained = command.explain();
        let (result, _) = run(command.as_mut()).await;

        let error = format!("{:#}", result.unwrap_err());
        assert!(
            error.starts_with("problem ") && error.contains("500") && error.contains("boom"),
            "{explained}: {error}"
        );
    }
}
