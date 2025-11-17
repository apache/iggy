use iggy::prelude::*;
use iggy_common::{CREATE_TOPIC_CODE, IggyError, LOGIN_USER_CODE, PING_CODE};
use serde_json::Value;
use std::fmt::Display;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

const DEFAULT_ROOT_USERNAME: &str = "iggy";
const DEFAULT_ROOT_PASSWORD: &str = "iggy";

const SERVER_IP: &str = "127.0.0.1";
const SERVER_TCP_PORT: u16 = 8090;
const SERVER_CONNECT_TIMEOUT_MS: u64 = 5000;

const CAPTURE_START_WAIT_MS: u64 = 1000;
const OPERATION_WAIT_MS: u64 = 2000;
const CAPTURE_STOP_WAIT_MS: u64 = 500;

fn get_field<T>(iggy: &Value, field: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: Display,
{
    iggy.get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("{} field missing", field))?
        .parse::<T>()
        .map_err(|e| format!("Failed to parse {}: {}", field, e))
}

fn expect_field<T>(iggy: &Value, field: &str) -> T
where
    T: FromStr,
    T::Err: Display,
{
    get_field(iggy, field).unwrap_or_else(|e| panic!("{}", e))
}

fn status_code_to_name(status_code: u32) -> &'static str {
    if status_code == 0 {
        return "ok";
    }

    match IggyError::from_repr(status_code) {
        Some(error) => error.into(),
        None => "unknown",
    }
}

fn check_tshark_available() -> bool {
    ProcessCommand::new("tshark")
        .arg("--version")
        .output()
        .is_ok()
}

struct TsharkCapture {
    ip: String,
    port: u16,
    pcap_file: PathBuf,
    tshark_process: Option<Child>,
}

impl TsharkCapture {
    fn new(ip: &str, port: u16) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let pid = std::process::id();

        let file = format!("/tmp/iggy_test_{}_{}.pcap", timestamp, pid);
        let pcap_file = PathBuf::from(file);

        Self {
            ip: ip.to_string(),
            port,
            pcap_file,
            tshark_process: None,
        }
    }

    fn capture(&mut self) -> io::Result<()> {
        let dissector_path = std::env::current_dir()?.join("dissector.lua");

        if !dissector_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Dissector not found at: {}", dissector_path.display()),
            ));
        }

        let lua_script_arg = format!("lua_script:{}", dissector_path.display());
        let port_config_arg = format!("iggy.server_port:{}", self.port);
        let filter_arg = format!("tcp and host {} and port {}", self.ip, self.port);

        let mut command = ProcessCommand::new("tshark");

        command
            .args([
                "-i",
                "lo",
                "-w",
                self.pcap_file.to_str().unwrap(),
                "-f",
                &filter_arg,
                "-X",
                &lua_script_arg,
                "-o",
                &port_config_arg,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        let process = command.spawn()?;
        self.tshark_process = Some(process);
        Ok(())
    }

    fn stop(&mut self) {
        if let Some(mut process) = self.tshark_process.take() {
            let _ = process.kill();
        }
    }

    fn analyze(&self) -> io::Result<Vec<Value>> {
        let dissector_path = std::env::current_dir()?.join("dissector.lua");

        if !dissector_path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Dissector not found at: {}", dissector_path.display()),
            ));
        }

        let lua_script_arg = format!("lua_script:{}", dissector_path.display());
        let port_config_arg = format!("iggy.server_port:{}", self.port);

        let output = ProcessCommand::new("tshark")
            .args([
                "-r",
                self.pcap_file.to_str().unwrap(),
                "-Y",
                "iggy",
                "-T",
                "json",
                "-X",
                &lua_script_arg,
                "-o",
                &port_config_arg,
                "-V",
            ])
            .output()?;

        if !output.status.success() {
            return Err(io::Error::other(format!(
                "tshark failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let json_str = String::from_utf8_lossy(&output.stdout);

        if json_str.trim().is_empty() {
            return Ok(Vec::new());
        }

        let packets: Vec<Value> =
            serde_json::from_str(&json_str).map_err(io::Error::other)?;

        Ok(packets)
    }
}

impl Drop for TsharkCapture {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_file(&self.pcap_file) {
            eprintln!(
                "Warning: Failed to remove pcap file {}: {}",
                self.pcap_file.display(),
                e
            );
        }
    }
}

struct TestFixture {
    capture: TsharkCapture,
    client: IggyClient,
}

impl TestFixture {
    fn new() -> Self {
        if !check_tshark_available() {
            panic!("tshark is required but not available.");
        }

        let capture = TsharkCapture::new(SERVER_IP, SERVER_TCP_PORT);

        let tcp_config = TcpClientConfig {
            server_address: format!("{}:{}", SERVER_IP, SERVER_TCP_PORT),
            ..Default::default()
        };

        let tcp_client =
            TcpClient::create(Arc::new(tcp_config)).expect("Failed to create TCP client");
        let client = IggyClient::new(ClientWrapper::Tcp(tcp_client));

        Self { capture, client }
    }

    async fn start(&mut self, login: bool) -> Result<(), Box<dyn std::error::Error>> {
        self.capture.capture()?;
        sleep(Duration::from_millis(CAPTURE_START_WAIT_MS)).await;

        tokio::time::timeout(
            Duration::from_millis(SERVER_CONNECT_TIMEOUT_MS),
            self.client.connect(),
        )
        .await??;

        if login {
            self.client
                .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
                .await?;
        }

        Ok(())
    }

    async fn stop_and_analyze(&mut self) -> Result<Vec<Value>, Box<dyn std::error::Error>> {
        sleep(Duration::from_millis(OPERATION_WAIT_MS)).await;

        self.client.disconnect().await?;

        self.capture.stop();
        sleep(Duration::from_millis(CAPTURE_STOP_WAIT_MS)).await;

        let packets = self.capture.analyze()?;
        let iggy_layers = extract_iggy_layers(&packets);

        print_packet_json(&packets, false);

        if iggy_layers.is_empty() {
            return Err("No Iggy packets captured".into());
        }

        Ok(packets)
    }
}

fn extract_iggy_layers(packets: &[Value]) -> Vec<&Value> {
    packets
        .iter()
        .filter_map(|p| p["_source"]["layers"].get("iggy"))
        .collect()
}

fn find_request_response<'a>(
    iggy_layers: &[&'a Value],
    command_id: u32,
) -> Result<(&'a Value, &'a Value), Box<dyn std::error::Error>> {
    let req_idx = iggy_layers
        .iter()
        .position(|iggy| {
            get_field::<u32>(iggy, "iggy.request.command")
                .ok()
                .map(|command| command == command_id)
                .unwrap_or(false)
        })
        .ok_or_else(|| format!("Request with command {} not found in capture", command_id))?;

    let req = iggy_layers[req_idx];

    let command_name: String = get_field(req, "iggy.request.command_name")
        .map_err(|e| format!("Failed to get command name from request: {}", e))?;

    let resp = iggy_layers[req_idx + 1..]
        .iter()
        .find(|iggy| {
            get_field::<String>(iggy, "iggy.request.command_name")
                .ok()
                .map(|name| name == command_name)
                .unwrap_or(false)
                && iggy.get("iggy.response.status").is_some()
        })
        .copied()
        .ok_or_else(|| {
            format!(
                "Response for command {} ({}) not found in capture",
                command_id, command_name
            )
        })?;

    Ok((req, resp))
}

fn verify_request_packet(iggy: &Value, expected_length: u32) {
    let command: u32 = expect_field(iggy, "iggy.request.command");

    let expected_cmd_name = iggy_common::get_name_from_code(command)
        .unwrap_or_else(|_| panic!("Invalid command code: {}", command));

    let cmd_name: String = expect_field(iggy, "iggy.request.command_name");
    assert_eq!(cmd_name, expected_cmd_name);

    let length: u32 = expect_field(iggy, "iggy.request.length");
    assert_eq!(length, expected_length);
}

fn verify_response_packet(iggy: &Value, expected_status_code: u32, expected_length: u32) {
    let _cmd_name: String = expect_field(iggy, "iggy.request.command_name");

    let status: u32 = expect_field(iggy, "iggy.response.status");
    assert_eq!(status, expected_status_code);

    let status_name: String = expect_field(iggy, "iggy.response.status_name");
    let expected_status_name = status_code_to_name(expected_status_code);
    assert_eq!(status_name, expected_status_name);

    let length: u32 = expect_field(iggy, "iggy.response.length");
    assert_eq!(length, expected_length);
}

fn get_request_payload(packet: &Value) -> Option<&Value> {
    packet.get("iggy.request.payload_tree")
}

fn get_response_payload(packet: &Value) -> Option<&Value> {
    packet.get("iggy.response.payload_tree")
}

fn print_packet_json(packets: &[Value], show_all: bool) {
    println!("\n=== Captured Packets JSON ===");

    for (idx, packet) in packets.iter().enumerate() {
        println!("\n--- Packet {} ---", idx);

        if show_all {
            let json = serde_json::to_string_pretty(packet)
                .unwrap_or_else(|_| "Error serializing packet".to_string());
            println!("{}", json);
        } else if let Some(iggy) = packet["_source"]["layers"].get("iggy") {
            println!("\nIggy Protocol:");
            let json = serde_json::to_string_pretty(iggy).unwrap_or_else(|_| "Error".to_string());
            println!("{}", json);
        }
    }
    println!("\n=== End of Packets ===\n");
}

#[tokio::test]
async fn test_ping_dissection() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = TestFixture::new();
    fixture.start(true).await?;

    fixture.client.ping().await?;

    let packets = fixture.stop_and_analyze().await?;
    let iggy_layers = extract_iggy_layers(&packets);

    let (req, resp) = find_request_response(&iggy_layers, PING_CODE)?;

    verify_request_packet(req, 4);
    assert!(
        get_request_payload(req).is_none(),
        "Ping request should not have payload"
    );

    verify_response_packet(resp, 0, 0);
    assert!(
        get_response_payload(resp).is_none(),
        "Ping response should not have payload"
    );

    Ok(())
}

#[tokio::test]
async fn test_login_user_dissection() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = TestFixture::new();
    fixture.start(false).await?;

    fixture
        .client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await?;

    let packets = fixture.stop_and_analyze().await?;
    let iggy_layers = extract_iggy_layers(&packets);

    let (req, resp) = find_request_response(&iggy_layers, LOGIN_USER_CODE)?;

    verify_request_packet(req, 27);

    let req_payload = get_request_payload(req).expect("LoginUser request should have payload");

    let username: String = expect_field(req_payload, "iggy.login_user.req.username");
    assert_eq!(username, DEFAULT_ROOT_USERNAME);

    let username_len: u32 = expect_field(req_payload, "iggy.login_user.req.username_len");
    assert_eq!(username_len, DEFAULT_ROOT_USERNAME.len() as u32);

    let password_len: u32 = expect_field(req_payload, "iggy.login_user.req.password_len");
    assert_eq!(password_len, DEFAULT_ROOT_PASSWORD.len() as u32);

    verify_response_packet(resp, 0, 4);

    let resp_payload = get_response_payload(resp).expect("LoginUser response should have payload");

    let user_id: u32 = expect_field(resp_payload, "iggy.login_user.resp.user_id");
    assert!(user_id > 0, "User ID should be greater than 0");

    Ok(())
}

#[tokio::test]
async fn test_create_topic_dissection() -> Result<(), Box<dyn std::error::Error>> {
    let mut fixture = TestFixture::new();
    fixture.start(true).await?;

    let stream_name = "test_create_topic_stream";
    let topic_name = "test_create_topic";

    let _ = fixture
        .client
        .delete_stream(&Identifier::named(stream_name)?)
        .await;

    fixture.client.create_stream(stream_name, None).await?;

    let partitions_count = 3u32;

    fixture
        .client
        .create_topic(
            &Identifier::named(stream_name)?,
            topic_name,
            partitions_count,
            CompressionAlgorithm::None,
            None,
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await?;

    fixture
        .client
        .delete_stream(&Identifier::named(stream_name)?)
        .await?;

    let packets = fixture.stop_and_analyze().await?;
    let iggy_layers = extract_iggy_layers(&packets);

    let (req, resp) = find_request_response(&iggy_layers, CREATE_TOPIC_CODE)?;

    verify_request_packet(req, 74);

    let req_payload = get_request_payload(req).expect("CreateTopic request should have payload");

    let req_stream_id_kind: u32 = expect_field(req_payload, "iggy.create_topic.req.stream_id_kind");
    assert_eq!(req_stream_id_kind, IdKind::String.as_code() as u32);

    let req_stream_id_length: u32 =
        expect_field(req_payload, "iggy.create_topic.req.stream_id_length");
    assert_eq!(req_stream_id_length, stream_name.len() as u32);

    let req_stream_id_value: String =
        expect_field(req_payload, "iggy.create_topic.req.stream_id_value_string");
    assert_eq!(req_stream_id_value, stream_name);

    let name: String = expect_field(req_payload, "iggy.create_topic.req.name");
    assert_eq!(name, topic_name);

    let name_len: u32 = expect_field(req_payload, "iggy.create_topic.req.name_len");
    assert_eq!(name_len, topic_name.len() as u32);

    let partitions: u32 = expect_field(req_payload, "iggy.create_topic.req.partitions_count");
    assert_eq!(partitions, partitions_count);

    verify_response_packet(resp, 0, 188);

    let resp_payload =
        get_response_payload(resp).expect("CreateTopic response should have payload");

    let resp_topic_id: u32 = expect_field(resp_payload, "iggy.create_topic.resp.topic_id");
    assert!(resp_topic_id > 0, "Topic ID should be greater than 0");

    let resp_name: String = expect_field(resp_payload, "iggy.create_topic.resp.name");
    assert_eq!(resp_name, topic_name);

    let resp_name_len: u32 = expect_field(resp_payload, "iggy.create_topic.resp.name_len");
    assert_eq!(resp_name_len, topic_name.len() as u32);

    let resp_partitions: u32 =
        expect_field(resp_payload, "iggy.create_topic.resp.partitions_count");
    assert_eq!(resp_partitions, partitions_count);

    let resp_messages_count: u32 =
        expect_field(resp_payload, "iggy.create_topic.resp.messages_count");
    assert_eq!(resp_messages_count, 0);

    let resp_size: u32 = expect_field(resp_payload, "iggy.create_topic.resp.size");
    assert_eq!(resp_size, 0);

    Ok(())
}
