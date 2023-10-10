pub mod http;
pub mod quic;
pub mod scenarios;
pub mod tcp;

use assert_cmd::prelude::CommandCargoExt;
use async_trait::async_trait;
use iggy::client::{Client, UserClient};
use iggy::clients::client::IggyClient;
use iggy::models::permissions::{GlobalPermissions, Permissions};
use iggy::models::user_status::UserStatus::Active;
use iggy::users::create_user::CreateUser;
use iggy::users::defaults::*;
use iggy::users::login_user::LoginUser;
use std::collections::HashMap;
use std::fs;
use std::process::{Child, Command, Stdio};
use std::thread::{panicking, sleep};
use std::time::Duration;
use uuid::Uuid;

const SYSTEM_PATH_ENV_VAR: &str = "IGGY_SYSTEM_PATH";
const USER_PASSWORD: &str = "secret";

#[async_trait]
pub trait ClientFactory: Sync + Send {
    async fn create_client(&self) -> Box<dyn Client>;
}

pub struct TestServer {
    files_path: String,
    envs: Option<HashMap<String, String>>,
    child_handle: Option<Child>,
    stdout: String,
    stderr: String,
}

impl TestServer {
    pub fn new(envs: Option<HashMap<String, String>>) -> Self {
        Self::create(TestServer::get_random_path(), envs)
    }

    pub fn create(files_path: String, envs: Option<HashMap<String, String>>) -> Self {
        Self {
            files_path,
            envs,
            child_handle: None,
            stdout: String::new(),
            stderr: String::new(),
        }
    }

    pub fn start(&mut self) {
        // Sleep before starting server - it takes some time for the OS to release the port
        let duration = if std::env::var("IGGY_CI_BUILD").is_ok() {
            Duration::from_secs(5)
        } else {
            Duration::from_secs(1)
        };
        sleep(duration);

        self.cleanup();
        let files_path = self.files_path.clone();
        let mut command = Command::cargo_bin("iggy-server").unwrap();
        command.env(SYSTEM_PATH_ENV_VAR, files_path.clone());
        if let Some(env) = &self.envs {
            command.envs(env);
        }

        // When running action from github CI, binary needs to be started via QEMU.
        if let Ok(runner) = std::env::var("QEMU_RUNNER") {
            let mut runner_command = Command::new(runner);
            runner_command
                .arg(command.get_program().to_str().unwrap())
                .env(SYSTEM_PATH_ENV_VAR, files_path);
            if let Some(env) = &self.envs {
                runner_command.envs(env);
            }
            command = runner_command;
        };

        // By default, server all logs are redirected to local variable
        // and dumped to stderr when test fails. With IGGY_TEST_VERBOSE=1
        // logs are dumped to stdout during test execution.
        if std::env::var("IGGY_TEST_VERBOSE").is_err() {
            command.stdout(Stdio::piped());
            command.stderr(Stdio::piped());
        }

        self.child_handle = Some(command.spawn().unwrap());

        // Sleep after starting server - it needs some time to bind to given port and start listening
        let duration = if cfg!(any(
            target = "aarch64-unknown-linux-musl",
            target = "arm-unknown-linux-musleabi"
        )) {
            Duration::from_secs(40)
        } else if std::env::var("IGGY_CI_BUILD").is_ok() {
            Duration::from_secs(5)
        } else {
            Duration::from_secs(1)
        };
        sleep(duration);
    }

    pub fn stop(&mut self) {
        #[allow(unused_mut)]
        if let Some(mut child_handle) = self.child_handle.take() {
            #[cfg(unix)]
            unsafe {
                use libc::kill;
                use libc::SIGTERM;
                kill(child_handle.id() as libc::pid_t, SIGTERM);
            }

            #[cfg(not(unix))]
            child_handle.kill().unwrap();

            if let Ok(output) = child_handle.wait_with_output() {
                self.stdout
                    .push_str(String::from_utf8_lossy(&output.stdout).to_string().as_str());
                self.stderr
                    .push_str(String::from_utf8_lossy(&output.stderr).to_string().as_str());
            }
        }
        self.cleanup();
    }

    fn cleanup(&self) {
        if fs::metadata(&self.files_path).is_ok() {
            fs::remove_dir_all(&self.files_path).unwrap();
        }
    }

    pub fn get_random_path() -> String {
        format!("local_data_{}", Uuid::new_v4().to_u128_le())
    }
}
impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
        if panicking() {
            if !self.stdout.is_empty() {
                eprintln!("Iggy server stdout:\n{}", self.stdout);
            }
            if !self.stderr.is_empty() {
                eprintln!("Iggy server stderr:\n{}", self.stderr);
            }
        }
    }
}

impl Default for TestServer {
    fn default() -> Self {
        TestServer::new(None)
    }
}

async fn create_user(client: &IggyClient, username: &str) {
    client
        .create_user(&CreateUser {
            username: username.to_string(),
            password: USER_PASSWORD.to_string(),
            status: Active,
            permissions: Some(Permissions {
                global: GlobalPermissions {
                    manage_servers: true,
                    read_servers: true,
                    manage_users: true,
                    read_users: true,
                    manage_streams: true,
                    read_streams: true,
                    manage_topics: true,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                },
                streams: None,
            }),
        })
        .await
        .unwrap();
}

async fn login_root(client: &IggyClient) {
    client
        .login_user(&LoginUser {
            username: DEFAULT_ROOT_USERNAME.to_string(),
            password: DEFAULT_ROOT_PASSWORD.to_string(),
        })
        .await
        .unwrap();
}

async fn login_user(client: &IggyClient, username: &str) {
    client
        .login_user(&LoginUser {
            username: username.to_string(),
            password: USER_PASSWORD.to_string(),
        })
        .await
        .unwrap();
}
