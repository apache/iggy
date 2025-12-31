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

use fantoccini::Locator;
use fantoccini::elements::Element;
use fantoccini::error::CmdError;
use fantoccini::wd::Capabilities;
use reqwest::Url;
use std::ops::Deref;
use std::sync::LazyLock;
use std::time::Duration;
use testcontainers::core::{WaitFor, ports::IntoContainerPort as _};
use testcontainers::runners::AsyncRunner as _;
use testcontainers::{ContainerAsync, GenericImage, ImageExt as _};

const IGGY_HTTP_PORT: u16 = 3000;
const IGGY_HTTP_ADDRESS: &str = "0.0.0.0:3000";
const IGGY_STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

/// We normally want to test the current edition of the app, and so having
/// predefined image name and tag should generally suffice. Having these flexible
/// allows for more specific tags (needed on our CI for example), but also gives
/// us an option of testing other builds (not necessarilly built and/or stored
/// locally), e.g. for the latest Iggy Server stored on Dockerhub or locally:
/// ```console
/// E2E_TEST_IGGY_IMAGE_TAG=edge E2E_TEST=true cargo t --test e2e-web
/// ```
static IGGY_IMAGE_NAME: LazyLock<String> = LazyLock::new(|| {
    std::env::var("E2E_TEST_IGGY_IMAGE_NAME")
        .ok()
        .unwrap_or("apache/iggy".into())
});
static IGGY_IMAGE_TAG: LazyLock<String> = LazyLock::new(|| {
    std::env::var("E2E_TEST_IGGY_IMAGE_TAG")
        .ok()
        .unwrap_or("local".into())
});

// On most workstations the wait timeout can actually have a subsecond value for
// pure rendering operations and just a few seconds when the app needs something
// from the back-end (or network in general) or N+ seconds when the app is polling
// the back-end every N seconds. However, on shared CI runners with constraint
// resources the operation can take much longer and we want to be able to increase
// the timeout, e.g.:
/// ```console
/// E2E_TEST_WAIT_TIMEOUT=10 E2E_TEST=true cargo t --test e2e-web
/// ```
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
static WAIT_TIMEOUT: LazyLock<Duration> = LazyLock::new(|| {
    std::env::var("E2E_TEST_WAIT_TIMEOUT")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_WAIT_TIMEOUT)
});

static WEBDRIVER_ADDRESS: LazyLock<String> = LazyLock::new(|| {
    let port = std::env::var("WEBDRIVER_PORT")
        .ok()
        .unwrap_or("4444".into());
    format!("http://localhost:{}", port)
});

static WEBDRIVER_CAPABILITIES: LazyLock<Capabilities> = LazyLock::new(|| {
    let mut args = Vec::new();
    // on CI we want to run end-to-end tests in headless mode
    if std::env::var("E2E_TEST_HEADLESS")
        .ok()
        .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"))
    {
        args.extend([
            "--headless",
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ]);
    }
    let mut caps = serde_json::map::Map::new();
    caps.insert(
        "goog:chromeOptions".to_string(),
        serde_json::json!({
            "args": args,
        }),
    );
    caps
});

#[derive(Debug)]
pub(crate) struct IggyContainer {
    _handle: ContainerAsync<GenericImage>,
    pub url: Url,
}

/// Launch Iggy server instance in a container.
///
/// This will spin up a container and wait until it is ready and return [`IggyContainer`],
/// which keeps the container's handle (whose `Drop` implemention knows how to
/// clean up) as well as the host port and HTTP url of the Iggy server.
///
/// Note that this relies on the "apache/iggy:local" image to be available, i.e. we
/// need to build and tag the image prior to running tests which are using this utility.
/// E.g. with `docker` the image can be built with (from the workspace root):
///
/// ```console
/// docker build -t apache/iggy:local -f core/server/Dockerfile .
/// ```
///
/// For debugging/inspecting of what is being done here programmatically, you can
/// launch a container with the equivalent (again, using `docker`):
/// ```console
/// docker run \
///     --security-opt seccomp=unconfined \
///     -e IGGY_SYSTEM_SHARDING_CPU_ALLOCATION=2 \
///     -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 \
///     -e IGGY_HTTP_WEB_UI=true \
///     -e IGGY_TCP_ENABLED=false \
///     -e IGGY_WEBSOCKET_ENABLED=false \
///     -e IGGY_QUIC_ENABLED=false \
///     -p 0:3000 apache/iggy:local
/// ```
pub(crate) async fn launch_iggy_container() -> IggyContainer {
    let container = GenericImage::new(&*IGGY_IMAGE_NAME, &*IGGY_IMAGE_TAG)
        .with_exposed_port(IGGY_HTTP_PORT.tcp())
        // this needle we are looking for in stdout comes from `http_server.rs`,
        // once it's found we know that the server is ready to accept connections
        .with_wait_for(WaitFor::message_on_stdout(format!(
            "Started HTTP API on: {IGGY_HTTP_ADDRESS}"
        )))
        // Apache Iggy is powered by `io_uring` (find rationale and insights here:
        // https://www.youtube.com/watch?v=oddHJslao64&t=2649s), but io_uring
        // runtime requires specific syscalls (io_uring_setup, io_uring_enter,
        // io_uring_register) which are by default blocked in containerized
        // environments (such as Docker or Podman); for testing purposes solely,
        // we are removing all the restrictions from the environment
        .with_security_opt("seccomp=unconfined")
        // from our local testing experience, container quickly runs out
        // of memory during the bootstrapping process and so we can the number
        // of shards we are spawning
        .with_env_var("IGGY_SYSTEM_SHARDING_CPU_ALLOCATION", "2")
        // as of Iggy Server v0.6.0, 3000 is the default port anyways, but we need
        // Iggy to be listening across all interfaces (by default it's only loopback)
        // since it is running in a container; note though that we are going to talk
        // to Iggy via an ephemeral port on the host that OS is going to assign
        .with_env_var("IGGY_HTTP_ADDRESS", IGGY_HTTP_ADDRESS)
        .with_env_var("IGGY_HTTP_WEB_UI", "true")
        // since Iggy Web UI utilizes HTTP, we are switching off other transports
        .with_env_var("IGGY_TCP_ENABLED", "false")
        .with_env_var("IGGY_WEBSOCKET_ENABLED", "false")
        .with_env_var("IGGY_QUIC_ENABLED", "false")
        .with_startup_timeout(IGGY_STARTUP_TIMEOUT)
        .start()
        .await
        .expect("container with Iggy server to be up and running");

    let host_port = container
        .ports()
        .await
        .expect("port to have been published")
        .map_to_host_port_ipv4(IGGY_HTTP_PORT)
        .expect("host port to have been assigned by OS");
    let url = format!("http://127.0.0.1:{}", host_port).parse().unwrap();

    IggyContainer {
        _handle: container,
        url,
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Client {
    pub fantoccini: fantoccini::Client,
    pub wait_timeout: Duration,
}

impl Client {
    pub(crate) async fn init() -> Self {
        let fantoccini = fantoccini::ClientBuilder::rustls()
            .expect("rustls based ClientBuilder to have been instantiated")
            .capabilities((*WEBDRIVER_CAPABILITIES).clone())
            .connect(&WEBDRIVER_ADDRESS)
            .await
            .expect("web driver to be available");
        Client {
            fantoccini,
            wait_timeout: *WAIT_TIMEOUT,
        }
    }

    /// Wait for an element with this [`Locator`] with [`Client::wait_timeout`].
    pub(crate) async fn wait_for_element(&self, locator: Locator<'_>) -> Result<Element, CmdError> {
        self.wait()
            .at_most(self.wait_timeout)
            .for_element(locator)
            .await
    }
}

impl Deref for Client {
    type Target = fantoccini::Client;
    fn deref(&self) -> &Self::Target {
        &self.fantoccini
    }
}

#[derive(Debug, Clone)]
pub(crate) struct TestCtx {
    pub client: Client,
    pub url: Url,
}

#[macro_export]
macro_rules! test {
    ($test_fn:ident) => {
        #[tokio::test]
        async fn $test_fn() {
            let e2e_tests_enabled = std::env::var("E2E_TEST")
                .ok()
                .is_some_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
            if (!e2e_tests_enabled) {
                return;
            }
            // setup
            if rustls::crypto::CryptoProvider::get_default().is_none() {
                rustls::crypto::ring::default_provider()
                    .install_default()
                    .ok();
            }
            let iggy_container = $crate::utils::launch_iggy_container().await;
            let client = $crate::utils::Client::init().await;
            let url = iggy_container.url.join("ui").unwrap();
            let ctx = $crate::utils::TestCtx {
                client: client.clone(),
                url,
            };
            // run test (catching panic)
            let res = tokio::spawn(super::$test_fn(ctx)).await;
            // teardown
            client.fantoccini.close().await.unwrap();
            drop(iggy_container);
            // unwind (if test panicked)
            if let Err(caught_panic) = res {
                std::panic::resume_unwind(Box::new(caught_panic))
            }
        }
    };
}
