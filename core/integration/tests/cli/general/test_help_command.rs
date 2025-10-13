/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::cli::common::{CLAP_INDENT, IggyCmdTest, USAGE_PREFIX, help::TestHelpCmd};
use serial_test::parallel;

#[tokio::test]
#[parallel]
pub async fn should_help_match() {
    let mut iggy_cmd_test = IggyCmdTest::help_message();

    iggy_cmd_test
        .execute_test_for_help_command(TestHelpCmd::new(
            vec!["help"],
            format!(
                r#"CLI for Iggy message streaming platform

{USAGE_PREFIX} [OPTIONS] [COMMAND]

Commands:
  stream           stream operations [aliases: s]
  topic            topic operations [aliases: t]
  partition        partition operations [aliases: p]
  segment          segments operations [aliases: seg]
  ping             ping iggy server
  me               get current client info
  stats            get iggy server statistics
  snapshot         collect iggy server troubleshooting data
  pat              personal access token operations
  user             user operations [aliases: u]
  client           client operations [aliases: c]
  consumer-group   consumer group operations [aliases: g]
  consumer-offset  consumer offset operations [aliases: o]
  message          message operations [aliases: m]
  context          context operations [aliases: ctx]
  login            login to Iggy server [aliases: li]
  logout           logout from Iggy server [aliases: lo]
  help             Print this message or the help of the given subcommand(s)

Options:
      --transport <TRANSPORT>
          The transport to use. Valid values are `quic`, `http`, `tcp` and `ws`
{CLAP_INDENT}
          [default: tcp]

      --encryption-key <ENCRYPTION_KEY>
          Optional encryption key for the message payload used by the client
{CLAP_INDENT}
          (default: none)

      --credentials-username <CREDENTIALS_USERNAME>
          Optional username for initial login
{CLAP_INDENT}
          [default: DEFAULT_ROOT_USERNAME]

      --credentials-password <CREDENTIALS_PASSWORD>
          Optional password for initial login
{CLAP_INDENT}
          [default: DEFAULT_ROOT_PASSWORD]

      --http-api-url <HTTP_API_URL>
          The optional API URL for the HTTP transport
{CLAP_INDENT}
          [default: http://localhost:3000]

      --http-retries <HTTP_RETRIES>
          The optional number of retries for the HTTP transport
{CLAP_INDENT}
          [default: 3]

      --tcp-server-address <TCP_SERVER_ADDRESS>
          The optional client address for the TCP transport
{CLAP_INDENT}
          [default: 127.0.0.1:8090]

      --tcp-reconnection-max-retries <TCP_RECONNECTION_MAX_RETRIES>
          The optional number of max reconnect retries for the TCP transport
{CLAP_INDENT}
          [default: 3]

      --tcp-reconnection-interval <TCP_RECONNECTION_INTERVAL>
          The optional reconnect interval for the TCP transport
{CLAP_INDENT}
          [default: "1s"]

      --tcp-tls-enabled
          Flag to enable TLS for the TCP transport

      --tcp-tls-domain <TCP_TLS_DOMAIN>
          The optional TLS domain for the TCP transport
{CLAP_INDENT}
          [default: localhost]

      --quic-client-address <QUIC_CLIENT_ADDRESS>
          The optional client address for the QUIC transport
{CLAP_INDENT}
          [default: 127.0.0.1:0]

      --quic-server-address <QUIC_SERVER_ADDRESS>
          The optional server address for the QUIC transport
{CLAP_INDENT}
          [default: 127.0.0.1:8080]

      --quic-server-name <QUIC_SERVER_NAME>
          The optional server name for the QUIC transport
{CLAP_INDENT}
          [default: localhost]

      --quic-reconnection-max-retries <QUIC_RECONNECTION_MAX_RETRIES>
          The optional number of max reconnect retries for the QUIC transport
{CLAP_INDENT}
          [default: 3]

      --quic-reconnection-interval <QUIC_RECONNECTION_INTERVAL>
          The optional reconnect interval for the QUIC transport
{CLAP_INDENT}
          [default: "1s"]

      --quic-max-concurrent-bidi-streams <QUIC_MAX_CONCURRENT_BIDI_STREAMS>
          The optional maximum number of concurrent bidirectional streams for QUIC
{CLAP_INDENT}
          [default: 10000]

      --quic-datagram-send-buffer-size <QUIC_DATAGRAM_SEND_BUFFER_SIZE>
          The optional datagram send buffer size for QUIC
{CLAP_INDENT}
          [default: 100000]

      --quic-initial-mtu <QUIC_INITIAL_MTU>
          The optional initial MTU for QUIC
{CLAP_INDENT}
          [default: 1200]

      --quic-send-window <QUIC_SEND_WINDOW>
          The optional send window for QUIC
{CLAP_INDENT}
          [default: 100000]

      --quic-receive-window <QUIC_RECEIVE_WINDOW>
          The optional receive window for QUIC
{CLAP_INDENT}
          [default: 100000]

      --quic-response-buffer-size <QUIC_RESPONSE_BUFFER_SIZE>
          The optional response buffer size for QUIC
{CLAP_INDENT}
          [default: 1048576]

      --quic-keep-alive-interval <QUIC_KEEP_ALIVE_INTERVAL>
          The optional keep alive interval for QUIC
{CLAP_INDENT}
          [default: 5000]

      --quic-max-idle-timeout <QUIC_MAX_IDLE_TIMEOUT>
          The optional maximum idle timeout for QUIC
{CLAP_INDENT}
          [default: 10000]

      --quic-validate-certificate
          Flag to enable certificate validation for QUIC

      --websocket-server-address <WEBSOCKET_SERVER_ADDRESS>
          The optional server address for the WebSocket transport
{CLAP_INDENT}
          [default: 127.0.0.1:8092]

      --websocket-reconnection-max-retries <WEBSOCKET_RECONNECTION_MAX_RETRIES>
          The optional number of max reconnect retries for the WebSocket transport
{CLAP_INDENT}
          [default: 3]

      --websocket-reconnection-interval <WEBSOCKET_RECONNECTION_INTERVAL>
          The optional reconnect interval for the WebSocket transport
{CLAP_INDENT}
          [default: "1s"]

  -q, --quiet
          Quiet mode (disabled stdout printing)

  -d, --debug <DEBUG>
          Debug mode (verbose printing to given file)

  -u, --username <USERNAME>
          Iggy server username

  -p, --password <PASSWORD>
          Iggy server password
{CLAP_INDENT}
          An optional parameter to specify the password for authentication.
          If not provided, user will be prompted interactively to enter the
          password securely.

  -t, --token <TOKEN>
          Iggy server personal access token

  -n, --token-name <TOKEN_NAME>
          Iggy server personal access token name
{CLAP_INDENT}
          When personal access token is created using command line tool and stored
          inside platform-specific secure storage its name can be used as a value
          for this option without revealing the token value.

      --generate <GENERATOR>
          Shell completion generator for iggy command
{CLAP_INDENT}
          Option prints shell completion code on standard output for selected shell.
          Redirect standard output to file and follow and use selected shell means
          to enable completion for iggy command.
          Option cannot be combined with other options.
{CLAP_INDENT}
          Example:
           source <(iggy --generate bash)
          or
           iggy --generate bash > iggy_completion.bash
           source iggy_completion.bash
{CLAP_INDENT}
          [possible values: bash, elvish, fish, powershell, zsh]

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
"#,
            ),
        ))
        .await;
}
