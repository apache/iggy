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

package client

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/apache/iggy/foreign/go/client/tcp"
	ierror "github.com/apache/iggy/foreign/go/errors"
)

const (
	defaultProtocol = "iggy"
	protocolPrefix  = "iggy+"
	tcpProtocol     = "tcp"
)

// ParseConnectionString parses an Iggy TCP connection string into the TCP
// client options it describes, so the result can be passed to WithTcp:
//
//	client.NewIggyClient(client.WithTcp(opts...))
//
// with opts from client.ParseConnectionString("iggy://iggy:iggy@localhost:8090").
//
// The accepted shape is:
//
//	iggy://[username:password@ | personal_access_token@]host:port[?options]
//	iggy+tcp://[username:password@ | personal_access_token@]host:port[?options]
//
// The scheme iggy:// means TCP, the same as the explicit iggy+tcp://.
// Other iggy+<transport> schemes such as iggy+quic:// are rejected,
// because the Go SDK implements the TCP transport only, which covers both
// plain TCP and TLS. Credentials are username:password or a single
// personal access token before the @; either way auto-login is configured,
// matching what the Rust and Node SDKs do with a connection string. The
// host may be a bracketed IPv6 literal, as in [::1]:8090.
//
// Query options follow the grammar the other SDKs share:
//
//	tls=true|false              enable TLS
//	tls_domain=<name>           TLS server name indication (SNI)
//	tls_ca_file=<path>          CA certificate file for verification
//	reconnection_retries=<n|unlimited>
//	reconnection_interval=<duration>
//	reestablish_after=<duration>
//	heartbeat_interval=<duration>
//	nodelay=true|false
//
// The Go client takes its reconnect, heartbeat, and nodelay behavior from
// its own defaults, so those spellings are validated for compatibility and
// then ignored; a value the other SDKs would reject is still rejected here,
// so one connection string is either valid everywhere or nowhere.
func ParseConnectionString(connectionString string) ([]tcp.Option, error) {
	protocolParts := strings.Split(connectionString, "://")
	if len(protocolParts) != 2 {
		return nil, ierror.InvalidConnectionString{}
	}

	protocol, err := parseProtocol(protocolParts[0])
	if err != nil {
		return nil, err
	}
	if protocol != tcpProtocol {
		return nil, fmt.Errorf("%w: unsupported transport %q, Go SDK supports tcp only",
			ierror.InvalidConnectionString{}, protocol)
	}

	parts := strings.Split(protocolParts[1], "@")
	if len(parts) != 2 {
		return nil, ierror.InvalidConnectionString{}
	}

	credentials, err := parseCredentials(parts[0])
	if err != nil {
		return nil, err
	}

	serverAndOptions := strings.Split(parts[1], "?")
	if len(serverAndOptions) > 2 {
		return nil, ierror.InvalidConnectionString{}
	}

	var tlsConfig parsedTLS
	if len(serverAndOptions) == 2 {
		tlsConfig, err = parseConnectionOptions(serverAndOptions[1])
		if err != nil {
			return nil, err
		}
	}

	serverAddress := serverAndOptions[0]
	if serverAddress == "" {
		return nil, ierror.InvalidConnectionString{}
	}
	host, portStr, err := net.SplitHostPort(serverAddress)
	if err != nil || host == "" {
		return nil, ierror.InvalidConnectionString{}
	}
	// Node and Rust accept any u16 port, 0 included; the dial fails later
	// instead of the parse.
	if _, err := strconv.ParseUint(portStr, 10, 16); err != nil {
		return nil, ierror.InvalidNumberValue{}
	}

	options := []tcp.Option{tcp.WithServerAddress(serverAddress)}
	options = append(options, tcp.WithAutoLogin(credentials))
	if tlsConfig.enabled {
		var tlsOpts []tcp.TLSOption
		if tlsConfig.domain != "" {
			tlsOpts = append(tlsOpts, tcp.WithTLSDomain(tlsConfig.domain))
		}
		if tlsConfig.caFile != "" {
			tlsOpts = append(tlsOpts, tcp.WithTLSCAFile(tlsConfig.caFile))
		}
		options = append(options, tcp.WithTLS(tlsOpts...))
	}
	return options, nil
}

// parseProtocol extracts the transport from the scheme, which is either the
// bare default protocol or an explicit iggy+<transport> spelling.
func parseProtocol(scheme string) (string, error) {
	if scheme == defaultProtocol {
		return tcpProtocol, nil
	}
	if !strings.HasPrefix(scheme, protocolPrefix) {
		return "", ierror.InvalidConnectionString{}
	}
	return strings.TrimPrefix(scheme, protocolPrefix), nil
}

// parseCredentials turns the userinfo before the @ into auto-login
// credentials: username:password or a single personal access token.
func parseCredentials(userinfo string) (tcp.Credentials, error) {
	username, password, hasSeparator := strings.Cut(userinfo, ":")
	if !hasSeparator {
		if username == "" {
			return tcp.Credentials{}, ierror.InvalidConnectionString{}
		}
		return tcp.NewPersonalAccessTokenCredentials(username), nil
	}
	if username == "" || password == "" {
		return tcp.Credentials{}, ierror.InvalidConnectionString{}
	}
	return tcp.NewUsernamePasswordCredentials(username, password), nil
}

type parsedTLS struct {
	enabled bool
	domain  string
	caFile  string
}

// parseConnectionOptions validates the shared connection-string grammar,
// keeping the tls options and ignoring the reconnect spellings the Go
// client does not take from a connection string.
func parseConnectionOptions(optionsString string) (parsedTLS, error) {
	parsed := parsedTLS{}
	if optionsString == "" {
		return parsed, ierror.InvalidConnectionString{}
	}
	for _, option := range strings.Split(optionsString, "&") {
		name, value, found := strings.Cut(option, "=")
		if !found {
			return parsed, ierror.InvalidConnectionString{}
		}
		switch name {
		case "tls":
			if value != "true" && value != "false" {
				return parsed, ierror.InvalidConnectionString{}
			}
			parsed.enabled = value == "true"
		case "tls_domain":
			parsed.domain = value
		case "tls_ca_file":
			parsed.caFile = value
		case "reconnection_retries":
			if value != "unlimited" {
				if _, err := strconv.ParseUint(value, 10, 32); err != nil {
					return parsed, ierror.InvalidNumberValue{}
				}
			}
		case "reconnection_interval", "reestablish_after", "heartbeat_interval":
			if !isValidIggyDuration(value) {
				return parsed, ierror.InvalidConnectionString{}
			}
		case "nodelay":
			if value != "true" && value != "false" {
				return parsed, ierror.InvalidConnectionString{}
			}
		default:
			return parsed, ierror.InvalidConnectionString{}
		}
	}
	return parsed, nil
}

// isValidIggyDuration reports whether value is a duration the Rust SDK
// accepts for its connection-string options, which the Go SDK reuses for
// its grammar: the zero spellings, or a humantime-style duration such as
// 1s, 500ms, 1h 30m or 1h30m.
func isValidIggyDuration(value string) bool {
	lower := strings.ToLower(value)
	switch lower {
	case "0", "unlimited", "disabled", "none":
		return true
	}
	remainder := strings.TrimLeft(lower, " ")
	for remainder != "" {
		amount, rest, ok := splitLeadingNumber(remainder)
		if !ok || amount <= 0 {
			return false
		}
		unit, rest := cutDurationUnit(rest)
		if unit == "" {
			return false
		}
		// Parts may be separated by spaces, as in 1h 30m, or run
		// together, as in 1h30m.
		remainder = strings.TrimLeft(rest, " ")
	}
	return true
}

// splitLeadingNumber consumes the leading decimal digits of s, with an
// optional fractional part as in 0.5s or 1.005s. A leading sign is
// rejected, because negative durations do not parse. The value is
// returned as a number, so a fraction-only amount such as 0.5 still
// counts as positive.
func splitLeadingNumber(s string) (float64, string, bool) {
	digits := 0
	for digits < len(s) && s[digits] >= '0' && s[digits] <= '9' {
		digits++
	}
	// A fraction only counts when a digit follows the dot, so a bogus
	// 1.s does not parse and 5. does not either.
	if digits+2 <= len(s) && s[digits] == '.' && s[digits+1] >= '0' && s[digits+1] <= '9' {
		digits++ // the dot
		for digits < len(s) && s[digits] >= '0' && s[digits] <= '9' {
			digits++
		}
	}
	if digits == 0 {
		return 0, "", false
	}
	n, err := strconv.ParseFloat(s[:digits], 64)
	if err != nil {
		return 0, "", false
	}
	return n, s[digits:], true
}

// durationUnits are the humantime units, longest spelling first so a
// prefix match cannot cut a longer unit short.
var durationUnits = []string{
	"milliseconds", "millisecond", "msec", "millis", "ms",
	"microseconds", "microsecond", "usec", "us",
	"nanoseconds", "nanosecond", "nsec", "ns",
	"seconds", "second", "secs", "sec", "s",
	"minutes", "minute", "mins", "min", "m",
	"hours", "hour", "h",
	"days", "day", "d",
	"weeks", "week", "w",
	"years", "year", "y",
}

// cutDurationUnit consumes the leading duration unit of s, longest
// spelling first so a prefix match cannot cut a longer unit short. The
// unit may run straight into the next amount, as in 1h30m; a leftover
// with no leading unit, such as the x of 5x, still fails the caller.
func cutDurationUnit(s string) (unit string, rest string) {
	for _, unit := range durationUnits {
		if strings.HasPrefix(s, unit) {
			return unit, s[len(unit):]
		}
	}
	return "", s
}
