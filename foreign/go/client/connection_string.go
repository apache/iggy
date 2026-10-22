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
	"math"
	"math/big"
	"net"
	"strconv"
	"strings"
	"time"

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
//	reconnection_interval=<non-zero duration>
//	reestablish_after=<duration>
//	heartbeat_interval=<non-zero duration>
//	nodelay=true|false
//
// Durations take the units the other SDKs share, from nanos to years,
// including the hr, hrs, wk, wks, month and µs spellings, with optional
// fractions and parts run together or spaced, as in 1h30m, 1h 30m or 1 s.
// The reconnection_interval and heartbeat_interval spellings must be
// non-zero and no shorter than one microsecond, because the Rust SDK
// types them as non-zero durations whose wire format carries whole
// microseconds; reestablish_after takes the zero spellings, which Rust
// accepts there.
// The Go client keeps its own reconnect pacing, so the reconnection
// spellings are validated for compatibility and then ignored, and
// a value the other SDKs would reject is still rejected here, so one
// connection string is either valid everywhere or nowhere. The
// heartbeat_interval and nodelay spellings are applied, because the Go
// client carries matching options: heartbeat_interval defaults to the
// client five-second pacing, and nodelay defaults to true, the Go
// client default, so only nodelay=false ever turns Nagle back on.
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
		// Only a plain transport name is echoed, because a malformed
		// scheme may carry credentials, as the C# SDK notes on the same
		// check.
		if !isASCIILetters(protocol) {
			return nil, ierror.InvalidConnectionString{}
		}
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

	// The no-delay default lives here for the no-options path and in
	// parseConnectionOptions for queries, so a query that leaves the
	// spelling out keeps Nagle off.
	var tlsConfig parsedTLS
	tlsConfig.noDelay = true
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
	// instead of the parse. A bad port reports the same error as every other
	// parse failure, which is what the Rust SDK returns here.
	if _, err := strconv.ParseUint(portStr, 10, 16); err != nil {
		return nil, ierror.InvalidConnectionString{}
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
	options = append(options, tcp.WithNoDelay(tlsConfig.noDelay))
	if tlsConfig.heartbeatIntervalNanos != 0 {
		// time.Duration counts nanoseconds in an int64, so an interval the
		// grammar accepts but Go cannot represent fails the parse, like
		// every other spelling the connection string cannot honor.
		if tlsConfig.heartbeatIntervalNanos > math.MaxInt64 {
			return nil, ierror.InvalidConnectionString{}
		}
		options = append(options, tcp.WithHeartbeatInterval(time.Duration(tlsConfig.heartbeatIntervalNanos)))
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
	noDelay bool
	// heartbeatIntervalNanos carries the parsed heartbeat_interval, zero
	// when the spelling is absent so the client keeps its own pacing.
	heartbeatIntervalNanos uint64
}

// parseConnectionOptions validates the shared connection-string grammar,
// keeping the tls and heartbeat options and ignoring the reconnect
// spellings the Go client does not take from a connection string.
func parseConnectionOptions(optionsString string) (parsedTLS, error) {
	parsed := parsedTLS{noDelay: true}
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
		case "reconnection_interval", "heartbeat_interval":
			// Rust types both as NonZeroIggyDuration, which refuses the
			// zero spellings and anything shorter than the whole
			// microseconds the wire format carries.
			nanos, err := parseIggyDuration(value)
			if err != nil || nanos < 1_000 {
				return parsed, ierror.InvalidConnectionString{}
			}
			if name == "heartbeat_interval" {
				parsed.heartbeatIntervalNanos = nanos
			}
		case "reestablish_after":
			// Rust takes this one as a plain duration, where the zero
			// spellings parse.
			if _, err := parseIggyDuration(value); err != nil {
				return parsed, ierror.InvalidConnectionString{}
			}
		case "nodelay":
			if value != "true" && value != "false" {
				return parsed, ierror.InvalidConnectionString{}
			}
			parsed.noDelay = value == "true"
		default:
			return parsed, ierror.InvalidConnectionString{}
		}
	}
	return parsed, nil
}

// isASCIILetters reports whether s is non-empty and ASCII letters only, so
// an unsupported-transport error can echo it without ever echoing the rest
// of a malformed scheme, which may carry credentials.
func isASCIILetters(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if (s[i] < 'a' || s[i] > 'z') && (s[i] < 'A' || s[i] > 'Z') {
			return false
		}
	}
	return true
}

// parseIggyDuration parses a duration the Rust SDK accepts, returning the
// total nanoseconds. It mirrors IggyDuration::from_str: the value is
// lowercased, the zero spellings become a zero duration, and everything
// else goes through a humantime-style grammar of amounts and units, with
// optional fractions and parts run together as in 1h30m or spaced as in
// 1h 30m. A space is allowed between an amount and its unit as in 1 s,
// and the µs spelling parses like us. Fractions must land on a whole
// nanosecond, as in the Rust and Node parsers, so 1.5us parses while
// 0.5ns does not.
func parseIggyDuration(value string) (uint64, error) {
	lower := strings.ToLower(value)
	switch lower {
	case "0", "unlimited", "disabled", "none":
		return 0, nil
	}
	remainder := strings.TrimLeft(lower, " 	")
	if remainder == "" {
		return 0, errInvalidDuration(value)
	}

	var seconds, nanos big.Int
	for remainder != "" {
		wholePart, fracPart, fracScale, rest, ok := cutLeadingAmount(remainder)
		if !ok {
			return 0, errInvalidDuration(value)
		}
		// humantime skips whitespace between an amount and its unit, as
		// in 1 s, the way spaces between parts are skipped below.
		remainder = strings.TrimLeft(rest, " 	")

		index, rest, ok := cutDurationUnit(remainder)
		if !ok {
			return 0, errInvalidDuration(value)
		}
		remainder = strings.TrimLeft(rest, " 	")
		unit := durationUnits[index]

		whole := new(big.Int).SetUint64(wholePart)
		seconds.Add(&seconds, whole.Mul(whole, new(big.Int).SetUint64(unit.seconds)))
		whole = new(big.Int).SetUint64(wholePart)
		nanos.Add(&nanos, whole.Mul(whole, new(big.Int).SetUint64(unit.nanos)))

		if fracScale != 0 {
			// A fraction of a nanosecond always loses precision, as the
			// Rust parser reports outright.
			if unit.seconds == 0 && unit.nanos == 1 {
				return 0, errInvalidDuration(value)
			}
			numerator := new(big.Int).SetUint64(fracPart)
			denominator := new(big.Int).SetUint64(fracScale)
			frac := new(big.Int)
			switch {
			case unit.nanos != 0: // usec and millis land in nanoseconds
				frac.Mul(numerator, new(big.Int).SetUint64(unit.nanos))
				if new(big.Int).Mod(frac, denominator).Sign() != 0 {
					return 0, errInvalidDuration(value)
				}
				nanos.Add(&nanos, frac.Div(frac, denominator))
			case unit.seconds == 1: // seconds land in nanoseconds
				frac.Mul(numerator, big.NewInt(1_000_000_000))
				if new(big.Int).Mod(frac, denominator).Sign() != 0 {
					return 0, errInvalidDuration(value)
				}
				nanos.Add(&nanos, frac.Div(frac, denominator))
			default: // minutes and above land in whole seconds
				frac.Mul(numerator, new(big.Int).SetUint64(unit.seconds))
				if new(big.Int).Mod(frac, denominator).Sign() != 0 {
					return 0, errInvalidDuration(value)
				}
				seconds.Add(&seconds, frac.Div(frac, denominator))
			}
		}
	}

	total := new(big.Int).Mul(&seconds, big.NewInt(1_000_000_000))
	total.Add(total, &nanos)
	if total.BitLen() > 64 {
		return 0, errInvalidDuration(value)
	}
	return total.Uint64(), nil
}

func errInvalidDuration(value string) error {
	return fmt.Errorf("%w: invalid duration %q", ierror.InvalidConnectionString{}, value)
}

// cutLeadingAmount consumes the leading amount of s: decimal digits with an
// optional fractional part, as in 5, 0.5 or 1.005. A leading sign is
// rejected, because negative durations do not parse, and so is a fraction
// without a digit after the dot. The fraction comes back as a numerator and
// a power-of-ten scale, so 1.005 is 1005 over 1000 and the caller can
// divide exactly the way the Rust parser does.
func cutLeadingAmount(s string) (whole uint64, fraction uint64, scale uint64, rest string, ok bool) {
	digits := 0
	for digits < len(s) && isDigitByte(s[digits]) {
		digits++
	}
	if digits == 0 {
		return 0, 0, 0, "", false
	}
	whole, err := strconv.ParseUint(s[:digits], 10, 64)
	if err != nil {
		return 0, 0, 0, "", false
	}
	rest = s[digits:]
	if len(rest) >= 2 && rest[0] == '.' && isDigitByte(rest[1]) {
		fractionDigits := 1
		for fractionDigits < len(rest) && isDigitByte(rest[fractionDigits]) {
			fractionDigits++
		}
		text := rest[1:fractionDigits]
		if len(text) > 19 {
			// Beyond u64 precision, which a fraction could not carry
			// exactly anyway.
			return 0, 0, 0, "", false
		}
		fraction, err = strconv.ParseUint(text, 10, 64)
		if err != nil {
			return 0, 0, 0, "", false
		}
		scale = 1
		for range len(text) {
			scale *= 10
		}
		rest = rest[fractionDigits:]
	}
	return whole, fraction, scale, rest, true
}

func isDigitByte(b byte) bool {
	return b >= '0' && b <= '9'
}

// durationUnit is one unit of the duration grammar, with its whole-second
// and sub-second nanosecond weights. A month is 30.44 days and a year is
// 365.25 days, the same weights the Rust and Node tables carry.
type durationUnit struct {
	name    string
	seconds uint64
	nanos   uint64
}

// durationUnits are the humantime units, ordered longest spelling first so
// a prefix match cannot cut a longer unit short, which is also why the
// single-letter minute spelling m comes after month and millis. The table
// matches the one the Node and C# SDKs parse: it carries the hr, hrs, wk,
// wks and month spellings, the µs spelling beside us, and takes millis
// but not milliseconds.
var durationUnits = []durationUnit{
	{"seconds", 1, 0},
	{"minutes", 60, 0},
	{"second", 1, 0},
	{"minute", 60, 0},
	{"millis", 0, 1_000_000},
	{"months", 2_630_016, 0},
	{"month", 2_630_016, 0},
	{"nanos", 0, 1},
	{"hours", 3600, 0},
	{"weeks", 604_800, 0},
	{"years", 31_557_600, 0},
	{"nsec", 0, 1},
	{"usec", 0, 1_000},
	{"µs", 0, 1_000},
	{"msec", 0, 1_000_000},
	{"secs", 1, 0},
	{"mins", 60, 0},
	{"hour", 3600, 0},
	{"week", 604_800, 0},
	{"year", 31_557_600, 0},
	{"days", 86_400, 0},
	{"sec", 1, 0},
	{"min", 60, 0},
	{"hrs", 3600, 0},
	{"day", 86_400, 0},
	{"wks", 604_800, 0},
	{"yrs", 31_557_600, 0},
	{"ns", 0, 1},
	{"us", 0, 1_000},
	{"ms", 0, 1_000_000},
	{"hr", 3600, 0},
	{"wk", 604_800, 0},
	{"yr", 31_557_600, 0},
	{"s", 1, 0},
	{"m", 60, 0},
	{"h", 3600, 0},
	{"d", 86_400, 0},
	{"w", 604_800, 0},
	{"y", 31_557_600, 0},
}

// cutDurationUnit consumes the leading duration unit of s. The unit may
// run straight into the next amount, as in 1h30m; a leftover with no
// leading unit, such as the x of 5x, still fails the caller.
func cutDurationUnit(s string) (int, string, bool) {
	for i, unit := range durationUnits {
		if strings.HasPrefix(s, unit.name) {
			return i, s[len(unit.name):], true
		}
	}
	return 0, s, false
}
