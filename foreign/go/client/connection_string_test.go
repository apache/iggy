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
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client/tcp"
	"github.com/apache/iggy/foreign/go/internal/command"
	. "github.com/apache/iggy/foreign/go/internal/fakevsr"
	"github.com/apache/iggy/foreign/go/internal/vsr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// applyOptions runs options the way NewIggyClient does, and returns the
// resulting TCP client so a test can observe what the parser produced.
func applyOptions(t *testing.T, options []tcp.Option) *tcp.IggyTcpClient {
	t.Helper()
	return tcp.NewIggyTcpClient(nil, options...)
}

func TestParseConnectionString_ParsesTheDefaultSchemeWithPasswordCredentials(t *testing.T) {
	opts, err := ParseConnectionString("iggy://iggy:secret@127.0.0.1:8090")
	require.NoError(t, err)
	client := applyOptions(t, opts)

	assert.Equal(t, "127.0.0.1:8090", client.GetConnectionInfo().ServerAddress)
}

func TestParseConnectionString_ParsesTheExplicitTcpSchemeWithAPersonalAccessToken(t *testing.T) {
	opts, err := ParseConnectionString("iggy+tcp://iggypat-1234567890abcdef@localhost:8090")
	require.NoError(t, err)
	client := applyOptions(t, opts)

	assert.Equal(t, "localhost:8090", client.GetConnectionInfo().ServerAddress)
}

func TestParseConnectionString_ParsesBracketedIPv6Host(t *testing.T) {
	opts, err := ParseConnectionString("iggy://iggy:secret@[::1]:8090")
	require.NoError(t, err)
	client := applyOptions(t, opts)

	assert.Equal(t, "[::1]:8090", client.GetConnectionInfo().ServerAddress)
}

// TestParseConnectionString_NoDelayOptionLandsOnTheClient pins the nodelay
// rule: the spelling validates and, unlike a dropped validation, the
// parsed value reaches the client, so ?nodelay=false really turns Nagle
// back on, while a query that leaves nodelay out keeps the client
// default and stays with Nagle off.
func TestParseConnectionString_NoDelayOptionLandsOnTheClient(t *testing.T) {
	withoutOption, err := ParseConnectionString("iggy://iggy:secret@localhost:8090")
	require.NoError(t, err)
	assert.True(t, applyOptions(t, withoutOption).GetNoDelay(),
		"the default must keep Nagle disabled")

	unspecified, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?tls=true")
	require.NoError(t, err)
	assert.True(t, applyOptions(t, unspecified).GetNoDelay(),
		"a query without nodelay must keep Nagle disabled")

	enabled, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?nodelay=true")
	require.NoError(t, err)
	assert.True(t, applyOptions(t, enabled).GetNoDelay())

	disabled, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?nodelay=false")
	require.NoError(t, err)
	assert.False(t, applyOptions(t, disabled).GetNoDelay(),
		"?nodelay=false must reach the dial instead of being dropped")
}

// TestParseConnectionString_HeartbeatIntervalLandsOnTheClient pins the
// parsed heartbeat_interval reaching the client instead of leaving the
// five-second default, and the representation check that refuses an
// interval wider than the client own duration type.
func TestParseConnectionString_HeartbeatIntervalLandsOnTheClient(t *testing.T) {
	withoutOption, err := ParseConnectionString("iggy://iggy:secret@localhost:8090")
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, applyOptions(t, withoutOption).GetHeartbeatInterval(),
		"a query without heartbeat_interval must keep the default pacing")

	oneSecond, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?heartbeat_interval=1s")
	require.NoError(t, err)
	assert.Equal(t, time.Second, applyOptions(t, oneSecond).GetHeartbeatInterval(),
		"?heartbeat_interval=1s must reach the client instead of being dropped")

	fractions, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?heartbeat_interval=250ms")
	require.NoError(t, err)
	assert.Equal(t, 250*time.Millisecond, applyOptions(t, fractions).GetHeartbeatInterval())

	tooLarge, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?heartbeat_interval=1000y")
	require.Error(t, err,
		"an interval wider than time.Duration must fail the parse instead of wrapping")
	assert.Empty(t, tooLarge)
}

// TestParseConnectionString_NonZeroDurationOptionsRejectZeroSpellings
// pins the typing the Rust SDK gives each option:
// reconnection_interval and heartbeat_interval are non-zero durations
// whose wire format carries whole microseconds, so the zero spellings,
// zero-valued durations and anything below one microsecond are refused,
// while reestablish_after is a plain duration where they parse.
func TestParseConnectionString_NonZeroDurationOptionsRejectZeroSpellings(t *testing.T) {
	for _, option := range []string{"reconnection_interval", "heartbeat_interval"} {
		for _, value := range []string{"0", "none", "disabled", "unlimited", "0s", "0ms", "1ns", "999ns"} {
			invalid := "iggy://iggy:secret@localhost:8090?" + option + "=" + value
			t.Run(invalid, func(t *testing.T) {
				opts, err := ParseConnectionString(invalid)
				require.Error(t, err,
					"%s is a non-zero duration in the Rust SDK", option)
				assert.Empty(t, opts)
			})
		}
	}
	for _, value := range []string{"0", "none", "disabled", "unlimited", "0s", "0ms"} {
		valid := "iggy://iggy:secret@localhost:8090?reestablish_after=" + value
		t.Run(valid, func(t *testing.T) {
			opts, err := ParseConnectionString(valid)
			require.NoError(t, err,
				"reestablish_after is a plain duration in the Rust SDK")
			assert.NotEmpty(t, opts)
		})
	}
	for _, option := range []string{"reconnection_interval", "heartbeat_interval"} {
		for _, value := range []string{"1000ns", "1µs", "1us"} {
			valid := "iggy://iggy:secret@localhost:8090?" + option + "=" + value
			t.Run(valid, func(t *testing.T) {
				opts, err := ParseConnectionString(valid)
				require.NoError(t, err,
					"%s must parse at the one-microsecond floor", option)
				assert.NotEmpty(t, opts)
			})
		}
	}
}

// TestParseConnectionString_AcceptsTheExtendedDurationUnits pins the
// spellings the Node and C# SDKs parse and the shared grammar carries:
// hr, hrs, wk, wks and month, plus the millis spelling they use instead
// of milliseconds.
func TestParseConnectionString_AcceptsTheExtendedDurationUnits(t *testing.T) {
	for _, options := range []string{
		"reconnection_interval=1hr",
		"reconnection_interval=2hrs",
		"reconnection_interval=1wk",
		"reconnection_interval=2wks",
		"reconnection_interval=1month",
		"reconnection_interval=2months",
		"reconnection_interval=1hr 30m",
		"reconnection_interval=250millis",
		"heartbeat_interval=1hr",
		"reestablish_after=1month",
	} {
		t.Run(options, func(t *testing.T) {
			opts, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?" + options)
			require.NoError(t, err)
			assert.NotEmpty(t, opts)
		})
	}
	// The long spelling stays outside the grammar, as it is in the Node
	// and C# SDKs.
	opts, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?reconnection_interval=1milliseconds")
	require.Error(t, err)
	assert.Empty(t, opts)
}

// TestParseConnectionString_RejectsUnsupportedTransports checks the scheme
// rule and pins the credential rule together: unsupported transports are
// rejected with an error naming the transport, and a malformed scheme
// never leaks the credentials embedded in it.
func TestParseConnectionString_RejectsUnsupportedTransportsAndNeverEchoesCredentials(t *testing.T) {
	for _, value := range []string{
		"iggy+quic://iggy:secret@localhost:8090",
		"iggy+ws://iggy:secret@localhost:8090",
		"http://iggy:secret@localhost:8090",
	} {
		t.Run(value, func(t *testing.T) {
			opts, err := ParseConnectionString(value)
			require.Error(t, err)
			assert.Empty(t, opts)
			assert.NotContains(t, err.Error(), "secret",
				"the transport error must not echo the connection string")
		})
	}
	// A scheme that embeds credentials after iggy+ must not surface them
	// in the error, only the ASCII-letter transport name is echoed.
	leaking := "iggy+user:secret@host:8090://host:8090"
	t.Run(leaking, func(t *testing.T) {
		opts, err := ParseConnectionString(leaking)
		require.Error(t, err)
		assert.Empty(t, opts)
		assert.NotContains(t, err.Error(), "secret")
		assert.NotContains(t, err.Error(), "user:secret@host")
	})
}

// TestParseConnectionString_ConnectsAndLogsInAgainstAServer drives the
// full connect-and-login flow from the issue acceptance criteria: opts
// from ParseConnectionString, wired into NewIggyClient via WithTcp,
// connecting and signing in against a running (fake) server.
func TestParseConnectionString_ConnectsAndLogsInAgainstAServer(t *testing.T) {
	server := Listen(t, nil)
	address := server.Listener().Addr().String()

	opts, err := ParseConnectionString("iggy://iggy:iggy@" + address)
	require.NoError(t, err)
	cli, err := NewIggyClient(WithTcp(opts...))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	require.NoError(t, cli.Connect(context.Background()))

	recorded := server.Recorded()
	require.Len(t, recorded, 2)
	// The roster read is auth-gated, so the sign-in comes first.
	assert.Equal(t, vsr.OperationRegister, recorded[0].Operation())
	assert.Contains(t, string(recorded[0].Payload), "iggy")
	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[1].Code())
}

// TestParseConnectionString_TLSEnabledByQueryString drives the same flow
// with tls=true and a TLS-only fake server: a client that ignored the
// option would send its register frame in plaintext and the TLS handshake
// would fail, so a successful connect-and-login proves the option landed.
func TestParseConnectionString_TLSEnabledByQueryString(t *testing.T) {
	certificate, caPath := SelfSignedCert(t)

	server := Listen(t, func(conn net.Conn) net.Conn {
		return tls.Server(conn, &tls.Config{Certificates: []tls.Certificate{certificate}})
	})
	address := server.Listener().Addr().String()
	host, port, err := net.SplitHostPort(address)
	require.NoError(t, err)

	connectionString := "iggy://iggy:iggy@" + host + ":" + port +
		"?tls=true&tls_domain=localhost&tls_ca_file=" + caPath
	opts, err := ParseConnectionString(connectionString)
	require.NoError(t, err)
	cli, err := NewIggyClient(WithTcp(opts...))
	require.NoError(t, err)
	t.Cleanup(func() { _ = cli.Close() })

	require.NoError(t, cli.Connect(context.Background()))

	recorded := server.Recorded()
	require.Len(t, recorded, 2)
	assert.Equal(t, vsr.OperationRegister, recorded[0].Operation())
	assert.Equal(t, uint32(command.GetClusterMetadataCode), recorded[1].Code())
}

// TestParseConnectionString_RejectsMalformedConnectionStrings pins every
// malformed shape the Node SDK rejects, copied from its test table. The Rust
// SDK is laxer about some of these shapes, so the Node table is the shared
// floor.
func TestParseConnectionString_RejectsMalformedConnectionStrings(t *testing.T) {
	for _, value := range []string{
		"",
		"iggy",
		"iggy://",
		"iggy://:secret@localhost:8090",
		"iggy://iggy:@localhost:8090",
		"iggy://iggy:secret@localhost",
		"iggy://iggy:secret@:8090",
		"iggy://iggy:secret@localhost:port",
		"iggy://iggy:secret@localhost:70000",
		"iggy://iggy:secret@localhost:8090?unknown=value",
		"iggy://iggy:secret@localhost:8090?tls=maybe",
		"iggy://iggy:secret@localhost:8090?reconnection_retries=three",
		"iggy://iggy:secret@localhost:8090?reconnection_retries=4294967296",
		"iggy://iggy:secret@localhost:8090?reconnection_interval=5",
		"iggy://iggy:secret@localhost:8090?reconnection_interval=garbage",
		"iggy://iggy:secret@localhost:8090?reconnection_interval=-1s",
		"iggy://iggy:secret@localhost:8090?nodelay=yes",
		"iggy://iggy:secret@[::1:8090",
		"iggy://iggy:secret@[]:8090",
		"iggy://iggy:secret@[::1]x:8090",
		"iggy://iggy:secret@2001:db8::1:8090",
		"iggy://iggy:secret@host:8090:9090",
		"iggy://iggy:secret@localhost:8090?",
		"iggy://iggy:secret@localhost:8090?&",
		"iggy://iggy:secret@localhost:8090?reestablish_after=garbage",
	} {
		t.Run(value, func(t *testing.T) {
			opts, err := ParseConnectionString(value)
			require.Error(t, err)
			assert.Empty(t, opts)
		})
	}
}

// TestParseConnectionString_AcceptsPortZero pins the u16 port rule the
// other SDKs share: port 0 parses, because the port is only a u16 on the
// wire and the dial fails later instead of the parse.
func TestParseConnectionString_AcceptsPortZero(t *testing.T) {
	opts, err := ParseConnectionString("iggy://iggy:secret@localhost:0")
	require.NoError(t, err)
	client := applyOptions(t, opts)
	assert.Equal(t, "localhost:0", client.GetConnectionInfo().ServerAddress)
}

// TestParseConnectionString_AcceptsFractionalAndMultiUnitDurations pins
// the duration grammar the other SDKs share: fractional seconds,
// sequences of units, a space between an amount and its unit, and the
// µs spelling, all accepted in connection-string options.
func TestParseConnectionString_AcceptsFractionalAndMultiUnitDurations(t *testing.T) {
	for _, options := range []string{
		"reconnection_interval=500ms",
		"reconnection_interval=0.5s",
		"reconnection_interval=1.005s",
		"reconnection_interval=2m",
		"reconnection_interval=1h 1m 1s",
		"reconnection_interval=1h30m",
		"reconnection_interval=1µs",
		"reestablish_after=1 s",
		"reestablish_after=1µs",
		"reconnection_interval=5sec",
		"reconnection_interval=5msec",
		"heartbeat_interval=10s",
		"reestablish_after=10s",
	} {
		t.Run(options, func(t *testing.T) {
			opts, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?" + options)
			require.NoError(t, err)
			assert.NotEmpty(t, opts)
		})
	}
}

// TestParseConnectionString_AcceptsUnlimitedRetries pins the u32 ceiling
// rule: unlimited is the keyword, and any u32 value parses.
func TestParseConnectionString_AcceptsUnlimitedRetries(t *testing.T) {
	for _, options := range []string{
		"reconnection_retries=unlimited",
		"reconnection_retries=3",
		"reconnection_retries=4294967295",
	} {
		t.Run(options, func(t *testing.T) {
			opts, err := ParseConnectionString("iggy://iggy:secret@localhost:8090?" + options)
			require.NoError(t, err)
			assert.NotEmpty(t, opts)
		})
	}
}

// TestParseConnectionString_ErrorsNeverLeakTheSecret checks that no error
// message contains the password or the token from the rejected string.
func TestParseConnectionString_ErrorsNeverLeakTheSecret(t *testing.T) {
	secrets := []string{"hunter2", "iggypat-1234567890abcdef"}
	for _, value := range []string{
		"iggy://iggy:hunter2@localhost",
		"iggy+tcp://iggypat-1234567890abcdef@localhost",
		"iggy://iggy:hunter2@localhost:8090?unknown=value",
		"iggy://iggy:hunter2@localhost:8090?tls=maybe",
		"iggy://iggy:hunter2@localhost:8090?reconnection_retries=three",
		"iggy://iggy:hunter2@localhost:70000",
	} {
		opts, err := ParseConnectionString(value)
		require.Error(t, err)
		assert.Empty(t, opts)
		for _, secret := range secrets {
			assert.NotContains(t, err.Error(), secret,
				"error message leaked a secret")
		}
	}
}
