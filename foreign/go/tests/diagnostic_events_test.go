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

package tests_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// sharedServerAddr is the address of the Iggy server started once for the
// whole package by TestMain and reused by every diagnostic-event test.
var sharedServerAddr string

// TestMain starts one Iggy server container for the whole package, runs the
// tests against it, and tears it down.
func TestMain(m *testing.M) {
	ctx := context.Background()

	container, addr, err := startPlainContainer(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "diagnostic_events_test: failed to start Iggy container: %v\n", err)
		os.Exit(1)
	}
	sharedServerAddr = addr

	code := m.Run()

	_ = container.Terminate(ctx)
	os.Exit(code)
}

// startPlainContainer starts a non-TLS Iggy server container and returns it
// along with its mapped address.
func startPlainContainer(ctx context.Context) (testcontainers.Container, string, error) {
	dockerImage := os.Getenv("IGGY_SERVER_DOCKER_IMAGE")
	if dockerImage == "" {
		dockerImage = "apache/iggy:edge"
	}

	req := testcontainers.ContainerRequest{
		Image:        dockerImage,
		ExposedPorts: []string{"8090/tcp"},
		Env: map[string]string{
			"IGGY_ROOT_USERNAME": "iggy",
			"IGGY_ROOT_PASSWORD": "iggy",
			"IGGY_TCP_ADDRESS":   "0.0.0.0:8090",
		},
		WaitingFor: wait.ForAll(
			wait.ForLog("Iggy server is running"),
			wait.ForListeningPort("8090/tcp"),
		),
		Privileged: true,
		AutoRemove: true,
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, "", err
	}
	port, err := container.MappedPort(ctx, "8090")
	if err != nil {
		return nil, "", err
	}

	return container, fmt.Sprintf("%s:%s", host, port.Port()), nil
}

// recvEvent reads one event from the channel with a timeout. Failing the
// test on timeout keeps a flaky CI from hanging the whole suite.
func recvEvent(t *testing.T, ch <-chan iggcon.DiagnosticEvent, timeout time.Duration) (iggcon.DiagnosticEvent, bool) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-time.After(timeout):
		t.Fatalf("timed out after %v waiting for diagnostic event", timeout)
		return 0, false
	}
}

// drainEvents collects events until the channel is closed or the timeout
// expires. Used to assert the full sequence emitted during a scenario.
func drainEvents(t *testing.T, ch <-chan iggcon.DiagnosticEvent, timeout time.Duration) []iggcon.DiagnosticEvent {
	t.Helper()
	deadline := time.After(timeout)
	var events []iggcon.DiagnosticEvent
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return events
			}
			events = append(events, ev)
		case <-deadline:
			t.Fatalf("timed out after %v draining diagnostic events; got %v", timeout, events)
			return events
		}
	}
}

func newPlainClient(t *testing.T, addr string) iggcon.Client {
	t.Helper()
	cli, err := client.NewIggyClient(
		client.WithTcp(
			tcp.WithServerAddress(addr),
		),
	)
	require.NoError(t, err, "Failed to create Iggy client")
	return cli
}

// TestDiagnosticEvents_LoginEmitsSignedIn verifies that LoginUser emits
// a SignedIn event after a successful authentication.
func TestDiagnosticEvents_LoginEmitsSignedIn(t *testing.T) {
	cli := newPlainClient(t, sharedServerAddr)
	defer func() { _ = cli.Close() }()

	require.NoError(t, cli.Connect(context.Background()), "Connect should succeed")

	// Subscribe after connecting so the first event we observe is SignedIn.
	events, cancel := cli.SubscribeEvents()
	defer cancel()

	_, err := cli.LoginUser(context.Background(), defaultUsername, defaultPassword)
	require.NoError(t, err, "Login should succeed")

	ev, ok := recvEvent(t, events, 5*time.Second)
	require.True(t, ok, "channel should not be closed")
	assert.Equal(t, iggcon.DiagnosticEventSignedIn, ev, "expected signed_in event after LoginUser")
}

// TestDiagnosticEvents_LogoutEmitsSignedOut verifies that LogoutUser emits
// a SignedOut event after a successful logout.
func TestDiagnosticEvents_LogoutEmitsSignedOut(t *testing.T) {
	cli := newPlainClient(t, sharedServerAddr)
	defer func() { _ = cli.Close() }()

	require.NoError(t, cli.Connect(context.Background()), "Connect should succeed")

	_, err := cli.LoginUser(context.Background(), defaultUsername, defaultPassword)
	require.NoError(t, err, "Login should succeed")

	// Subscribe after login so the first event we observe is SignedOut.
	events, cancel := cli.SubscribeEvents()
	defer cancel()

	err = cli.LogoutUser(context.Background())
	require.NoError(t, err, "Logout should succeed")

	ev, ok := recvEvent(t, events, 5*time.Second)
	require.True(t, ok, "channel should not be closed")
	assert.Equal(t, iggcon.DiagnosticEventSignedOut, ev, "expected signed_out event after LogoutUser")
}

// TestDiagnosticEvents_CloseEmitsShutdownAndClosesChannel verifies that
// Close emits a final Shutdown event and then closes the subscriber
// channel, so consumers can range over the channel and exit cleanly.
func TestDiagnosticEvents_CloseEmitsShutdownAndClosesChannel(t *testing.T) {
	cli := newPlainClient(t, sharedServerAddr)
	require.NoError(t, cli.Connect(context.Background()), "Connect should succeed")

	// Subscribe after connecting so the only event we observe is Shutdown.
	events, cancel := cli.SubscribeEvents()
	defer cancel()

	require.NoError(t, cli.Close(), "Close should succeed")

	ev, ok := recvEvent(t, events, 5*time.Second)
	require.True(t, ok, "should receive shutdown before channel close")
	assert.Equal(t, iggcon.DiagnosticEventShutdown, ev, "expected shutdown event on Close")

	// Channel must be closed after the final Shutdown event.
	select {
	case _, ok := <-events:
		assert.False(t, ok, "channel should be closed after shutdown")
	case <-time.After(2 * time.Second):
		t.Fatal("channel was not closed after shutdown")
	}
}

// TestDiagnosticEvents_MultipleSubscribers verifies that two independent
// subscribers each receive the full sequence of events.
func TestDiagnosticEvents_MultipleSubscribers(t *testing.T) {
	cli := newPlainClient(t, sharedServerAddr)
	require.NoError(t, cli.Connect(context.Background()), "Connect should succeed")

	// Subscribe after connecting so both subscribers observe the same
	// login → logout → close sequence.
	a, cancelA := cli.SubscribeEvents()
	defer cancelA()
	b, cancelB := cli.SubscribeEvents()
	defer cancelB()

	_, err := cli.LoginUser(context.Background(), defaultUsername, defaultPassword)
	require.NoError(t, err)
	require.NoError(t, cli.LogoutUser(context.Background()))
	require.NoError(t, cli.Close())

	want := []iggcon.DiagnosticEvent{
		iggcon.DiagnosticEventSignedIn,
		iggcon.DiagnosticEventSignedOut,
		iggcon.DiagnosticEventShutdown,
	}
	assert.Equal(t, want, drainEvents(t, a, 10*time.Second), "subscriber A should receive full event sequence")
	assert.Equal(t, want, drainEvents(t, b, 10*time.Second), "subscriber B should receive full event sequence")
}

// TestDiagnosticEvents_FullLifecycle verifies the complete event sequence
// emitted across login → logout → close on a single subscriber.
func TestDiagnosticEvents_FullLifecycle(t *testing.T) {
	cli := newPlainClient(t, sharedServerAddr)

	// Subscribe before connecting so the sequence includes Connected.
	events, cancel := cli.SubscribeEvents()
	defer cancel()

	require.NoError(t, cli.Connect(context.Background()), "Connect should succeed")

	_, err := cli.LoginUser(context.Background(), defaultUsername, defaultPassword)
	require.NoError(t, err, "Login should succeed")

	require.NoError(t, cli.LogoutUser(context.Background()), "Logout should succeed")

	require.NoError(t, cli.Close(), "Close should succeed")

	got := drainEvents(t, events, 10*time.Second)
	want := []iggcon.DiagnosticEvent{
		iggcon.DiagnosticEventConnected,
		iggcon.DiagnosticEventSignedIn,
		iggcon.DiagnosticEventSignedOut,
		iggcon.DiagnosticEventShutdown,
	}
	assert.Equal(t, want, got, "expected full lifecycle event sequence")
}
