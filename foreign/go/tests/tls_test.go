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

// Package tests_test contains TLS integration tests for the Go SDK.
//
// These tests use testcontainers to spin up a TLS-enabled Iggy server,
// making them fully self-contained and suitable for CI.
//
// The tests default to using apache/iggy:edge. Override with:
//
//	IGGY_SERVER_DOCKER_IMAGE=custom-image go test -v ./tests
//
// In CI, the image is built from the current PR by docker-build-test-server action.
package tests_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
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

const (
	defaultUsername = "iggy"
	defaultPassword = "iggy"
)

// setupTLSContainer starts a TLS-enabled Iggy server container.
func setupTLSContainer(t *testing.T) (testcontainers.Container, string, string) {
	ctx := context.Background()

	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	require.NoError(t, err)
	certsPath := filepath.Join(repoRoot, "core", "certs")
	caFile := filepath.Join(certsPath, "iggy_ca_cert.pem")

	// Verify certs exist
	_, err = os.Stat(certsPath)
	require.NoError(t, err, "Certs directory not found at %s", certsPath)
	_, err = os.Stat(caFile)
	require.NoError(t, err, "CA cert not found at %s", caFile)

	// In CI, the image is built by docker-build-test-server action.
	// Locally, defaults to apache/iggy:edge for convenience.
	dockerImage := os.Getenv("IGGY_SERVER_DOCKER_IMAGE")
	if dockerImage == "" {
		dockerImage = "apache/iggy:edge"
	}

	req := testcontainers.ContainerRequest{
		Image:        dockerImage,
		ExposedPorts: []string{"8090/tcp"},
		Env: map[string]string{
			"IGGY_ROOT_USERNAME":     "iggy",
			"IGGY_ROOT_PASSWORD":     "iggy",
			"IGGY_TCP_TLS_ENABLED":   "true",
			"IGGY_TCP_TLS_CERT_FILE": "/app/certs/iggy_cert.pem",
			"IGGY_TCP_TLS_KEY_FILE":  "/app/certs/iggy_key.pem",
			"IGGY_TCP_ADDRESS":       "0.0.0.0:8090",
		},
		Files: []testcontainers.ContainerFile{
			{
				HostFilePath:      certsPath,
				ContainerFilePath: "/app/certs",
				FileMode:          0755,
			},
		},
		// Wait for both log message and listening port to ensure TLS is fully initialized
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
	require.NoError(t, err, "Failed to start TLS container")

	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "8090")
	require.NoError(t, err)
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	// Verify certs are mounted correctly to catch path issues early
	_, err = container.CopyFileFromContainer(ctx, "/app/certs/iggy_cert.pem")
	require.NoError(t, err, "Failed to verify cert mount - check certs path")

	waitForServer(t, addr, 60*time.Second)

	return container, addr, caFile
}

// waitForServer waits for the TCP port to accept connections.
func waitForServer(t *testing.T, addr string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	interval := 1 * time.Second

	for time.Now().Before(deadline) {
		dialer := net.Dialer{Timeout: interval}
		conn, err := dialer.Dial("tcp", addr)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(interval)
	}

	t.Fatalf("Server at %s did not become available within %v", addr, timeout)
}

// TestTCPTLSConnection_WithCA_Success tests that a TLS connection succeeds
// when the client is configured with the CA certificate.
func TestTCPTLSConnection_WithCA_Success(t *testing.T) {
	container, serverAddr, caFile := setupTLSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	// Use "localhost" because container.Host() may return "127.0.0.1" which won't match the cert
	_, portStr, _ := net.SplitHostPort(serverAddr)
	connectAddr := fmt.Sprintf("localhost:%s", portStr)

	cli, err := client.NewIggyClient(
		client.WithTcp(
			tcp.WithServerAddress(connectAddr),
			tcp.WithTLS(
				tcp.WithTLSCAFile(caFile),
				tcp.WithTLSDomain("localhost"),
			),
		),
	)
	require.NoError(t, err, "Failed to create TLS client")
	defer func() { _ = cli.Close() }()

	_, err = cli.LoginUser(defaultUsername, defaultPassword)
	require.NoError(t, err, "Login should succeed over TLS")
}

// TestTCPTLSConnection_WithoutTLS_Failure tests that a non-TLS connection
// fails when the server requires TLS.
func TestTCPTLSConnection_WithoutTLS_Failure(t *testing.T) {
	container, serverAddr, _ := setupTLSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	_, portStr, _ := net.SplitHostPort(serverAddr)
	connectAddr := fmt.Sprintf("localhost:%s", portStr)

	cli, err := client.NewIggyClient(
		client.WithTcp(
			tcp.WithServerAddress(connectAddr),
		),
	)

	if err == nil && cli != nil {
		defer func() { _ = cli.Close() }()
		_, err = cli.LoginUser(defaultUsername, defaultPassword)
	}

	assert.Error(t, err, "Connection/login should fail when TLS is required but not used")
}

// TestTCPTLSConnection_MessageFlow_Success tests complete message flow
// (create stream/topic, send messages, poll messages) over TLS.
func TestTCPTLSConnection_MessageFlow_Success(t *testing.T) {
	container, serverAddr, caFile := setupTLSContainer(t)
	defer func() {
		if err := container.Terminate(context.Background()); err != nil {
			t.Logf("Failed to terminate container: %v", err)
		}
	}()

	_, portStr, _ := net.SplitHostPort(serverAddr)
	connectAddr := fmt.Sprintf("localhost:%s", portStr)

	cli, err := client.NewIggyClient(
		client.WithTcp(
			tcp.WithServerAddress(connectAddr),
			tcp.WithTLS(
				tcp.WithTLSCAFile(caFile),
				tcp.WithTLSDomain("localhost"),
			),
		),
	)
	require.NoError(t, err, "Failed to create TLS client")
	defer func() { _ = cli.Close() }()

	_, err = cli.LoginUser(defaultUsername, defaultPassword)
	require.NoError(t, err, "Login should succeed")

	streamName := "tls-test-stream"
	_, err = cli.CreateStream(streamName)
	require.NoError(t, err, "Failed to create stream")

	defer func() {
		streamIdentifier, _ := iggcon.NewIdentifier(streamName)
		_ = cli.DeleteStream(streamIdentifier)
	}()

	topicName := "tls-test-topic"
	streamIdentifier, _ := iggcon.NewIdentifier(streamName)
	_, err = cli.CreateTopic(
		streamIdentifier,
		topicName,
		1, // partitionCount
		iggcon.CompressionAlgorithmNone,
		iggcon.IggyExpiryNeverExpire,
		0,
		nil,
	)
	require.NoError(t, err, "Failed to create topic")

	messageCount := 10
	messages := make([]iggcon.IggyMessage, messageCount)
	for i := 0; i < messageCount; i++ {
		payload := fmt.Sprintf("message-%d", i+1)
		msg, _ := iggcon.NewIggyMessage([]byte(payload))
		messages[i] = msg
	}

	topicIdentifier, _ := iggcon.NewIdentifier(topicName)
	partitionID := uint32(0)
	partitioning := iggcon.PartitionId(partitionID)
	err = cli.SendMessages(streamIdentifier, topicIdentifier, partitioning, messages)
	require.NoError(t, err, "Failed to send messages over TLS")

	consumer := iggcon.DefaultConsumer()
	offset := uint64(0)
	pollMessages, err := cli.PollMessages(
		streamIdentifier,
		topicIdentifier,
		consumer,
		iggcon.OffsetPollingStrategy(offset),
		uint32(messageCount),
		false,
		&partitionID,
	)
	require.NoError(t, err, "Failed to poll messages over TLS")

	assert.Len(t, pollMessages.Messages, messageCount, "Should receive all sent messages")

	for i, msg := range pollMessages.Messages {
		expectedPayload := fmt.Sprintf("message-%d", i+1)
		assert.Equal(t, expectedPayload, string(msg.Payload), "Message payload should match")
	}
}
