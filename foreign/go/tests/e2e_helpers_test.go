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

// Package tests_test exercises the Go SDK against a running Iggy server.
//
// The suite skips unless IGGY_TCP_ADDRESS points at a server. Start one with:
//
//	cargo build --bin iggy-server
//	IGGY_PATH=/tmp/iggy-go-e2e \
//	IGGY_TCP_ADDRESS=127.0.0.1:8090 \
//	IGGY_HTTP_ENABLED=false IGGY_QUIC_ENABLED=false IGGY_WEBSOCKET_ENABLED=false \
//	IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
//	IGGY_ENCRYPTION_ENABLED=true IGGY_ENCRYPTION_KEY=<base64 of 32 random bytes> \
//	target/debug/iggy-server
//
//	IGGY_TCP_ADDRESS=127.0.0.1:8090 go test ./tests
//
// The stream option cases need the encryption settings: without them a keyless
// stream reports no generated key and a stream key is refused.
//
// Set IGGY_TCP_TLS_ENABLED=true to run the TLS cases against a server started
// with IGGY_TCP_TLS_ENABLED=true and the certificate pair in core/certs.
package tests_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	"github.com/apache/iggy/foreign/go/internal/command"
	"github.com/stretchr/testify/require"
)

const (
	rootUsername = "iggy"
	rootPassword = "iggy"

	// Command codes the raw-request cases drive directly.
	getSnapshotCode          = 11
	storeConsumerOffsetCode  = 121
	deleteConsumerOffsetCode = 122
	vendorCode               = 60000

	// AckLevel values of the consumer-offset requests.
	ackNoAck  = 0
	ackQuorum = 1

	// How long a member that joined may take to own its partitions, and how
	// often the wait checks the group.
	groupAssignmentBudget       = 20 * time.Second
	groupAssignmentPollInterval = 50 * time.Millisecond
)

// serverAddress returns the address the suite runs against, skipping when the
// environment does not name one.
func serverAddress(t *testing.T) string {
	t.Helper()

	address := os.Getenv("IGGY_TCP_ADDRESS")
	if address == "" {
		t.Skip("set IGGY_TCP_ADDRESS to run the end-to-end suite against a server")
	}
	return address
}

// tlsEnabled reports whether the server under test speaks TLS on its TCP port.
func tlsEnabled() bool {
	return os.Getenv("IGGY_TCP_TLS_ENABLED") == "true"
}

func certPath(name string) string {
	return filepath.Join("..", "..", "..", "core", "certs", name)
}

// connect builds a client for the server under test and signs it in.
func connect(t *testing.T, options ...tcp.Option) iggcon.Client {
	t.Helper()

	connected := newClient(t, options...)
	_, err := connected.LoginUser(context.Background(), rootUsername, rootPassword)
	require.NoError(t, err)
	return connected
}

// newClient builds a connected client without signing it in.
func newClient(t *testing.T, options ...tcp.Option) iggcon.Client {
	t.Helper()

	address := serverAddress(t)
	all := append([]tcp.Option{tcp.WithServerAddress(address)}, options...)
	if tlsEnabled() {
		all = append(all, tcp.WithTLS(
			tcp.WithTLSCAFile(certPath("iggy_ca_cert.pem")),
			tcp.WithTLSDomain("localhost"),
		))
	}

	built, err := client.NewIggyClient(client.WithTcp(all...))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	require.NoError(t, built.Connect(ctx))

	t.Cleanup(func() { _ = built.Close() })
	return built
}

// scratchTopic creates a stream and a topic scoped to one test and returns
// their identifiers. Both are deleted when the test ends.
func scratchTopic(t *testing.T, connected iggcon.Client, partitionsCount uint32) (
	iggcon.Identifier, iggcon.Identifier,
) {
	t.Helper()
	ctx := context.Background()

	name := fmt.Sprintf("go-e2e-%d-%s", time.Now().UnixNano(), t.Name())
	if len(name) > 255 {
		name = name[:255]
	}

	stream, err := connected.CreateStream(ctx, name, nil)
	require.NoError(t, err)
	streamId, err := iggcon.NewIdentifier(stream.Id)
	require.NoError(t, err)
	t.Cleanup(func() { _ = connected.DeleteStream(context.Background(), streamId) })

	topic, err := connected.CreateTopic(ctx, streamId, name, &iggcon.TopicOptions{PartitionsCount: &partitionsCount})
	require.NoError(t, err)
	topicId, err := iggcon.NewIdentifier(topic.Id)
	require.NoError(t, err)

	return streamId, topicId
}

// waitForGroupAssignment waits until the only member of the group owns every
// partition of the topic. A join commits the membership at once, but the
// member owns a partition only once that partition has installed it as the
// owner. Until then a sync returns no partitions, and group polls and offset
// writes are refused with ConsumerGroupPartitionNotOwned.
func waitForGroupAssignment(
	t *testing.T,
	connected iggcon.Client,
	streamId, topicId, groupId iggcon.Identifier,
) {
	t.Helper()

	deadline := time.Now().Add(groupAssignmentBudget)
	for {
		group, err := connected.GetConsumerGroup(context.Background(), streamId, topicId, groupId)
		require.NoError(t, err)
		if len(group.Members) == 1 && group.Members[0].PartitionsCount == group.PartitionsCount {
			return
		}
		if time.Now().After(deadline) {
			require.Failf(t, "the only member does not own every partition",
				"after %v the group of %d partitions has members %+v",
				groupAssignmentBudget, group.PartitionsCount, group.Members)
		}
		time.Sleep(groupAssignmentPollInterval)
	}
}

// requireSplitPrimaries fails unless the primary of partition 0 of the topic
// is another node than the metadata primary that the roster names. It returns
// the address of that partition primary.
func requireSplitPrimaries(
	t *testing.T,
	ctx context.Context,
	connected iggcon.Client,
	streamId, topicId iggcon.Identifier,
) string {
	t.Helper()

	stream, err := streamId.MarshalBinary()
	require.NoError(t, err)
	topic, err := topicId.MarshalBinary()
	require.NoError(t, err)
	payload := binary.LittleEndian.AppendUint32(append(stream, topic...), 0)
	body, err := connected.SendBinaryRequest(ctx, uint32(command.GetPartitionRouteCode), payload)
	require.NoError(t, err)
	var route iggcon.PartitionRoute
	require.NoError(t, route.UnmarshalBinary(body))
	metadata, err := connected.GetClusterMetadata(ctx)
	require.NoError(t, err)
	var primary, metadataPrimary *iggcon.ClusterNode
	for index := range metadata.Nodes {
		node := &metadata.Nodes[index]
		if node.ReplicaID == route.PrimaryReplica {
			primary = node
		}
		if node.Role == iggcon.RoleLeader {
			metadataPrimary = node
		}
	}
	require.NotNil(t, primary, "route primary must be a configured replica")
	require.NotNil(t, metadataPrimary, "the roster must name a metadata primary")
	require.NotEqual(t, metadataPrimary.ReplicaID, primary.ReplicaID,
		"the fixture must separate metadata and partition primaries")
	return net.JoinHostPort(strings.Trim(primary.IP, "[]"), strconv.Itoa(int(primary.Endpoints.Tcp)))
}

// testMessages builds count messages with predictable payloads.
func testMessages(t *testing.T, count int) []iggcon.IggyMessage {
	t.Helper()

	messages := make([]iggcon.IggyMessage, 0, count)
	for index := range count {
		message, err := iggcon.NewIggyMessage([]byte(fmt.Sprintf("message %d", index)))
		require.NoError(t, err)
		messages = append(messages, message)
	}
	return messages
}

// consumerOffsetPayload builds the shared prefix of the consumer-offset
// requests: [consumer][stream][topic][partition flag][partition].
func consumerOffsetPayload(
	t *testing.T,
	consumer iggcon.Consumer,
	streamId, topicId iggcon.Identifier,
	partitionId uint32,
) []byte {
	t.Helper()

	payload, err := consumer.MarshalBinary()
	require.NoError(t, err)
	stream, err := streamId.MarshalBinary()
	require.NoError(t, err)
	topic, err := topicId.MarshalBinary()
	require.NoError(t, err)

	payload = append(payload, stream...)
	payload = append(payload, topic...)
	payload = append(payload, 1)
	return binary.LittleEndian.AppendUint32(payload, partitionId)
}
