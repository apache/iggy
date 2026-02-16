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

package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iggy/foreign/go/client"
	"github.com/apache/iggy/foreign/go/client/tcp"
	iggcon "github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/cucumber/godog"
)

const defaultRootUsername = "iggy"
const defaultRootPassword = "iggy"

type leaderCtxKey struct{}
type leaderCtx struct {
	Clients   map[string]iggcon.Client
	Servers   ServerConfigs
	Cluster   ClusterState
	TestState TestState
}

type ServerConfigs struct {
	// Server addresses by role (e.g., "leader" -> "iggy-leader:8091")
	Addresses map[string]string
}

// ClusterState holds cluster enablement and node list.
type ClusterState struct {
	Enabled bool
	Nodes   []iggcon.ClusterNode
}

// TestState tracks per-scenario ephemeral state.
type TestState struct {
	RedirectionOccurred bool
	LastStreamID        *uint32
	LastStreamName      *string
}

type leaderSteps struct{}

func getLeaderContext(ctx context.Context) *leaderCtx {
	return ctx.Value(leaderCtxKey{}).(*leaderCtx)
}

func createAndConnectClient(addr string) (iggcon.Client, error) {
	cli, err := client.NewIggyClient(
		client.WithTcp(
			tcp.WithServerAddress(addr),
		),
	)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func resolveServerAddress(role string, port uint16) string {
	role = strings.ToLower(role)

	switch {
	case role == "leader" && port == 8091:
		if addr, ok := os.LookupEnv("IGGY_TCP_ADDRESS_LEADER"); ok {
			return addr
		}
		return "iggy-leader:8091"

	case role == "follower" && port == 8092:
		if addr, ok := os.LookupEnv("IGGY_TCP_ADDRESS_FOLLOWER"); ok {
			return addr
		}
		return "iggy-follower:8092"

	case port == 8090:
		if addr, ok := os.LookupEnv("IGGY_TCP_ADDRESS"); ok {
			return addr
		}
		return "iggy-server:8090"

	default:
		return "iggy-server:" + strconv.Itoa(int(port))
	}
}

func serverTypeFromPort(port uint16) string {
	switch port {
	case 8090:
		return "single"
	case 8091:
		return "leader"
	case 8092:
		return "follower"
	default:
		return "unknown"
	}
}

func verifyLeaderInMetadata(client iggcon.Client) (*iggcon.ClusterNode, error) {
	metadata, err := client.GetClusterMetadata()
	if err != nil {
		if isClusteringUnavailable(err) {
			// Clustering not enabled, this is OK
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get cluster metadata: %w", err)
	}

	for i := range metadata.Nodes {
		node := &metadata.Nodes[i]
		if node.Role == iggcon.RoleLeader &&
			node.Status == iggcon.Healthy {
			return node, nil
		}
	}

	return nil, nil
}

func verifyClientConnection(client iggcon.Client, expectedPort uint16) (string, error) {
	connInfo := client.GetConnectionInfo()

	expectedSuffix := fmt.Sprintf(":%d", expectedPort)
	if !strings.Contains(connInfo.ServerAddress, expectedSuffix) {
		return "", fmt.Errorf(
			"expected connection to port %d, but connected to: %s",
			expectedPort,
			connInfo.ServerAddress,
		)
	}

	// Verify client can communicate
	if err := client.Ping(); err != nil {
		return "", fmt.Errorf("client cannot ping server: %w", err)
	}

	return connInfo.ServerAddress, nil
}

func isClusteringUnavailable(err error) bool {
	return errors.Is(err, ierror.ErrFeatureUnavailable) || errors.Is(err, ierror.ErrInvalidCommand)
}

func (s leaderSteps) givenClusterConfig(ctx context.Context, nodeCount int) error {
	c := getLeaderContext(ctx)
	c.Cluster.Enabled = true
	c.Cluster.Nodes = make([]iggcon.ClusterNode, 0, nodeCount)
	return nil
}

func (s leaderSteps) givenNodeConfigured(ctx context.Context, nodeID uint32, port uint16) error {
	var quicPort, httpPort, wsPort uint16
	switch port {
	case 8091:
		quicPort, httpPort, wsPort = 8081, 3001, 8071
	case 8092:
		quicPort, httpPort, wsPort = 8082, 3002, 8072
	default:
		quicPort, httpPort, wsPort = port-10, port-5090, port-20
	}

	node := iggcon.ClusterNode{
		Name: fmt.Sprintf("node-%d", nodeID),
		IP:   "iggy-server",
		Endpoints: iggcon.TransportEndpoints{
			Tcp:       port,
			Quic:      quicPort,
			Http:      httpPort,
			WebSocket: wsPort,
		},
		Role:   iggcon.RoleFollower,
		Status: iggcon.Healthy,
	}
	c := getLeaderContext(ctx)
	c.Cluster.Nodes = append(c.Cluster.Nodes, node)
	return nil
}

func (s leaderSteps) givenStartClusteredServer(ctx context.Context, nodeID uint32, port uint16, roleStr string) error {
	c := getLeaderContext(ctx)

	addr := resolveServerAddress(roleStr, port)
	c.Servers.Addresses[roleStr] = addr

	// Update node role in cluster configuration
	var role iggcon.ClusterNodeRole
	switch roleStr {
	case "leader":
		role = iggcon.RoleLeader
	case "follower":
		role = iggcon.RoleFollower
	default:
		return errors.New("regex ensures only leader or follower")
	}
	for i := range c.Cluster.Nodes {
		node := &c.Cluster.Nodes[i]
		if node.Name == fmt.Sprintf("node-%d", nodeID) && node.Endpoints.Tcp == port {
			node.Role = role
			node.Status = iggcon.Healthy
		}
	}
	return nil
}

func (s leaderSteps) givenStartSingleServer(ctx context.Context, port uint16) error {
	c := getLeaderContext(ctx)

	addr := resolveServerAddress("single", port)
	c.Servers.Addresses["single"] = addr
	c.Cluster.Enabled = false
	return nil
}

func (s leaderSteps) whenCreateClientToRole(ctx context.Context, role string, _ uint16) error {
	c := getLeaderContext(ctx)

	addr, ok := c.Servers.Addresses[role]
	if !ok {
		return fmt.Errorf("%s server should be configured", role)
	}

	cli, err := createAndConnectClient(addr)
	if err != nil {
		return err
	}

	c.Clients["main"] = cli
	if role == "leader" {
		c.TestState.RedirectionOccurred = false
	}
	return nil
}

func (s leaderSteps) whenCreateClientDirectToLeader(ctx context.Context, port uint16) error {
	c := getLeaderContext(ctx)

	addr, ok := c.Servers.Addresses["leader"]
	if !ok {
		return errors.New("leader server should be configured")
	}
	if matched, _ := regexp.MatchString(fmt.Sprintf(":%d$", port), addr); !matched {
		return fmt.Errorf("leader should be on port %d, but address is %s", port, addr)
	}
	var err error
	c.Clients["main"], err = createAndConnectClient(addr)
	if err != nil {
		return err
	}
	c.TestState.RedirectionOccurred = false
	return nil
}

func (s leaderSteps) whenCreateClientToPort(ctx context.Context, port uint16) error {
	c := getLeaderContext(ctx)

	role := serverTypeFromPort(port)
	addr, ok := c.Servers.Addresses[role]
	if !ok {
		return fmt.Errorf("server on port %d should be configured", port)
	}
	var err error
	c.Clients["main"], err = createAndConnectClient(addr)
	if err != nil {
		return err
	}
	return nil
}

func (s leaderSteps) whenCreateNamedClient(ctx context.Context, name string, port uint16) error {
	c := getLeaderContext(ctx)
	role := serverTypeFromPort(port)
	addr, ok := c.Servers.Addresses[role]
	if !ok {
		return fmt.Errorf("server on port %d should be configured", port)
	}
	var err error
	c.Clients[name], err = createAndConnectClient(addr)
	if err != nil {
		return err
	}
	return nil
}

func (s leaderSteps) whenAuthenticateRoot(ctx context.Context) error {
	c := getLeaderContext(ctx)

	var names []string
	if len(c.Clients) > 1 {
		names = make([]string, 0, len(c.Clients))
		for k := range c.Clients {
			names = append(names, k)
		}
	} else {
		names = []string{"main"}
	}

	for _, name := range names {
		cli, ok := c.Clients[name]
		if !ok {
			return fmt.Errorf("client %s should be created", name)
		}
		if _, err := cli.LoginUser(defaultRootUsername, defaultRootPassword); err != nil {
			return err
		}
		// Small delay between multiple authentications to avoid race conditions
		if len(c.Clients) > 1 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

func (s leaderSteps) whenCreateStream(ctx context.Context, streamName string) error {
	c := getLeaderContext(ctx)
	cli, ok := c.Clients["main"]
	if !ok {
		return errors.New("client should be available")
	}

	stream, err := cli.CreateStream(streamName)
	if err != nil {
		return err
	}

	c.TestState.LastStreamID = &stream.Id
	c.TestState.LastStreamName = &streamName
	return nil
}

func (s leaderSteps) thenStreamCreatedSuccessfully(ctx context.Context) error {
	c := getLeaderContext(ctx)
	if c.TestState.LastStreamID == nil {
		return errors.New("stream should have been created on leader")
	}
	return nil
}

func (s leaderSteps) thenVerifyClientPort(ctx context.Context, expectedPort uint16) error {
	c := getLeaderContext(ctx)

	cli, ok := c.Clients["main"]
	if !ok {
		return errors.New("client should exist")
	}

	if _, err := verifyClientConnection(cli, expectedPort); err != nil {
		return err
	}
	leader, err := verifyLeaderInMetadata(cli)
	if err != nil {
		return err
	}

	if leader != nil && leader.Endpoints.Tcp == expectedPort {
		c.TestState.RedirectionOccurred = true
	}
	return nil
}

func (s leaderSteps) thenVerifyNamedClientPort(ctx context.Context, clientName string, port uint16) error {
	c := getLeaderContext(ctx)

	cli, ok := c.Clients[clientName]
	if !ok {
		return fmt.Errorf("client %s should exist", clientName)
	}

	if _, err := verifyClientConnection(cli, port); err != nil {
		return fmt.Errorf("client %s connection should succeed: %w", clientName, err)
	}
	leader, err := verifyLeaderInMetadata(cli)
	if err != nil {
		return err
	}
	if leader != nil && leader.Endpoints.Tcp == 0 {
		return fmt.Errorf("client %v should find valid leader TCP port in cluster metadata", clientName)
	}
	// metadata checks skipped in scaffold
	return nil
}

func (s leaderSteps) thenNoRedirection(ctx context.Context) error {
	c := getLeaderContext(ctx)

	if c.TestState.RedirectionOccurred {
		return errors.New("no redirection should occur when connecting directly to leader")
	}
	return nil
}

func (s leaderSteps) thenConnectionRemains(ctx context.Context, port uint16) error {
	c := getLeaderContext(ctx)

	cli, ok := c.Clients["main"]
	if !ok {
		return errors.New("client should exist")
	}

	if _, err := verifyClientConnection(cli, port); err != nil {
		return fmt.Errorf("should remain on original port: %w", err)
	}
	if c.TestState.RedirectionOccurred {
		return errors.New("connection should not have been redirected")
	}
	return nil
}

func (s leaderSteps) thenConnectWithoutRedirection(ctx context.Context) error {
	c := getLeaderContext(ctx)

	cli, ok := c.Clients["main"]
	if !ok {
		return errors.New("client should exist")
	}
	if err := cli.Ping(); err != nil {
		return fmt.Errorf("client should be able to ping server: %v", err)
	}
	if c.TestState.RedirectionOccurred {
		return errors.New("no redirection should occur without clustering")
	}
	return nil
}

func (s leaderSteps) thenBothUseSameServer(ctx context.Context) error {
	c := getLeaderContext(ctx)

	a, okA := c.Clients["A"]
	if !okA {
		return errors.New("client A should exist")
	}
	b, okB := c.Clients["B"]
	if !okB {
		return errors.New("client B should exist")
	}
	if a.GetConnectionInfo().ServerAddress != b.GetConnectionInfo().ServerAddress {
		return errors.New("both clients should be connected to the same server")
	}
	if err := a.Ping(); err != nil {
		return errors.New("client A should be able to ping")
	}
	if err := b.Ping(); err != nil {
		return errors.New("client B should be able to ping")
	}

	leaderA, err := verifyLeaderInMetadata(a)
	if err != nil {
		return err
	}
	leaderB, err := verifyLeaderInMetadata(b)
	if err != nil {
		return err
	}
	if leaderA.IP != leaderB.IP || leaderA.Endpoints.Tcp != leaderB.Endpoints.Tcp {
		return errors.New("both clients should see the same leader")
	}

	return nil
}

func initLeaderRedirectionScenario(sc *godog.ScenarioContext) {
	sc.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		return context.WithValue(context.Background(), leaderCtxKey{}, &leaderCtx{
			Clients: make(map[string]iggcon.Client),
			Servers: ServerConfigs{
				Addresses: make(map[string]string),
			},
			Cluster: ClusterState{
				Enabled: false,
				Nodes:   []iggcon.ClusterNode{},
			},
			TestState: TestState{},
		}), nil
	})

	s := leaderSteps{}
	sc.Step(`^I have cluster configuration enabled with (\d+) nodes$`, s.givenClusterConfig)
	sc.Step(`^node (\d+) is configured on port (\d+)$`, s.givenNodeConfigured)
	sc.Step(`^I start server (\d+) on port (\d+) as (leader|follower)$`, s.givenStartClusteredServer)
	sc.Step(`^I start a single server on port (\d+) without clustering enabled$`, s.givenStartSingleServer)
	sc.Step(`^I create a client connecting to (follower|leader) on port (\d+)$`, s.whenCreateClientToRole)
	sc.Step(`^I create a client connecting directly to leader on port (\d+)$`, s.whenCreateClientDirectToLeader)
	sc.Step(`^I create a client connecting to port (\d+)$`, s.whenCreateClientToPort)
	sc.Step(`^I create client ([A-Z]) connecting to port (\d+)$`, s.whenCreateNamedClient)
	sc.Step(`^(?:I|both clients) authenticate as root user$`, s.whenAuthenticateRoot)
	sc.Step(`^I create a stream named "(.+)"$`, s.whenCreateStream)
	sc.Step(`^the stream should be created successfully on the leader$`, s.thenStreamCreatedSuccessfully)
	sc.Step(`^the client should (?:automatically redirect to leader on|stay connected to|redirect to) port (\d+)$`, s.thenVerifyClientPort)
	sc.Step(`^client ([A-Z]) should (?:stay connected to|redirect to) port (\d+)$`, s.thenVerifyNamedClientPort)
	sc.Step(`^the client should not perform any redirection$`, s.thenNoRedirection)
	sc.Step(`^the connection should remain on port (\d+)$`, s.thenConnectionRemains)
	sc.Step(`^the client should connect successfully without redirection$`, s.thenConnectWithoutRedirection)
	sc.Step(`^both clients should be using the same server$`, s.thenBothUseSameServer)

	sc.After(func(ctx context.Context, sc *godog.Scenario, scErr error) (context.Context, error) {
		c := getLeaderContext(ctx)

		for name, cli := range c.Clients {
			if err := cli.Close(); err != nil {
				scErr = errors.Join(scErr, fmt.Errorf("failed to close client %s: %w", name, err))
			}
		}

		return ctx, scErr
	})
}
