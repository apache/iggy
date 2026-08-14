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

package util

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"time"

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// How long a transiently leaderless cluster is polled for an elected leader
// before proceeding on the current node anyway. Vars, not consts, so tests
// can shrink the budget.
var (
	leaderlessWaitBudget   = 5 * time.Second
	leaderlessPollInterval = 250 * time.Millisecond
)

// leaderOutcome is one leader-check verdict from a cluster-metadata snapshot.
type leaderOutcome int

const (
	// The current node is the leader (or the cluster is single-node).
	leaderIsCurrent leaderOutcome = iota
	// A healthy leader exists elsewhere; reconnect to it.
	redirectToLeader
	// No healthy leader is marked (e.g. mid-election).
	noLeader
)

// CheckAndRedirectToLeader queries the client for cluster metadata and returns
// an address to redirect to (empty string means no redirection needed).
//
// A cluster can be transiently leaderless: a restarted node cedes the
// primaryship its stale view assigns it, and until the peers' election
// completes the roster reports no leader. That window is roughly one
// heartbeat timeout; poll through it instead of proceeding leaderless (a
// replicated request against a non-primary replays for its whole read
// timeout).
func CheckAndRedirectToLeader(
	ctx context.Context,
	c iggcon.Client,
	currentAddress string,
	transport iggcon.Protocol,
	logger *slog.Logger,
) (string, []string, error) {
	logger.Debug("Checking cluster metadata for leader detection")

	deadline := time.Now().Add(leaderlessWaitBudget)
	for {
		meta, err := c.GetClusterMetadata(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return "", nil, err
			}
			// The read is auth-gated, so an Unauthenticated answer means the
			// session died between the sign-in and this check. Like any other
			// failure it keeps the current node; the caller's next request
			// surfaces the eviction.
			logger.Warn(
				"Failed to get cluster metadata, connection will continue on server node",
				"error", err,
				"current_address", currentAddress,
			)
			return "", nil, nil
		}

		logger.Debug(
			"Got cluster metadata",
			"nodes", len(meta.Nodes),
			"cluster", meta.Name,
		)
		addresses, err := clusterAddresses(meta, transport)
		if err != nil {
			return "", nil, err
		}
		outcome, leader, err := processClusterMetadata(meta, currentAddress, transport, logger)
		if err != nil {
			return "", nil, err
		}
		switch outcome {
		case redirectToLeader:
			return leader, addresses, nil
		case leaderIsCurrent:
			return "", addresses, nil
		case noLeader:
			if time.Now().After(deadline) {
				logger.Warn(
					"No active leader found in cluster metadata within the wait budget, connection will continue on server node",
					"wait_budget", leaderlessWaitBudget,
					"current_address", currentAddress,
				)
				return "", addresses, nil
			}
			select {
			case <-ctx.Done():
				return "", nil, ctx.Err()
			case <-time.After(leaderlessPollInterval):
			}
		}
	}
}

func clusterAddresses(metadata *iggcon.ClusterMetadata, transport iggcon.Protocol) ([]string, error) {
	addresses := make([]string, 0, len(metadata.Nodes))
	for i := range metadata.Nodes {
		node := &metadata.Nodes[i]
		if node.Status != iggcon.Healthy {
			continue
		}
		address, err := clusterNodeAddress(node, transport)
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, address)
	}
	return addresses, nil
}

func processClusterMetadata(metadata *iggcon.ClusterMetadata, currentAddress string, transport iggcon.Protocol, logger *slog.Logger) (leaderOutcome, string, error) {
	if len(metadata.Nodes) == 1 {
		logger.Debug(
			"Single-node cluster detected, no leader redirection needed",
			"node", metadata.Nodes[0].Name,
		)
		return leaderIsCurrent, "", nil
	}

	var leader *iggcon.ClusterNode
	for i := range metadata.Nodes {
		node := &metadata.Nodes[i]
		if node.Role == iggcon.RoleLeader && node.Status == iggcon.Healthy {
			leader = node
			break
		}
	}

	if leader == nil {
		return noLeader, "", nil
	}

	leaderAddress, err := clusterNodeAddress(leader, transport)
	if err != nil {
		return leaderIsCurrent, "", err
	}
	logger.Debug(
		"Found leader node",
		"leader", leader.Name,
		"leader_address", leaderAddress,
		"transport", transport,
	)

	if !isSameAddress(currentAddress, leaderAddress) {
		logger.Info(
			"Current connection is not the leader, redirecting",
			"current_address", currentAddress,
			"leader_address", leaderAddress,
		)
		return redirectToLeader, leaderAddress, nil
	}

	logger.Debug("Already connected to leader", "current_address", currentAddress)
	return leaderIsCurrent, "", nil
}

func clusterNodeAddress(node *iggcon.ClusterNode, transport iggcon.Protocol) (string, error) {
	var port uint16
	switch transport {
	case iggcon.Tcp:
		port = node.Endpoints.Tcp
	case iggcon.Quic:
		port = node.Endpoints.Quic
	case iggcon.Http:
		port = node.Endpoints.Http
	case iggcon.WebSocket:
		port = node.Endpoints.WebSocket
	default:
		return "", fmt.Errorf("unsupported transport: %v", transport)
	}
	return net.JoinHostPort(node.IP, strconv.Itoa(int(port))), nil
}

// isSameAddress reports whether two addresses refer to the same endpoint.
// The comparison is lexical after normalization, with an IP-literal fast path
// for spelling differences like ::1 versus 0:0:0:0:0:0:0:1. It deliberately
// never resolves names: both sides come from the same cluster-metadata roster
// or config, and this runs on the failover path where a resolver lookup would
// block with neither a context nor a deadline to interrupt it.
func isSameAddress(addr1, addr2 string) bool {
	host1, port1 := splitAddress(normalizeAddress(addr1))
	host2, port2 := splitAddress(normalizeAddress(addr2))
	if port1 != port2 {
		return false
	}
	if ip1, ip2 := net.ParseIP(host1), net.ParseIP(host2); ip1 != nil && ip2 != nil {
		return ip1.Equal(ip2)
	}
	return host1 == host2
}

// splitAddress splits host:port, treating an unparsable address as a bare
// host so the comparison degrades to a string match instead of failing.
func splitAddress(addr string) (string, string) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr, ""
	}
	return host, port
}

// normalizeAddress canonicalizes address strings for fallback comparison.
func normalizeAddress(addr string) string {
	out := strings.ToLower(addr)
	out = strings.ReplaceAll(out, "localhost", "127.0.0.1")
	out = strings.ReplaceAll(out, "[::]", "[::1]")
	return out
}
