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

	iggcon "github.com/apache/iggy/foreign/go/contracts"
)

// CheckAndRedirectToLeader queries the client for cluster metadata and returns
// an address to redirect to (empty string means no redirection needed).
func CheckAndRedirectToLeader(ctx context.Context, c iggcon.Client, currentAddress string, transport iggcon.Protocol, logger *slog.Logger) (string, error) {
	logger.Debug("Checking cluster metadata for leader detection")

	meta, err := c.GetClusterMetadata(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return "", err
		}
		logger.Warn(
			"Failed to get cluster metadata, connection will continue on server node",
			"error", err,
			"current_address", currentAddress,
		)
		return "", nil
	}

	logger.Debug(
		"Got cluster metadata",
		"nodes", len(meta.Nodes),
		"cluster", meta.Name,
	)
	return processClusterMetadata(meta, currentAddress, transport, logger)
}

func processClusterMetadata(metadata *iggcon.ClusterMetadata, currentAddress string, transport iggcon.Protocol, logger *slog.Logger) (string, error) {
	if len(metadata.Nodes) == 1 {
		logger.Debug(
			"Single-node cluster detected, no leader redirection needed",
			"node", metadata.Nodes[0].Name,
		)
		return "", nil
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
		logger.Warn(
			"No active leader found in cluster metadata, connection will continue on server node",
			"current_address", currentAddress,
		)
		return "", nil
	}

	var leaderPort uint16
	switch transport {
	case iggcon.Tcp:
		leaderPort = leader.Endpoints.Tcp
	case iggcon.Quic:
		leaderPort = leader.Endpoints.Quic
	case iggcon.Http:
		leaderPort = leader.Endpoints.Http
	case iggcon.WebSocket:
		leaderPort = leader.Endpoints.WebSocket
	default:
		return "", fmt.Errorf("unsupported transport: %v", transport)
	}

	leaderAddress := net.JoinHostPort(leader.IP, strconv.Itoa(int(leaderPort)))
	logger.Info(
		"Found leader node",
		"leader", leader.Name,
		"address", leaderAddress,
		"transport", transport,
	)

	if !isSameAddress(currentAddress, leaderAddress) {
		logger.Info(
			"Current connection is not the leader, redirecting",
			"current_address", currentAddress,
			"leader_address", leaderAddress,
		)
		return leaderAddress, nil
	}

	logger.Debug("Already connected to leader at", "current_address", currentAddress)
	return "", nil
}

// isSameAddress returns true if two addresses refer to the same endpoint.
func isSameAddress(addr1, addr2 string) bool {
	a1 := parseAddress(addr1)
	a2 := parseAddress(addr2)

	if a1 != nil && a2 != nil {
		return a1.IP.Equal(a2.IP) && a1.Port == a2.Port
	}

	return normalizeAddress(addr1) == normalizeAddress(addr2)
}

// parseAddress attempts to parse an address into a *net.TCPAddr.
func parseAddress(addr string) *net.TCPAddr {
	// Try direct parse
	if ta, err := net.ResolveTCPAddr("tcp", addr); err == nil {
		return ta
	}
	// Normalize then try again
	normalized := normalizeAddress(addr)
	if ta, err := net.ResolveTCPAddr("tcp", normalized); err == nil {
		return ta
	}
	return nil
}

// normalizeAddress canonicalizes address strings for fallback comparison.
func normalizeAddress(addr string) string {
	out := strings.ToLower(addr)
	out = strings.ReplaceAll(out, "localhost", "127.0.0.1")
	out = strings.ReplaceAll(out, "[::]", "[::1]")
	return out
}
