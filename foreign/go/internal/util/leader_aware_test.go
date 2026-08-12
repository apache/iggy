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
	"log/slog"
	"testing"

	"github.com/apache/iggy/foreign/go/contracts"
	ierror "github.com/apache/iggy/foreign/go/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// metadataClient answers GetClusterMetadata and panics on anything else, so a
// test fails loudly if CheckAndRedirectToLeader starts calling more.
type metadataClient struct {
	iggcon.Client
	metadata *iggcon.ClusterMetadata
	err      error
}

func (c *metadataClient) GetClusterMetadata(context.Context) (*iggcon.ClusterMetadata, error) {
	return c.metadata, c.err
}

func clusterNode(ip string, port uint16, role iggcon.ClusterNodeRole, status iggcon.ClusterNodeStatus) iggcon.ClusterNode {
	return iggcon.ClusterNode{
		Name:      ip,
		IP:        ip,
		Role:      role,
		Status:    status,
		Endpoints: iggcon.TransportEndpoints{Tcp: port},
	}
}

func checkRedirect(t *testing.T, client iggcon.Client, current string) (string, []string, error) {
	t.Helper()
	return CheckAndRedirectToLeader(
		context.Background(), client, current, iggcon.Tcp, slog.New(slog.DiscardHandler))
}

func TestCheckAndRedirectToLeader_PointsAFollowerConnectionAtTheLeader(t *testing.T) {
	client := &metadataClient{metadata: &iggcon.ClusterMetadata{
		Name: "cluster",
		Nodes: []iggcon.ClusterNode{
			clusterNode("10.0.0.1", 8090, iggcon.RoleLeader, iggcon.Healthy),
			clusterNode("10.0.0.2", 8090, iggcon.RoleFollower, iggcon.Healthy),
		},
	}}

	leader, addresses, err := checkRedirect(t, client, "10.0.0.2:8090")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.1:8090", leader)
	assert.Equal(t, []string{"10.0.0.1:8090", "10.0.0.2:8090"}, addresses)
}

func TestCheckAndRedirectToLeader_KeepsAConnectionThatIsAlreadyOnTheLeader(t *testing.T) {
	client := &metadataClient{metadata: &iggcon.ClusterMetadata{
		Nodes: []iggcon.ClusterNode{
			clusterNode("10.0.0.1", 8090, iggcon.RoleLeader, iggcon.Healthy),
			clusterNode("10.0.0.2", 8090, iggcon.RoleFollower, iggcon.Healthy),
		},
	}}

	leader, _, err := checkRedirect(t, client, "10.0.0.1:8090")
	require.NoError(t, err)
	assert.Empty(t, leader)
}

func TestCheckAndRedirectToLeader_NeverRedirectsASingleNodeCluster(t *testing.T) {
	client := &metadataClient{metadata: &iggcon.ClusterMetadata{
		Nodes: []iggcon.ClusterNode{
			clusterNode("10.0.0.1", 8090, iggcon.RoleLeader, iggcon.Healthy),
		},
	}}

	leader, addresses, err := checkRedirect(t, client, "10.0.0.9:8090")
	require.NoError(t, err)
	assert.Empty(t, leader)
	assert.Equal(t, []string{"10.0.0.1:8090"}, addresses)
}

func TestCheckAndRedirectToLeader_StaysPutWhenNoHealthyLeaderExists(t *testing.T) {
	client := &metadataClient{metadata: &iggcon.ClusterMetadata{
		Nodes: []iggcon.ClusterNode{
			clusterNode("10.0.0.1", 8090, iggcon.RoleLeader, iggcon.Unreachable),
			clusterNode("10.0.0.2", 8090, iggcon.RoleFollower, iggcon.Healthy),
		},
	}}

	leader, addresses, err := checkRedirect(t, client, "10.0.0.2:8090")
	require.NoError(t, err)
	assert.Empty(t, leader, "an unhealthy leader is not a redirect target")
	assert.Equal(t, []string{"10.0.0.2:8090"}, addresses,
		"only healthy nodes are reconnect candidates")
}

func TestCheckAndRedirectToLeader_SwallowsAMetadataFailure(t *testing.T) {
	client := &metadataClient{err: ierror.ErrDisconnected}

	leader, addresses, err := checkRedirect(t, client, "10.0.0.1:8090")
	assert.NoError(t, err, "the connection continues on the current node")
	assert.Empty(t, leader)
	assert.Empty(t, addresses)
}

func TestCheckAndRedirectToLeader_PropagatesACancelledContext(t *testing.T) {
	client := &metadataClient{err: context.Canceled}

	_, _, err := checkRedirect(t, client, "10.0.0.1:8090")
	assert.ErrorIs(t, err, context.Canceled)
}

func TestIsSameAddress(t *testing.T) {
	cases := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{"identical", "127.0.0.1:8090", "127.0.0.1:8090", true},
		{"localhost", "localhost:8090", "127.0.0.1:8090", true},
		{"different port", "127.0.0.1:8090", "127.0.0.1:8091", false},
		{"different ip", "192.168.1.1:8090", "127.0.0.1:8090", false},
		{"ipv6 spellings", "[::1]:8090", "[0:0:0:0:0:0:0:1]:8090", true},
		{"same hostname", "IGGY.local:8090", "iggy.local:8090", true},
		// Hostnames compare lexically: this path runs during failover, where
		// a resolver lookup could block with no deadline.
		{"hostname versus its ip", "iggy.local:8090", "10.0.0.1:8090", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isSameAddress(tc.a, tc.b))
		})
	}
}

func TestNormalizeAddress(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"localhost", "localhost:8090", "127.0.0.1:8090"},
		{"uppercase localhost", "LOCALHOST:8090", "127.0.0.1:8090"},
		{"ipv6 any", "[::]:8090", "[::1]:8090"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, normalizeAddress(tc.in))
		})
	}
}

func TestLeaderRedirectionState(t *testing.T) {
	state := iggcon.NewLeaderRedirectionState()
	assert.True(t, state.CanRedirect())
	assert.Equal(t, uint8(0), state.RedirectCount)

	state.IncrementRedirect("127.0.0.1:8090")
	assert.True(t, state.CanRedirect())
	assert.Equal(t, uint8(1), state.RedirectCount)
	assert.Equal(t, "127.0.0.1:8090", state.LastLeaderAddress)

	state.IncrementRedirect("127.0.0.1:8091")
	state.IncrementRedirect("127.0.0.1:8092")
	assert.False(t, state.CanRedirect())
	assert.Equal(t, iggcon.MaxLeaderRedirects, state.RedirectCount)

	state.Reset()
	assert.True(t, state.CanRedirect())
	assert.Equal(t, uint8(0), state.RedirectCount)
}
