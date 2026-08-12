/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.bdd;

import io.cucumber.java.After;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.apache.iggy.client.blocking.tcp.IggyTcpClient;
import org.apache.iggy.cluster.ClusterNode;
import org.apache.iggy.cluster.ClusterNodeRole;
import org.apache.iggy.cluster.ClusterNodeStatus;
import org.apache.iggy.exception.IggyException;
import org.apache.iggy.stream.StreamDetails;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class LeaderRedirectionSteps {

    private static final String MAIN_CLIENT = "main";

    private final Map<String, IggyTcpClient> clients = new LinkedHashMap<>();
    private boolean redirectionOccurred;
    private Long lastStreamId;

    @After
    public void closeClients() {
        clients.values().forEach(IggyTcpClient::close);
        clients.clear();
    }

    // ---------- Background ----------

    @Given("I have cluster configuration enabled with {int} nodes")
    public void clusterConfigurationEnabled(int nodeCount) {
        assertTrue(nodeCount > 0, "Cluster must have at least one node");
    }

    @Given("node {int} is configured on port {int}")
    public void nodeConfiguredOnPort(int nodeId, int port) {
        assertFalse(addressForPort(port).isBlank(), "Node " + nodeId + " address should be configured");
    }

    // ---------- Server start steps (validate addresses are configured) ----------

    @Given("^I start server (\\d+) on port (\\d+) as (leader|follower)$")
    public void startServerAs(int nodeId, int port, String role) {
        assertAddressMatchesPort(addressForRole(role), port, role + " address");
    }

    @Given("I start a single server on port {int} without clustering enabled")
    public void startSingleServer(int port) {
        assertAddressMatchesPort(singleServerAddress(), port, "single-server address");
    }

    // ---------- Client creation ----------

    @When("^I create a client connecting to (follower|leader) on port (\\d+)$")
    public void createClientConnectingToRole(String role, int port) {
        String address = addressForRole(role);
        assertAddressMatchesPort(address, port, role + " address");
        createAndConnectClient(MAIN_CLIENT, address);
    }

    @When("I create a client connecting directly to leader on port {int}")
    public void createClientConnectingDirectlyToLeader(int port) {
        String address = addressForRole("leader");
        assertAddressMatchesPort(address, port, "leader address");
        createAndConnectClient(MAIN_CLIENT, address);
        redirectionOccurred = false;
    }

    @When("I create a client connecting to port {int}")
    public void createClientConnectingToPort(int port) {
        createAndConnectClient(MAIN_CLIENT, addressForPort(port));
    }

    @When("^I create client ([A-Z]) connecting to port (\\d+)$")
    public void createNamedClientConnectingToPort(String clientName, int port) {
        createAndConnectClient(clientName, addressForPort(port));
    }

    // ---------- Auth ----------

    @When("I authenticate as root user")
    public void authenticateAsRootUser() {
        authenticateAllClients();
    }

    @When("both clients authenticate as root user")
    public void bothClientsAuthenticateAsRootUser() {
        authenticateAllClients();
    }

    // ---------- Stream operations ----------

    @When("I create a stream named {string}")
    public void createStreamNamed(String streamName) {
        StreamDetails stream = client(MAIN_CLIENT).streams().createStream(streamName);
        assertNotNull(stream, "Stream should be created");
        lastStreamId = stream.id();
    }

    @Then("the stream should be created successfully on the leader")
    public void streamCreatedSuccessfullyOnLeader() {
        assertNotNull(lastStreamId, "Stream should have been created on the leader");
    }

    // ---------- Connection / redirection assertions ----------

    @Then("the client should automatically redirect to leader on port {int}")
    public void clientAutomaticallyRedirectsToLeader(int expectedPort) {
        verifyClientPort(MAIN_CLIENT, expectedPort);
    }

    @Then("^client ([A-Z]) should stay connected to port (\\d+)$")
    public void namedClientStaysConnectedToPort(String clientName, int expectedPort) {
        verifyClientPort(clientName, expectedPort);
    }

    @Then("^client ([A-Z]) should redirect to port (\\d+)$")
    public void namedClientRedirectsToPort(String clientName, int expectedPort) {
        verifyClientPort(clientName, expectedPort);
    }

    @Then("the client should not perform any redirection")
    public void clientDoesNotRedirect() {
        assertFalse(redirectionOccurred, "No redirection should occur when connecting directly to the leader");
    }

    @Then("the connection should remain on port {int}")
    public void connectionRemainsOnPort(int expectedPort) {
        verifyClientPort(MAIN_CLIENT, expectedPort);
        assertFalse(redirectionOccurred, "Connection should not have been redirected");
    }

    @Then("the client should connect successfully without redirection")
    public void clientConnectsWithoutRedirection() {
        client(MAIN_CLIENT).system().ping();
        assertFalse(redirectionOccurred, "No redirection should occur without clustering");
    }

    @Then("both clients should be using the same server")
    public void bothClientsUseTheSameServer() {
        IggyTcpClient clientA = client("A");
        IggyTcpClient clientB = client("B");

        assertEquals(
                clientA.getConnectionInfo().serverAddress(),
                clientB.getConnectionInfo().serverAddress(),
                "Both clients should be connected to the same server");

        clientA.system().ping();
        clientB.system().ping();

        Optional<ClusterNode> leaderA = leaderFromMetadata(clientA);
        Optional<ClusterNode> leaderB = leaderFromMetadata(clientB);
        if (leaderA.isPresent() && leaderB.isPresent()) {
            assertEquals(
                    leaderA.get().ip() + ":" + leaderA.get().endpoints().tcp(),
                    leaderB.get().ip() + ":" + leaderB.get().endpoints().tcp(),
                    "Both clients should see the same leader");
        }
    }

    // ---------- Helpers ----------

    private void createAndConnectClient(String name, String address) {
        String[] hostPort = address.split(":", 2);
        IggyTcpClient client = IggyTcpClient.builder()
                .host(hostPort[0])
                .port(Integer.parseInt(hostPort[1]))
                .build();
        client.connect();
        clients.put(name, client);
    }

    private void authenticateAllClients() {
        String username = getenvOrDefault("IGGY_ROOT_USERNAME", "iggy");
        String password = getenvOrDefault("IGGY_ROOT_PASSWORD", "iggy");
        for (IggyTcpClient client : clients.values()) {
            String initialAddress = client.getConnectionInfo().serverAddress();
            client.users().login(username, password);
            if (!initialAddress.equals(client.getConnectionInfo().serverAddress())) {
                redirectionOccurred = true;
            }
        }
    }

    private void verifyClientPort(String clientName, int expectedPort) {
        IggyTcpClient client = client(clientName);
        assertEquals(
                expectedPort,
                client.getConnectionInfo().port(),
                "Client " + clientName + " should be connected to port " + expectedPort);
        client.system().ping();
    }

    private IggyTcpClient client(String name) {
        IggyTcpClient client = clients.get(name);
        if (client == null) {
            throw new IllegalStateException("Client " + name + " should exist");
        }
        return client;
    }

    /**
     * The leader per the roster, or empty when clustering is unavailable
     * (a server too old for the command, or metadata otherwise rejected).
     */
    private static Optional<ClusterNode> leaderFromMetadata(IggyTcpClient client) {
        try {
            return client.system().getClusterMetadata().nodes().stream()
                    .filter(node -> node.role() == ClusterNodeRole.Leader && node.status() == ClusterNodeStatus.Healthy)
                    .findFirst();
        } catch (IggyException error) {
            return Optional.empty();
        }
    }

    private static void assertAddressMatchesPort(String address, int port, String description) {
        assertTrue(address.endsWith(":" + port), description + " " + address + " should use port " + port);
    }

    private static String addressForRole(String role) {
        return switch (role) {
            case "leader" -> getenvOrDefault("IGGY_TCP_ADDRESS_LEADER", "127.0.0.1:8091");
            case "follower" -> getenvOrDefault("IGGY_TCP_ADDRESS_FOLLOWER", "127.0.0.1:8092");
            default -> throw new IllegalArgumentException("Unknown role: " + role);
        };
    }

    private static String addressForPort(int port) {
        return switch (port) {
            case 8090 -> singleServerAddress();
            case 8091 -> addressForRole("leader");
            case 8092 -> addressForRole("follower");
            default -> throw new IllegalArgumentException("Unknown port: " + port);
        };
    }

    private static String singleServerAddress() {
        return getenvOrDefault("IGGY_TCP_ADDRESS", "127.0.0.1:8090");
    }

    private static String getenvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
