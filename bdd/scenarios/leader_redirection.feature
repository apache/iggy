# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

@requires-leader-awareness
@rust
Feature: Leader-Aware Client Connections
  As a developer using Apache Iggy with clustering
  I want my SDK clients to automatically connect to the leader node
  So that I can always interact with the correct server instance

  Background:
    Given I have cluster configuration enabled with 2 nodes
    And node 0 is configured on port 8091
    And node 1 is configured on port 8092

  Scenario: Client redirects from follower to leader
    Given I start server 0 on port 8091 as leader
    And I start server 1 on port 8092 as follower
    When I create a client connecting to follower on port 8092
    And I authenticate as root user
    Then the client should automatically redirect to leader on port 8091
    When I create a stream named "test-stream"
    Then the stream should be created successfully on the leader

  Scenario: Client connects directly to leader without redirection
    Given I start server 0 on port 8091 as leader
    And I start server 1 on port 8092 as follower
    When I create a client connecting directly to leader on port 8091
    And I authenticate as root user
    Then the client should not perform any redirection
    And the connection should remain on port 8091

  Scenario: Client handles missing cluster metadata gracefully
    Given I start a single server on port 8090 without clustering enabled
    When I create a client connecting to port 8090
    And I authenticate as root user
    Then the client should connect successfully without redirection
    When I create a stream named "single-server-stream"
    Then the stream should be created successfully on the leader

  Scenario: Multiple clients converge to the same leader
    Given I start server 0 on port 8091 as leader
    And I start server 1 on port 8092 as follower
    When I create client A connecting to port 8091
    And I create client B connecting to port 8092
    And both clients authenticate as root user
    Then client A should stay connected to port 8091
    And client B should redirect to port 8091
    And both clients should be using the same server
