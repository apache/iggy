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

Feature: Basic Messaging Operations
  As a developer using Apache Iggy
  I want to perform basic messaging operations
  So that I can send and receive messages through the platform

  Background:
    Given I have a running Iggy server
    And I am authenticated as the root user

  Scenario: Create stream and send messages
    Given I have no streams in the system
    When I create a stream with name "test-stream"
    Then the stream should be created successfully
    And the stream should have name "test-stream"

    When I create a topic with name "test-topic" in stream 0 with 3 partitions
    Then the topic should be created successfully
    And the topic should have name "test-topic"
    And the topic should have 3 partitions

    When I send 10 messages to stream 0, topic 0, partition 0
    Then all messages should be sent successfully

    When I poll messages from stream 0, topic 0, partition 0 starting from offset 0
    Then I should receive 10 messages
    And the messages should have sequential offsets from 0 to 9
    And each message should have the expected payload content
    And the last polled message should match the last sent message
