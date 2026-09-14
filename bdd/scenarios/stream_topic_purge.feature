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

@stream-topic-purge
Feature: Stream and Topic Purge
  As a developer using Apache Iggy
  I want to purge all messages from a stream or topic
  So that I can clear data without deleting the resource

  Background:
    Given I have a running Iggy server
    And I am authenticated as the root user

  Scenario: Purge stream removes all messages
    Given I have a stream with name "purge-test-stream"
    And I have a topic with name "purge-topic" in stream "purge-test-stream" with 3 partitions
    And I have sent 10 messages to stream "purge-test-stream", topic "purge-topic", partition 0
    When I purge the stream "purge-test-stream"
    And I poll messages from stream "purge-test-stream", topic "purge-topic", partition 0 starting from offset 0
    Then I should receive 0 messages

  Scenario: Purge topic removes all messages
    Given I have a stream with name "purge-topic-stream"
    And I have a topic with name "purge-target-topic" in stream "purge-topic-stream" with 3 partitions
    And I have sent 10 messages to stream "purge-topic-stream", topic "purge-target-topic", partition 0
    When I purge topic "purge-target-topic" in stream "purge-topic-stream"
    And I poll messages from stream "purge-topic-stream", topic "purge-target-topic", partition 0 starting from offset 0
    Then I should receive 0 messages

  Scenario: Purge stream does not affect other stream
    Given I have a stream with name "purge-stream-a"
    And I have a topic with name "topic-in-a" in stream "purge-stream-a" with 1 partition
    And I have sent 10 messages to stream "purge-stream-a", topic "topic-in-a", partition 0
    And I have a stream with name "purge-stream-b"
    And I have a topic with name "topic-in-b" in stream "purge-stream-b" with 1 partition
    And I have sent 10 messages to stream "purge-stream-b", topic "topic-in-b", partition 0
    When I purge the stream "purge-stream-a"
    And I poll messages from stream "purge-stream-a", topic "topic-in-a", partition 0 starting from offset 0
    Then I should receive 0 messages
    And I poll messages from stream "purge-stream-b", topic "topic-in-b", partition 0 starting from offset 0
    Then I should receive 10 messages

  Scenario: Purge topic does not affect other topic
    Given I have a stream with name "multi-topic-stream"
    And I have a topic with name "topic-to-purge" in stream "multi-topic-stream" with 1 partition
    And I have a topic with name "topic-to-keep" in stream "multi-topic-stream" with 1 partition
    And I have sent 10 messages to stream "multi-topic-stream", topic "topic-to-purge", partition 0
    And I have sent 10 messages to stream "multi-topic-stream", topic "topic-to-keep", partition 0
    When I purge topic "topic-to-purge" in stream "multi-topic-stream"
    And I poll messages from stream "multi-topic-stream", topic "topic-to-purge", partition 0 starting from offset 0
    Then I should receive 0 messages
    And I poll messages from stream "multi-topic-stream", topic "topic-to-keep", partition 0 starting from offset 0
    Then I should receive 10 messages

  Scenario: Purge non-existing stream should return an error
    When I try to purge a non-existing stream
    Then the purge operation should fail with a stream not found error

  Scenario: Purge non-existing topic should return an error
    Given I have a stream with name "stream-for-purge"
    When I try to purge a non-existing topic in stream "stream-for-purge"
    Then the purge topic operation should fail with a topic not found error
