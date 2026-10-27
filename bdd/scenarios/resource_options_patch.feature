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

@resource-options-patch
Feature: Resource option updates
  As a developer using Apache Iggy
  I want to change only the settings I name on a topic or a user
  So that an update never resends or silently resets the others

  Background:
    Given I have a running Iggy server
    And I am authenticated as the root user

  Scenario: Update one topic setting
    Given a stream named "patch-topic-stream" exists
    And a topic named "patch-topic" exists in the stream with message expiry 3600000000 microseconds and max topic size 1073741824 bytes
    When I update the topic message expiry to 7200000000 microseconds without a name
    Then the topic name should be "patch-topic"
    And the topic message expiry should be 7200000000 microseconds set explicitly
    And the topic max topic size should be 1073741824 bytes set explicitly

  Scenario: Reset a topic setting to the server default
    Given a stream named "patch-reset-stream" exists
    And a topic named "patch-reset" exists in the stream with message expiry 3600000000 microseconds and max topic size 1073741824 bytes
    When I reset the topic option "max_topic_size"
    Then the topic max topic size should be derived from the server default
    And the topic message expiry should be 3600000000 microseconds set explicitly

  Scenario: Rename a topic
    Given a stream named "patch-rename-stream" exists
    And a topic named "patch-rename" exists in the stream with message expiry 3600000000 microseconds and max topic size 1073741824 bytes
    When I rename the topic to "patch-renamed"
    Then the topic name should be "patch-renamed"
    And the topic message expiry should be 3600000000 microseconds set explicitly
    And the topic max topic size should be 1073741824 bytes set explicitly

  Scenario: Update a user username and status
    Given a user named "patch-user" with password "patch-password" exists
    When I update the user username to "patch-user-renamed" and status to "inactive"
    Then the user should have username "patch-user-renamed"
    And the user should have status "inactive"
