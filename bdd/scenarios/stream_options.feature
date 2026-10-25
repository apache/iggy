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

@stream-options
Feature: Stream options
  As a developer using Apache Iggy
  I want to give a stream its own encryption key through typed options
  So that each stream keeps its data at rest under its own key

  Background:
    Given I have a running Iggy server
    And I am authenticated as the root user

  Scenario: Create a stream with its own key
    When I create a stream named "stream-options-own-key" with an encryption key of 32 bytes valued 7
    Then the stream should report encryption key source "stream"
    And the stream encryption key fingerprint should match the key
    And the stream should report at least one KEK fingerprint
    And the stream should expose no encryption key material

  Scenario: Create a stream with a server generated key
    When I create a stream named "stream-options-generated" without an encryption key
    Then the stream should report encryption key source "server"
    And the stream should report an encryption key fingerprint
    And the stream should report at least one KEK fingerprint
    And the stream should expose no encryption key material
    When I create a topic named "events" with 1 partition in the stream
    And I send a message with payload "generated-key-payload" to the topic
    Then polling the topic should return the payload "generated-key-payload"

  @message-headers
  Scenario: Send and poll a message with headers on a keyed stream
    Given a stream named "stream-options-custom" with an encryption key of 32 bytes valued 9 exists
    When I create a topic named "events" with 1 partition in the stream
    And I send a message with payload "custom-key-payload" and header "trace" set to "abc" to the topic
    Then polling the topic should return the payload "custom-key-payload" and header "trace" set to "abc"

  Scenario: Rename a keyed stream
    Given a stream named "stream-options-rename" with an encryption key of 32 bytes valued 11 exists
    When I create a topic named "events" with 1 partition in the stream
    And I send a message with payload "before-rename" to the topic
    And I rename the stream to "stream-options-renamed"
    Then the stream name should be "stream-options-renamed"
    And the stream should report encryption key source "stream"
    And the stream encryption key fingerprint should match the key
    And polling the topic should return the payload "before-rename"

  Scenario: Stream key reset is rejected
    Given a stream named "stream-options-reset" with an encryption key of 32 bytes valued 13 exists
    When I request a reset of the stream option "encryption_key"
    Then the stream update should be rejected
    And the stream name should be "stream-options-reset"
    And the stream encryption key fingerprint should match the key

  @describe-options
  Scenario: Describe stream options
    When I describe the stream options
    Then the stream option "name" should be updatable and not secret
    And the stream option "encryption_key" should be create only and secret
    And the stream option "encryption_key_fingerprint" should be read only
