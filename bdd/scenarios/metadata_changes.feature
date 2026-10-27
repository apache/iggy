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

@metadata-changes
Feature: Metadata changes
  As an operator of Apache Iggy
  I want to read the metadata changes the server committed
  So that a mirror of streams, topics, users and sessions stays current without listing everything again

  Background:
    Given I have a running Iggy server
    And I am authenticated as the root user

  Scenario: Follow stream changes from the latest position
    Given I hold the latest metadata change cursor for the groups "streams"
    When I create a stream named "changes-follow" without an encryption key
    And I rename the stream to "changes-follow-renamed"
    And I delete the stream
    Then following the metadata changes should return the kinds "stream_created,stream_updated,stream_deleted" for the stream
    And each followed change should name the root user as its actor

  Scenario: Filter changes by group
    Given I hold the latest metadata change cursor for the groups "topics"
    When I create a stream named "changes-filter" without an encryption key
    And I create a topic named "events" with 1 partition in the stream
    Then following the metadata changes should return the kinds "topic_created" for the stream

  Scenario: Record a two phase delete at its completion
    Given I hold the latest metadata change cursor for the groups "streams"
    When I create a stream named "changes-two-phase" without an encryption key
    And I create a topic named "events" with 1 partition in the stream
    And I delete the stream
    Then following the metadata changes should return the kinds "stream_created,stream_deleted" for the stream
    And the followed "stream_deleted" change should carry a requested op before its own op

  Scenario: Keyed stream changes carry fingerprints and no key
    Given I hold the latest metadata change cursor for the groups "streams"
    When I create a stream named "changes-keyed" with an encryption key of 32 bytes valued 21
    Then following the metadata changes should return the kinds "stream_created" for the stream
    And the followed "stream_created" change should report encryption key source "stream"
    And the followed "stream_created" change should carry the stream key fingerprint
    And the followed changes should expose no encryption key material

  @serial
  Scenario: Rebuild from current state after the cursor expired
    Given I hold the latest metadata change cursor for the groups "streams"
    When I create and delete 40 streams named "changes-churn"
    Then following the metadata changes should answer "expired"
    When I read the current state for the groups "streams"
    Then the current state should list the same streams as the stream list
    And following the metadata changes should answer "ok"

  Scenario: Reading changes needs the manage servers permission
    Given a user named "changes-reader" with password "changes-secret" and no server permissions exists
    When I authenticate as "changes-reader" with password "changes-secret"
    Then reading the metadata changes should be refused as unauthorized
