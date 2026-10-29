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

@deferred-polling
Feature: Deferred polling
  As a developer using Apache Iggy
  I want a poll to wait on the server until messages arrive
  So that a consumer gets a new message at once without a polling loop

  Background:
    Given I have a running Iggy server
    And I am authenticated as the root user

  Scenario: A parked deferred poll answers when a message arrives
    Given a stream named "deferred-wake" with a topic named "events" with 3 partitions exists
    When I start a deferred poll of partition 0 from offset 0 with a max wait of 10 seconds
    And I send a message with payload "wake-up" to partition 0
    Then the deferred poll should return the payload "wake-up" within 2 seconds

  Scenario: A deferred poll without messages returns empty after its max wait
    Given a stream named "deferred-idle" with a topic named "events" with 3 partitions exists
    When I start a deferred poll of partition 1 from offset 0 with a max wait of 1 second
    Then the deferred poll should return no messages between 1 and 3 seconds after it started

  Scenario: A fetch returns only the partitions that hold messages
    Given a stream named "deferred-ready" with a topic named "events" with 3 partitions exists
    When I start a fetch on partitions 0, 1 and 2 from offset 0 with a max wait of 10 seconds
    And I send a message with payload "ready" to partition 1
    Then the fetch should return only partition 1 within 2 seconds

  # The partitions get messages in descending order, so a poll that picks partitions in turn
  # parks on an idle partition in at least one round.
  Scenario: A group fetch reads the partition that holds messages
    Given a stream named "deferred-group" with a topic named "events" with 3 partitions exists
    And I am the only member of consumer group "readers" on the topic
    When I send a message with payload "first" to partition 2
    Then a group fetch with a max wait of 10 seconds should return the payload "first" within 2 seconds
    When I send a message with payload "second" to partition 1
    Then a group fetch with a max wait of 10 seconds should return the payload "second" within 2 seconds
    When I send a message with payload "third" to partition 0
    Then a group fetch with a max wait of 10 seconds should return the payload "third" within 2 seconds

  # The poll asks for 2 messages and finds 1, so it stays parked until the cancellation.
  # Without the cancellation it would expire after 1 second and commit the offset of "unseen".
  Scenario: A cancelled deferred poll does not commit its consumer offset
    Given a stream named "deferred-cancel" with a topic named "events" with 3 partitions exists
    When I send a message with payload "unseen" to partition 2
    And I start a deferred poll of partition 2 for consumer "cancel-check" with auto commit, a min count of 2 and a max wait of 1 second
    And I cancel the deferred poll after 200 milliseconds
    And I wait 2 seconds
    Then a poll of partition 2 for consumer "cancel-check" with the next strategy should return the payload "unseen"
