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
  I want a poll, a partition reader and a consumer to wait on the server until messages arrive
  So that a new message reaches me at once without a polling loop

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

  Scenario: A poll that commits on read stores the offset of its last message
    Given a stream named "deferred-on-read" with a topic named "events" with 3 partitions exists
    When I send a message with payload "one" to partition 0
    And I send a message with payload "two" to partition 0
    Then a poll of partition 0 for consumer "on-read" with the next strategy and the on read commit policy should return the payload "one"
    And a poll of partition 0 for consumer "on-read" with the next strategy should return the payload "two"

  # The poll asks for 2 messages and finds 1, so it stays parked until the cancellation.
  # Without the cancellation it would expire after 1 second and store the offset of "unseen".
  Scenario: A cancelled deferred poll does not commit its consumer offset
    Given a stream named "deferred-cancel" with a topic named "events" with 3 partitions exists
    When I send a message with payload "unseen" to partition 2
    And I start a deferred poll of partition 2 for consumer "cancel-check" with the on read commit policy, a min count of 2 and a max wait of 1 second
    And I cancel the deferred poll after 200 milliseconds
    And I wait 2 seconds
    Then a poll of partition 2 for consumer "cancel-check" with the next strategy should return the payload "unseen"

  Scenario: A partition reader returns the partition that holds messages
    Given a stream named "deferred-reader" with a topic named "events" with 3 partitions exists
    When I open a partition reader on partitions 0, 1 and 2 from offset 0 with a max wait of 10 seconds
    And I send a message with payload "ready" to partition 1
    Then the partition reader should return the payload "ready" from partition 1 within 2 seconds

  # The partitions get messages in descending order, so a consumer that reads partitions in
  # turn waits on an idle partition in at least one round.
  Scenario: A group consumer reads every partition that holds messages
    Given a stream named "deferred-group" with a topic named "events" with 3 partitions exists
    When I open a consumer in group "readers" on the topic with a max wait of 10 seconds
    And I send a message with payload "first" to partition 2
    Then the consumer should return the payload "first" within 2 seconds
    When I send a message with payload "second" to partition 1
    Then the consumer should return the payload "second" within 2 seconds
    When I send a message with payload "third" to partition 0
    Then the consumer should return the payload "third" within 2 seconds

  # Closing the consumer counts the message it handed over last as processed and stores it.
  Scenario: A group consumer that commits after processing resumes after its last message
    Given a stream named "deferred-resume" with a topic named "events" with 3 partitions exists
    When I send a message with payload "processed" to partition 0
    And I open a consumer in group "resumers" on the topic that commits after processing
    Then the consumer should return the payload "processed" within 2 seconds
    When I close the consumer
    And I send a message with payload "next" to partition 0
    And I open a consumer in group "resumers" on the topic that commits after processing
    Then the consumer should return the payload "next" within 2 seconds

  # The connection drops while a committing poll waits, so its outcome stays unknown, and the
  # partition takes no committing poll until the caller abandons that poll.
  Scenario: A committing poll whose reply is lost blocks its partition until abandoned
    Given a stream named "deferred-lost" with a topic named "events" with 3 partitions exists
    When I start a deferred poll of partition 0 for consumer "lost-check" with the on read commit policy and a max wait of 10 seconds
    And the client disconnects before the deferred poll answers
    Then the deferred poll should fail as uncertain
    When the client connects again
    Then a poll of partition 0 for consumer "lost-check" with the next strategy and the on read commit policy should fail as blocked by the deferred poll
    When I abandon the operation of the deferred poll
    And I send a message with payload "after" to partition 0
    Then a poll of partition 0 for consumer "lost-check" with the next strategy and the on read commit policy should return the payload "after"
