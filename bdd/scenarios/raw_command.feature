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

@raw-command
Feature: Raw Command API
  As a developer using Apache Iggy
  I want to send a command code with a payload directly
  So that I can call commands that have no typed SDK method

  Background:
    Given I have a running Iggy server
    And I am authenticated as the root user

  Scenario: Known command codes round-trip
    When I send a raw command with code 1 and an empty payload
    Then the raw command should succeed with an empty response

    When I send a raw command with code 10 and an empty payload
    Then the raw command should succeed with a non-empty response

  Scenario: Unknown command code is rejected
    When I send a raw command with code 60000 and an empty payload
    Then the raw command should fail with an invalid command error

  Scenario: Session control command code is rejected
    When I send a raw command with code 38 and an empty payload
    Then the raw command should fail with an invalid command error
