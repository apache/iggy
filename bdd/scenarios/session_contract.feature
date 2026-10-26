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

@session-contract
Feature: Session contract
  As a developer using Apache Iggy
  I want every SDK to end a call with the same error code when it has no session or no primary
  So that I can handle a sign-out and an unavailable cluster the same way in every language

  Scenario: A call after sign-out fails as unauthenticated
    Given I have a running Iggy server
    And I am authenticated as the root user
    When I sign out
    And I request the stream with ID 1
    Then the call should fail with error code 40

  Scenario: A sign-in that no primary accepts fails at its deadline
    Given I have a cluster node that cannot reach quorum
    When I sign in to that node as the root user
    Then the call should fail with error code 58
