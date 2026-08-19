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

@stream-crud
Feature: Stream CRUD operations
  As a developer using Apache Iggy
  I want to manage streams
  So that I can organize message data

  Background:
    Given I have a running Iggy server
    And I am authenticated as the root user

  Scenario: Create a stream
    When I create a stream with name "stream-crud-create"
    Then the stream should be created successfully
    And the stream should have name "stream-crud-create"
    And getting the stream by its numeric ID should return name "stream-crud-create"

  Scenario: Get a stream by numeric ID
    Given a stream with name "stream-crud-get" exists
    When I get the stream by its numeric ID
    Then the returned stream should have name "stream-crud-get"

  Scenario: List streams
    Given a stream with name "stream-crud-list" exists
    When I list all streams
    Then the stream list should contain the created stream

  Scenario: Update a stream
    Given a stream with name "stream-crud-update" exists
    When I update the stream name to "stream-crud-updated"
    Then getting the stream by its numeric ID should return name "stream-crud-updated"

  Scenario: Delete a stream
    Given a stream with name "stream-crud-delete" exists
    When I delete the stream by its numeric ID
    Then getting the stream by its numeric ID should return no stream
