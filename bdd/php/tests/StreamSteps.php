<?php
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

declare(strict_types=1);

require_once __DIR__ . '/TestEnvironment.php';

use Iggy\Client as IggyClient;

/**
 * The server, authentication, stream, topic and rename steps the stream options and metadata
 * changes features share.
 */
trait StreamSteps
{
    private ?IggyClient $client = null;
    private ?int $streamId = null;
    private ?string $streamName = null;
    private ?string $topicName = null;
    private ?string $keyBase64 = null;

    /** Runs a shared step, and tells whether the step was one of them. */
    private function runStreamStep(string $step): bool
    {
        if ($step === 'I have a running Iggy server') {
            $this->client = new IggyClient(TestEnvironment::serverAddress());
            $this->client->connect();
            $this->client->ping();
            return true;
        }

        if ($step === 'I am authenticated as the root user') {
            $this->requireClient()->loginUser(TestEnvironment::rootUsername(), TestEnvironment::rootPassword());
            return true;
        }

        if (preg_match('/^I create a stream named "([^"]+)" with an encryption key of 32 bytes valued (\d+)$/', $step, $matches) === 1
            || preg_match('/^a stream named "([^"]+)" with an encryption key of 32 bytes valued (\d+) exists$/', $step, $matches) === 1) {
            $this->keyBase64 = base64_encode(str_repeat(chr((int) $matches[2]), 32));
            $this->createStream($matches[1], $this->keyBase64);
            return true;
        }

        if (preg_match('/^I create a stream named "([^"]+)" without an encryption key$/', $step, $matches) === 1) {
            $this->createStream($matches[1], null);
            return true;
        }

        if (preg_match('/^I create a topic named "([^"]+)" with (\d+) partition in the stream$/', $step, $matches) === 1) {
            $this->topicName = $matches[1];
            $this->requireClient()->createTopic($this->requireStreamId(), $this->topicName, (int) $matches[2], null, null, null, null);
            return true;
        }

        if (preg_match('/^I rename the stream to "([^"]+)"$/', $step, $matches) === 1) {
            $this->requireClient()->updateStream($this->requireStreamId(), name: $matches[1]);
            $this->streamName = $matches[1];
            return true;
        }

        return false;
    }

    private function createStream(string $name, ?string $key): void
    {
        $this->requireClient()->createStream($name, $key);
        $stream = $this->requireClient()->getStream($name);
        assert_not_null($stream);
        $this->streamId = $stream->id;
        $this->streamName = $name;
    }

    private function requireClient(): IggyClient
    {
        assert_not_null($this->client);
        return $this->client;
    }

    private function requireStreamId(): int
    {
        assert_not_null($this->streamId);
        return $this->streamId;
    }
}
