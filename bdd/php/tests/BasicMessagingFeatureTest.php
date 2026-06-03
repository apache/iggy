<?php

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

declare(strict_types=1);

use Iggy\Client as IggyClient;
use Iggy\PollingStrategy;
use Iggy\SendMessage;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class BasicMessagingFeatureTest extends TestCase
{
    private ?IggyClient $client = null;
    private ?int $lastStreamId = null;
    private ?string $lastStreamName = null;
    private ?string $lastTopicName = null;
    private ?int $lastTopicPartitions = null;
    private array $lastPolledMessages = [];
    private array $sentPayloads = [];
    private ?int $lastSentMessageCount = null;

    #[TestDox('Basic messaging shared BDD scenario passes for the PHP SDK')]
    public function testBasicMessagingScenario(): void
    {
        foreach ($this->scenarioSteps() as $step) {
            $this->runStep($step);
        }
    }

    protected function tearDown(): void
    {
        if ($this->client !== null && $this->lastStreamName !== null) {
            cleanup_stream_with_topics(
                $this->client,
                $this->lastStreamName,
                $this->lastTopicName !== null ? [$this->lastTopicName] : [],
            );
        }

        parent::tearDown();
    }

    private function scenarioSteps(): array
    {
        $featureFile = getenv('BDD_FEATURE_FILE') ?: __DIR__ . '/../../scenarios/basic_messaging.feature';
        assert_true(is_file($featureFile), "feature file not found at {$featureFile}");

        $lines = file($featureFile, FILE_IGNORE_NEW_LINES);
        assert_true($lines !== false, "failed to read feature file at {$featureFile}");

        $steps = [];
        $insideTargetScenario = false;
        foreach ($lines as $line) {
            $line = trim($line);
            if ($line === '') {
                continue;
            }

            if (str_starts_with($line, 'Scenario:')) {
                $insideTargetScenario = $line === 'Scenario: Create stream and send messages';
                continue;
            }

            if (!$insideTargetScenario) {
                continue;
            }

            if (preg_match('/^(Given|When|Then|And) (.+)$/', $line, $matches) === 1) {
                $steps[] = $matches[2];
            }
        }

        assert_true($steps !== [], 'no BDD steps were loaded from the basic messaging scenario');

        return [
            'I have a running Iggy server',
            'I am authenticated as the root user',
            ...$steps,
        ];
    }

    private function runStep(string $step): void
    {
        if ($step === 'I have a running Iggy server') {
            $this->client = new IggyClient(server_host() . ':' . server_port());
            $this->client->connect();
            $this->client->ping();

            return;
        }

        if ($step === 'I am authenticated as the root user') {
            $this->requireClient()->loginUser(env_or_default('IGGY_USERNAME', 'iggy'), env_or_default('IGGY_PASSWORD', 'iggy'));

            return;
        }

        if ($step === 'I have no streams in the system') {
            return;
        }

        if (preg_match('/^I create a stream with name "([^"]+)"$/', $step, $matches) === 1) {
            $streamName = $matches[1];
            $this->requireClient()->createStream($streamName);
            $stream = $this->requireClient()->getStream($streamName);
            assert_not_null($stream);
            $this->lastStreamId = $stream->id;
            $this->lastStreamName = $streamName;

            return;
        }

        if ($step === 'the stream should be created successfully') {
            assert_not_null($this->requireClient()->getStream($this->requireStreamName()));

            return;
        }

        if (preg_match('/^the stream should have name "([^"]+)"$/', $step, $matches) === 1) {
            $stream = $this->requireClient()->getStream($matches[1]);
            assert_not_null($stream);
            assert_same($matches[1], $stream->name);

            return;
        }

        if (preg_match('/^I create a topic with name "([^"]+)" in stream (\d+) with (\d+) partitions$/', $step, $matches) === 1) {
            $topicName = $matches[1];
            $streamId = (int) $matches[2];
            $partitions = (int) $matches[3];

            $this->requireClient()->createTopic($streamId, $topicName, $partitions, null, null, null, null);
            $topic = $this->requireClient()->getTopic($streamId, $topicName);
            assert_not_null($topic);
            $this->lastTopicName = $topicName;
            $this->lastTopicPartitions = $partitions;

            return;
        }

        if ($step === 'the topic should be created successfully') {
            assert_not_null($this->requireClient()->getTopic($this->requireStreamId(), $this->requireTopicName()));

            return;
        }

        if (preg_match('/^the topic should have name "([^"]+)"$/', $step, $matches) === 1) {
            $topic = $this->requireClient()->getTopic($this->requireStreamId(), $matches[1]);
            assert_not_null($topic);
            assert_same($matches[1], $topic->name);

            return;
        }

        if (preg_match('/^the topic should have (\d+) partitions$/', $step, $matches) === 1) {
            $topic = $this->requireClient()->getTopic($this->requireStreamId(), $this->requireTopicName());
            assert_not_null($topic);
            assert_same((int) $matches[1], $topic->partitions_count);
            assert_same((int) $matches[1], $this->lastTopicPartitions);

            return;
        }

        if (preg_match('/^I send (\d+) messages to stream (\d+), topic (\d+), partition (\d+)$/', $step, $matches) === 1) {
            $messageCount = (int) $matches[1];
            $this->lastSentMessageCount = $messageCount;
            $this->sentPayloads = array_map(
                static fn (int $index): string => "bdd-php-message-{$index}",
                range(0, $messageCount - 1),
            );

            $this->requireClient()->sendMessages(
                (int) $matches[2],
                (int) $matches[3],
                (int) $matches[4],
                array_map(static fn (string $payload): SendMessage => new SendMessage($payload), $this->sentPayloads),
            );

            return;
        }

        if ($step === 'all messages should be sent successfully') {
            assert_not_null($this->lastSentMessageCount, 'sent message count has not been captured');
            assert_count($this->lastSentMessageCount, $this->sentPayloads);

            return;
        }

        if (preg_match('/^I poll messages from stream (\d+), topic (\d+), partition (\d+) starting from offset (\d+)$/', $step, $matches) === 1) {
            $this->lastPolledMessages = $this->requireClient()->pollMessages(
                (int) $matches[1],
                (int) $matches[2],
                (int) $matches[3],
                PollingStrategy::offset((int) $matches[4]),
                10,
                true,
            );

            return;
        }

        if (preg_match('/^I should receive (\d+) messages$/', $step, $matches) === 1) {
            assert_count((int) $matches[1], $this->lastPolledMessages);

            return;
        }

        if (preg_match('/^the messages should have sequential offsets from (\d+) to (\d+)$/', $step, $matches) === 1) {
            assert_same(range((int) $matches[1], (int) $matches[2]), collect_offsets($this->lastPolledMessages));

            return;
        }

        if ($step === 'each message should have the expected payload content') {
            assert_same($this->sentPayloads, collect_payloads($this->lastPolledMessages));

            return;
        }

        if ($step === 'the last polled message should match the last sent message') {
            $lastSentPayload = $this->sentPayloads[array_key_last($this->sentPayloads)];
            $lastPolledMessage = $this->lastPolledMessages[array_key_last($this->lastPolledMessages)];

            assert_same($lastSentPayload, $lastPolledMessage->payload());

            return;
        }

        self::fail("Unsupported BDD step: {$step}");
    }

    private function requireClient(): IggyClient
    {
        assert_not_null($this->client, 'BDD client has not been initialized');

        return $this->client;
    }

    private function requireStreamId(): int
    {
        assert_not_null($this->lastStreamId, 'stream id has not been captured');

        return $this->lastStreamId;
    }

    private function requireStreamName(): string
    {
        assert_not_null($this->lastStreamName, 'stream name has not been captured');

        return $this->lastStreamName;
    }

    private function requireTopicName(): string
    {
        assert_not_null($this->lastTopicName, 'topic name has not been captured');

        return $this->lastTopicName;
    }
}
