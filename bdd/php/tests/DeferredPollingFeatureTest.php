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

require_once __DIR__ . '/SharedFeatureParser.php';
require_once __DIR__ . '/TestEnvironment.php';

use Iggy\AutoCommit;
use Iggy\Client as IggyClient;
use Iggy\CommitPolicy;
use Iggy\Consumer as IggyConsumer;
use Iggy\PartitionReader;
use Iggy\PendingPoll;
use Iggy\PolledMessages;
use Iggy\PollingStrategy;
use Iggy\PollOptions;
use Iggy\ReceiveMessage;
use Iggy\SendMessage;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class DeferredPollingFeatureTest extends TestCase
{
    private const CONSUMER_ID = 1;
    private const MESSAGES_COUNT = 10;
    // A message that reaches the server before the call answers it at once, so a start step
    // waits until its call is parked.
    private const PARK_TIME_MICROS = 100_000;
    // The wait of a consumer whose step names none.
    private const CONSUMER_MAX_WAIT_SECONDS = 10;

    private ?IggyClient $client = null;
    private ?IggyClient $backgroundClient = null;
    private ?string $streamName = null;
    private ?string $topicName = null;
    private ?PendingPoll $deferredPoll = null;
    private ?float $deferredPollStarted = null;
    private ?PartitionReader $reader = null;
    private ?IggyConsumer $consumer = null;

    #[DataProvider('scenarioCases')]
    #[Group('deferred-polling')]
    #[TestDox('Deferred polling shared BDD scenario passes for the PHP SDK')]
    public function testDeferredPollingScenario(string $scenarioName, array $steps): void
    {
        assert_true($scenarioName !== '', 'scenario name must not be empty');

        foreach ($steps as $step) {
            $this->runStep($step);
        }
    }

    protected function tearDown(): void
    {
        $this->deferredPoll?->cancel();
        $this->consumer?->shutdown();
        $this->reader?->close();
        if ($this->client !== null && $this->streamName !== null) {
            cleanup_stream_with_topics(
                $this->client,
                $this->streamName,
                $this->topicName !== null ? [$this->topicName] : [],
            );
        }

        parent::tearDown();
    }

    public static function scenarioCases(): array
    {
        return SharedFeatureParser::load(__DIR__ . '/../../scenarios/deferred_polling.feature');
    }

    private function runStep(string $step): void
    {
        if ($step === 'I have a running Iggy server') {
            $this->client = new IggyClient(TestEnvironment::serverAddress());
            $this->client->connect();
            $this->client->ping();

            return;
        }

        if ($step === 'I am authenticated as the root user') {
            $this->requireClient()->loginUser(TestEnvironment::rootUsername(), TestEnvironment::rootPassword());

            return;
        }

        if (preg_match('/^a stream named "([^"]+)" with a topic named "([^"]+)" with (\d+) partitions exists$/', $step, $matches) === 1) {
            $client = $this->requireClient();
            if ($client->getStream($matches[1]) !== null) {
                $client->deleteStream($matches[1]);
            }
            $client->createStream($matches[1]);
            $client->createTopic($matches[1], $matches[2], (int) $matches[3]);
            $this->streamName = $matches[1];
            $this->topicName = $matches[2];

            return;
        }

        if (preg_match('/^I start a deferred poll of partition (\d+) from offset (\d+) with a max wait of (\d+) seconds?$/', $step, $matches) === 1) {
            $this->startPoll(
                (int) $matches[1],
                self::CONSUMER_ID,
                PollingStrategy::offset((int) $matches[2]),
                new PollOptions(count: self::MESSAGES_COUNT, max_wait_micros: micros((int) $matches[3])),
            );

            return;
        }

        if (preg_match('/^I start a deferred poll of partition (\d+) for consumer "([^"]+)" with the on read commit policy, a min count of (\d+) and a max wait of (\d+) seconds?$/', $step, $matches) === 1) {
            $this->startPoll(
                (int) $matches[1],
                $matches[2],
                PollingStrategy::next(),
                new PollOptions(
                    count: self::MESSAGES_COUNT,
                    max_wait_micros: micros((int) $matches[4]),
                    min_count: (int) $matches[3],
                    commit: CommitPolicy::onRead(),
                ),
            );

            return;
        }

        if (preg_match('/^I send a message with payload "([^"]+)" to partition (\d+)$/', $step, $matches) === 1) {
            $this->requireClient()->sendMessages(
                $this->requireStreamName(),
                $this->requireTopicName(),
                (int) $matches[2],
                [new SendMessage($matches[1])],
            );

            return;
        }

        if (preg_match('/^the deferred poll should return the payload "([^"]+)" within (\d+) seconds?$/', $step, $matches) === 1) {
            $polled = $this->takeDeferredPoll()->wait(micros((int) $matches[2]));
            assert_not_null($polled, "the deferred poll did not return within {$matches[2]} s");
            assert_same([$matches[1]], collect_payloads($polled->messages));

            return;
        }

        if (preg_match('/^the deferred poll should return no messages between (\d+) and (\d+) seconds after it started$/', $step, $matches) === 1) {
            assert_not_null($this->deferredPollStarted, 'deferred poll started');
            $remaining = $this->deferredPollStarted + (int) $matches[2] - microtime(true);
            $polled = $this->takeDeferredPoll()->wait(max(0, (int) ($remaining * 1_000_000)));
            $elapsed = microtime(true) - $this->deferredPollStarted;
            assert_not_null($polled, "the deferred poll did not return within {$matches[2]} s");
            assert_same([], collect_payloads($polled->messages));
            assert_true($elapsed >= (int) $matches[1], "the deferred poll returned after {$elapsed} s, before {$matches[1]} s");

            return;
        }

        if (preg_match('/^I cancel the deferred poll after (\d+) milliseconds$/', $step, $matches) === 1) {
            usleep((int) $matches[1] * 1_000);
            $poll = $this->takeDeferredPoll();
            assert_null($poll->wait(0), 'the deferred poll must still be parked when it is cancelled');
            // Dropping the parked request sends the cancellation.
            $poll->cancel();

            return;
        }

        if (preg_match('/^I wait (\d+) seconds?$/', $step, $matches) === 1) {
            sleep((int) $matches[1]);

            return;
        }

        if (preg_match('/^a poll of partition (\d+) for consumer "([^"]+)" with the next strategy should return the payload "([^"]+)"$/', $step, $matches) === 1) {
            $this->pollNextAndExpect((int) $matches[1], $matches[2], CommitPolicy::manual(), $matches[3]);

            return;
        }

        if (preg_match('/^a poll of partition (\d+) for consumer "([^"]+)" with the next strategy and the on read commit policy should return the payload "([^"]+)"$/', $step, $matches) === 1) {
            $this->pollNextAndExpect((int) $matches[1], $matches[2], CommitPolicy::onRead(), $matches[3]);

            return;
        }

        if (preg_match('/^I open a partition reader on partitions (\d+), (\d+) and (\d+) from offset (\d+) with a max wait of (\d+) seconds?$/', $step, $matches) === 1) {
            $this->reader = $this->requireClient()->reader(
                $this->requireStreamName(),
                $this->requireTopicName(),
                self::CONSUMER_ID,
                [(int) $matches[1], (int) $matches[2], (int) $matches[3]],
                PollingStrategy::offset((int) $matches[4]),
                new PollOptions(count: self::MESSAGES_COUNT, max_wait_micros: micros((int) $matches[5])),
            );

            return;
        }

        if (preg_match('/^the partition reader should return the payload "([^"]+)" from partition (\d+) within (\d+) seconds?$/', $step, $matches) === 1) {
            assert_not_null($this->reader, 'partition reader open');
            // The reader never yields an empty batch, so its first batch comes from the first
            // partition that gets a message.
            $polled = $this->reader->next(micros((int) $matches[3]));
            assert_not_null($polled, "the partition reader did not return within {$matches[3]} s");
            assert_same((int) $matches[2], $polled->partition_id);
            assert_same([$matches[1]], collect_payloads($polled->messages));

            return;
        }

        if (preg_match('/^I open a consumer in group "([^"]+)" on the topic with a max wait of (\d+) seconds?$/', $step, $matches) === 1) {
            $this->openConsumer(
                $matches[1],
                new PollOptions(count: self::MESSAGES_COUNT, max_wait_micros: micros((int) $matches[2])),
            );

            return;
        }

        if (preg_match('/^I open a consumer in group "([^"]+)" on the topic that commits after processing$/', $step, $matches) === 1) {
            // The default AutoCommit counts a message after processing.
            $this->openConsumer(
                $matches[1],
                new PollOptions(
                    count: self::MESSAGES_COUNT,
                    max_wait_micros: micros(self::CONSUMER_MAX_WAIT_SECONDS),
                    commit: CommitPolicy::auto(AutoCommit::default()),
                ),
            );

            return;
        }

        if (preg_match('/^the consumer should return the payload "([^"]+)" within (\d+) seconds?$/', $step, $matches) === 1) {
            assert_not_null($this->consumer, 'consumer open');
            $payloads = [];
            $count = $this->consumer->consumeMessages(
                static function (ReceiveMessage $message) use (&$payloads): void {
                    $payloads[] = $message->payload();
                },
                1,
                micros((int) $matches[2]),
            );
            assert_same(1, $count, "the consumer did not return within {$matches[2]} s");
            assert_same([$matches[1]], $payloads);

            return;
        }

        if ($step === 'I close the consumer') {
            assert_not_null($this->consumer, 'consumer open');
            $consumer = $this->consumer;
            $this->consumer = null;
            $consumer->shutdown();

            return;
        }

        self::fail("Unsupported BDD step: {$step}");
    }

    private function startPoll(int $partitionId, int|string $consumer, PollingStrategy $strategy, PollOptions $options): void
    {
        $this->deferredPollStarted = microtime(true);
        $this->deferredPoll = $this->backgroundClient()->startPollMessages(
            $this->requireStreamName(),
            $this->requireTopicName(),
            $partitionId,
            $consumer,
            $strategy,
            $options,
        );
        usleep(self::PARK_TIME_MICROS);
        assert_null($this->deferredPoll->wait(0), 'the deferred poll must park');
    }

    // A parked poll holds its connection, so it runs on a client of its own. The test keeps
    // that client, so that a cancelled call still has a link for its cancellation.
    private function backgroundClient(): IggyClient
    {
        if ($this->backgroundClient === null) {
            $this->backgroundClient = new IggyClient(TestEnvironment::serverAddress());
            $this->backgroundClient->connect();
            $this->backgroundClient->loginUser(TestEnvironment::rootUsername(), TestEnvironment::rootPassword());
        }

        return $this->backgroundClient;
    }

    private function takeDeferredPoll(): PendingPoll
    {
        assert_not_null($this->deferredPoll, 'deferred poll started');
        $poll = $this->deferredPoll;
        $this->deferredPoll = null;

        return $poll;
    }

    // Polls one message from next, so a poll that commits stores only the offset of the payload
    // it returns.
    private function pollNextAndExpect(int $partitionId, string $consumer, CommitPolicy $commit, string $payload): void
    {
        $polled = $this->requireClient()->pollMessages(
            $this->requireStreamName(),
            $this->requireTopicName(),
            $partitionId,
            $consumer,
            PollingStrategy::next(),
            new PollOptions(count: 1, commit: $commit),
        );
        assert_instance_of(PolledMessages::class, $polled);
        assert_same([$payload], array_slice(collect_payloads($polled->messages), 0, 1));
    }

    private function openConsumer(string $group, PollOptions $options): void
    {
        $this->consumer = $this->requireClient()->consumerGroup(
            $group,
            $this->requireStreamName(),
            $this->requireTopicName(),
            null,
            $options,
        );
    }

    private function requireClient(): IggyClient
    {
        assert_not_null($this->client, 'BDD client has not been initialized');

        return $this->client;
    }

    private function requireStreamName(): string
    {
        assert_not_null($this->streamName, 'stream name has not been captured');

        return $this->streamName;
    }

    private function requireTopicName(): string
    {
        assert_not_null($this->topicName, 'topic name has not been captured');

        return $this->topicName;
    }
}
