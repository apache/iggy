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
require_once __DIR__ . '/StreamSteps.php';
require_once __DIR__ . '/TestEnvironment.php';

use Iggy\Client as IggyClient;
use Iggy\OptionSpec;
use Iggy\OptionsScope;
use Iggy\PollingStrategy;
use Iggy\SendMessage;
use Iggy\StreamDetails;
use Iggy\StreamOptionKey;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

final class StreamOptionsFeatureTest extends TestCase
{
    use StreamSteps;

    private bool $updateRejected = false;
    /** @var array<string, OptionSpec> */
    private array $streamOptionSpecs = [];

    public static function scenarioCases(): array
    {
        return SharedFeatureParser::load(
            __DIR__ . '/../../scenarios/stream_options.feature',
            ['@message-headers'],
        );
    }

    #[DataProvider('scenarioCases')]
    #[Group('stream-options')]
    public function testStreamOptionsScenario(string $scenarioName, array $steps): void
    {
        assert_true($scenarioName !== '', 'scenario name must not be empty');
        foreach ($steps as $step) {
            $this->runStep($step);
        }
    }

    protected function tearDown(): void
    {
        if ($this->client !== null && $this->streamName !== null) {
            cleanup_stream_with_topics(
                $this->client,
                $this->streamName,
                $this->topicName === null ? [] : [$this->topicName],
            );
        }

        parent::tearDown();
    }

    private function runStep(string $step): void
    {
        if ($this->runStreamStep($step)) {
            return;
        }

        if (preg_match('/^the stream should report encryption key source "([^"]+)"$/', $step, $matches) === 1) {
            assert_same($matches[1], $this->requireStream()->encryption_key_source);
            return;
        }

        if ($step === 'the stream encryption key fingerprint should match the key') {
            assert_not_null($this->keyBase64);
            assert_same(
                IggyClient::encryptionKeyFingerprint($this->keyBase64),
                $this->requireStream()->encryption_key_fingerprint,
            );
            return;
        }

        if ($step === 'the stream should report an encryption key fingerprint') {
            self::assertMatchesRegularExpression('/^[0-9a-f]{16}$/', (string) $this->requireStream()->encryption_key_fingerprint);
            return;
        }

        if ($step === 'the stream should report at least one KEK fingerprint') {
            $fingerprints = $this->requireStream()->encryption_kek_fingerprint;
            assert_not_null($fingerprints);
            foreach (explode(',', $fingerprints) as $fingerprint) {
                self::assertMatchesRegularExpression('/^[0-9a-f]{16}$/', $fingerprint);
            }
            return;
        }

        if ($step === 'the stream should expose no encryption key material') {
            $stream = $this->requireStream();
            foreach (['encryption_key', 'encryption_key_wrapped', 'critical_keys'] as $secret) {
                self::assertArrayNotHasKey($secret, $stream->options);
                self::assertArrayNotHasKey($secret, $stream->derived_options);
            }
            return;
        }

        if (preg_match('/^I send a message with payload "([^"]+)" to the topic$/', $step, $matches) === 1) {
            assert_not_null($this->topicName);
            $this->requireClient()->sendMessages($this->requireStreamId(), $this->topicName, 0, [new SendMessage($matches[1])]);
            return;
        }

        if (preg_match('/^polling the topic should return the payload "([^"]+)"$/', $step, $matches) === 1) {
            assert_not_null($this->topicName);
            $messages = $this->requireClient()->pollMessages(
                $this->requireStreamId(), $this->topicName, 0, PollingStrategy::offset(0), 10, false,
            );
            assert_same([$matches[1]], collect_payloads($messages));
            return;
        }

        if (preg_match('/^the stream name should be "([^"]+)"$/', $step, $matches) === 1) {
            assert_same($matches[1], $this->requireStream()->name);
            return;
        }

        if (preg_match('/^I request a reset of the stream option "([^"]+)"$/', $step, $matches) === 1) {
            assert_throws(
                fn () => $this->requireClient()->updateStream(
                    $this->requireStreamId(), reset: [StreamOptionKey::from($matches[1])],
                ),
                'Option cannot be updated after creation',
            );
            $this->updateRejected = true;
            return;
        }

        if ($step === 'I describe the stream options') {
            foreach ($this->requireClient()->describeOptions(OptionsScope::Stream) as $spec) {
                $this->streamOptionSpecs[$spec->key] = $spec;
            }
            return;
        }

        if (preg_match('/^the stream option "([^"]+)" should be updatable and not secret$/', $step, $matches) === 1) {
            $spec = $this->describedOption($matches[1]);
            assert_true($spec->updatable && !$spec->secret, "{$matches[1]} must be updatable and not secret");
            return;
        }

        if (preg_match('/^the stream option "([^"]+)" should be create only and secret$/', $step, $matches) === 1) {
            $spec = $this->describedOption($matches[1]);
            assert_true(!$spec->updatable && $spec->secret, "{$matches[1]} must be create only and secret");
            return;
        }

        if (preg_match('/^the stream option "([^"]+)" should be read only$/', $step, $matches) === 1) {
            assert_true($this->describedOption($matches[1])->read_only, "{$matches[1]} must be read only");
            return;
        }

        if ($step === 'the stream update should be rejected') {
            assert_true($this->updateRejected, 'the reset must be rejected');
            return;
        }

        self::fail("Unsupported BDD step: {$step}");
    }

    private function describedOption(string $key): OptionSpec
    {
        assert_true(array_key_exists($key, $this->streamOptionSpecs), "{$key} must be described");

        return $this->streamOptionSpecs[$key];
    }

    private function requireStream(): StreamDetails
    {
        $stream = $this->requireClient()->getStream($this->requireStreamId());
        assert_not_null($stream);
        return $stream;
    }
}
