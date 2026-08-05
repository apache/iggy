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

use Iggy\Client as IggyClient;
use Iggy\Exception\IggyException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class RawCommandFeatureTest extends TestCase
{
    private ?IggyClient $client = null;
    private ?string $lastResponse = null;
    private ?Throwable $lastError = null;

    #[DataProvider('scenarioCases')]
    #[Group('raw-command')]
    #[TestDox('Raw command shared BDD scenario passes for the PHP SDK')]
    public function testRawCommandScenario(string $scenarioName, array $steps): void
    {
        assert_true($scenarioName !== '', 'scenario name must not be empty');
        foreach ($steps as $step) {
            $this->runStep($step);
        }
    }

    public static function scenarioCases(): array
    {
        return SharedFeatureParser::load(__DIR__ . '/../../scenarios/raw_command.feature');
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
            $this->requireClient()->loginUser(
                env_or_default('IGGY_USERNAME', 'iggy'),
                env_or_default('IGGY_PASSWORD', 'iggy'),
            );
            return;
        }
        if (preg_match('/^I send a raw command with code (\d+) and an empty payload$/', $step, $matches) === 1) {
            try {
                $this->lastResponse = $this->requireClient()->sendBinaryRequest((int) $matches[1], '');
                $this->lastError = null;
            } catch (Throwable $error) {
                $this->lastResponse = null;
                $this->lastError = $error;
            }
            return;
        }
        if ($step === 'the raw command should succeed with an empty response') {
            assert_same(null, $this->lastError);
            assert_same('', $this->lastResponse);
            return;
        }
        if ($step === 'the raw command should succeed with a non-empty response') {
            assert_same(null, $this->lastError);
            assert_true($this->lastResponse !== null && $this->lastResponse !== '');
            return;
        }
        if ($step === 'the raw command should fail with an invalid command error') {
            assert_same(null, $this->lastResponse);
            assert_instance_of(IggyException::class, $this->lastError);
            assert_true(str_contains(strtolower((string) $this->lastError?->getMessage()), 'invalid command'));
            return;
        }

        self::fail("Unsupported BDD step: {$step}");
    }

    private function requireClient(): IggyClient
    {
        assert_not_null($this->client, 'BDD client has not been initialized');
        return $this->client;
    }
}
