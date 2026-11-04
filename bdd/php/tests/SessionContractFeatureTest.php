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

use Iggy\Client as IggyClient;
use Iggy\Exception\IggyException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class SessionContractFeatureTest extends TestCase
{
    // The PHP SDK raises exceptions with the Rust error text and no code.
    private const ERROR_MESSAGES = [
        40 => 'Unauthenticated',
        58 => 'Request transiently not accepted',
    ];

    private ?IggyClient $client = null;
    private ?Throwable $lastError = null;

    #[DataProvider('scenarioCases')]
    #[Group('session-contract')]
    #[TestDox('Session contract shared BDD scenario passes for the PHP SDK')]
    public function testSessionContractScenario(string $scenarioName, array $steps): void
    {
        assert_true($scenarioName !== '', 'scenario name must not be empty');
        foreach ($steps as $step) {
            $this->runStep($step);
        }
    }

    public static function scenarioCases(): array
    {
        $cases = SharedFeatureParser::load(__DIR__ . '/../../scenarios/session_contract.feature');

        // The PHP SDK has no sign-out.
        return array_filter($cases, static fn (array $case): bool => !in_array('I sign out', $case[1], true));
    }

    private function runStep(string $step): void
    {
        if ($step === 'I have a running Iggy server') {
            $this->client = new IggyClient(TestEnvironment::serverAddress());
            $this->client->connect();
            $this->client->ping();
            return;
        }
        if ($step === 'I have a cluster node that cannot reach quorum') {
            $this->client = new IggyClient(TestEnvironment::noQuorumAddress());
            $this->client->connect();
            return;
        }
        if ($step === 'I am authenticated as the root user') {
            $this->requireClient()->loginUser(
                TestEnvironment::rootUsername(),
                TestEnvironment::rootPassword(),
            );
            return;
        }
        if (preg_match('/^I request the stream with ID (\d+)$/', $step, $matches) === 1) {
            $this->recordCallError(fn () => $this->requireClient()->getStream((int) $matches[1]));
            return;
        }
        if ($step === 'I sign in to that node as the root user') {
            $this->recordCallError(fn () => $this->requireClient()->loginUser(
                TestEnvironment::rootUsername(),
                TestEnvironment::rootPassword(),
            ));
            return;
        }
        if (preg_match('/^the call should fail with error code (\d+)$/', $step, $matches) === 1) {
            assert_instance_of(IggyException::class, $this->lastError);
            assert_true(
                str_contains((string) $this->lastError?->getMessage(), self::ERROR_MESSAGES[(int) $matches[1]]),
                (string) $this->lastError?->getMessage(),
            );
            return;
        }

        self::fail("Unsupported BDD step: {$step}");
    }

    private function recordCallError(callable $call): void
    {
        try {
            $call();
            $this->lastError = null;
        } catch (Throwable $error) {
            $this->lastError = $error;
        }
    }

    private function requireClient(): IggyClient
    {
        assert_not_null($this->client, 'BDD client has not been initialized');
        return $this->client;
    }
}
