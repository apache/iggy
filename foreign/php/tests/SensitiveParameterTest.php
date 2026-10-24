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

use Iggy\Client as IggyClient;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class SensitiveParameterTest extends TestCase
{
    private array $previousIni = [];

    public static function sensitiveParameters(): array
    {
        return [
            'connection string' => ['fromConnectionString', 'connection_string'],
            'password' => ['loginUser', 'password'],
            'raw binary payload' => ['sendBinaryRequest', 'payload'],
            'stream key' => ['createStream', 'encryption_key'],
            'fingerprinted key' => ['encryptionKeyFingerprint', 'key'],
            'raw stream options' => ['createStream', 'raw'],
            'raw stream update' => ['updateStream', 'raw'],
            'raw topic options' => ['createTopic', 'raw'],
            'raw topic update' => ['updateTopic', 'raw'],
        ];
    }

    // A raw entry may name a secret this build does not know, so every raw array is hidden.
    public static function rawOptionCalls(): array
    {
        return [
            'createStream' => ['createStream', 2, static fn (IggyClient $client, array $raw) => $client->createStream('never-sent', null, $raw, 'unexpected')],
            'updateStream' => ['updateStream', 3, static fn (IggyClient $client, array $raw) => $client->updateStream('never-sent', null, [], $raw, null, 'unexpected')],
            'createTopic' => ['createTopic', 13, static fn (IggyClient $client, array $raw) => $client->createTopic('never-sent', 'never-sent', 1, null, null, null, null, null, null, null, null, null, null, $raw, 'unexpected')],
            'updateTopic' => ['updateTopic', 6, static fn (IggyClient $client, array $raw) => $client->updateTopic('never-sent', 'never-sent', null, null, null, [], $raw, null, 'unexpected')],
        ];
    }

    protected function setUp(): void
    {
        parent::setUp();
        // Distribution php.ini files turn argument capture off, which would hide a leak.
        foreach (['zend.exception_ignore_args' => '0', 'zend.exception_string_param_max_len' => '15'] as $name => $value) {
            $previous = ini_set($name, $value);
            if ($previous !== false) {
                $this->previousIni[$name] = $previous;
            }
        }
    }

    protected function tearDown(): void
    {
        foreach ($this->previousIni as $name => $value) {
            ini_set($name, $value);
        }

        parent::tearDown();
    }

    #[DataProvider('sensitiveParameters')]
    #[TestDox('The $parameter parameter of $method is marked SensitiveParameter')]
    public function testGivenSecretParameterWhenReflectingShouldCarrySensitiveParameter(string $method, string $parameter): void
    {
        $parameters = (new ReflectionMethod(IggyClient::class, $method))->getParameters();
        $matching = array_values(array_filter(
            $parameters,
            static fn (ReflectionParameter $candidate): bool => $candidate->getName() === $parameter,
        ));

        assert_count(1, $matching, "{$method} has no parameter {$parameter}");
        assert_count(1, $matching[0]->getAttributes(SensitiveParameter::class), "{$method}({$parameter})");
    }

    // Engine argument errors are raised inside the native frame, so its arguments land in the trace.
    #[TestDox('A stream key never reaches the trace when createStream rejects another argument')]
    public function testGivenStreamKeyWhenCreateRejectsArgumentShouldHideKeyInTrace(): void
    {
        $key = base64_encode(random_bytes(32));
        $client = new IggyClient(server_address());

        $error = assert_throws(static fn () => $client->createStream('never-sent', $key, 'not-an-array'));

        assert_instance_of(TypeError::class, $error);
        self::assertHidden($error, 'createStream', 1, $key);
    }

    #[DataProvider('rawOptionCalls')]
    #[TestDox('A raw stream key never reaches the trace when $method rejects the call')]
    public function testGivenRawStreamKeyWhenCallRejectsArgumentsShouldHideRawOptionsInTrace(string $method, int $position, callable $call): void
    {
        $key = base64_encode(random_bytes(32));
        $client = new IggyClient(server_address());

        $error = assert_throws(static fn () => $call($client, ['encryption_key' => $key]));

        assert_instance_of(ArgumentCountError::class, $error);
        self::assertHidden($error, $method, $position, $key);
    }

    #[TestDox('A stream key never reaches the trace when encryptionKeyFingerprint rejects the call')]
    public function testGivenStreamKeyWhenFingerprintRejectsCallShouldHideKeyInTrace(): void
    {
        $key = base64_encode(random_bytes(32));

        $error = assert_throws(static fn () => IggyClient::encryptionKeyFingerprint($key, 'unexpected'));

        assert_instance_of(ArgumentCountError::class, $error);
        self::assertHidden($error, 'encryptionKeyFingerprint', 0, $key);
    }

    #[TestDox('A password never reaches the trace when loginUser rejects the call')]
    public function testGivenPasswordWhenLoginRejectsCallShouldHidePasswordInTrace(): void
    {
        $password = bin2hex(random_bytes(16));
        $client = new IggyClient(server_address());

        $error = assert_throws(static fn () => $client->loginUser('iggy', $password, 'unexpected'));

        assert_instance_of(ArgumentCountError::class, $error);
        self::assertHidden($error, 'loginUser', 1, $password);
    }

    #[TestDox('Connection string credentials never reach the trace when fromConnectionString rejects the call')]
    public function testGivenConnectionStringWhenFactoryRejectsCallShouldHideCredentialsInTrace(): void
    {
        $connectionString = 'iggy+tcp://iggy:' . bin2hex(random_bytes(16)) . '@' . server_address();

        $error = assert_throws(static fn () => IggyClient::fromConnectionString($connectionString, 'unexpected'));

        assert_instance_of(ArgumentCountError::class, $error);
        self::assertHidden($error, 'fromConnectionString', 0, $connectionString);
    }

    private static function assertHidden(Throwable $error, string $method, int $position, string $secret): void
    {
        $frames = array_values(array_filter(
            $error->getTrace(),
            static fn (array $frame): bool => ($frame['class'] ?? null) === IggyClient::class
                && $frame['function'] === $method,
        ));

        assert_count(1, $frames, "{$method} frame in the trace");
        Assert::assertArrayHasKey('args', $frames[0], 'exception arguments are captured');
        assert_instance_of(SensitiveParameterValue::class, $frames[0]['args'][$position] ?? null, $method);
        Assert::assertStringNotContainsString(substr($secret, 0, 15), $error->getTraceAsString());
        Assert::assertStringNotContainsString($secret, print_r($frames[0]['args'], true));
    }
}
