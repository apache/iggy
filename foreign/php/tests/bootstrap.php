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

final class TestFailure extends RuntimeException
{
}

function assert_true(bool $condition, string $message = 'expected condition to be true'): void
{
    if (!$condition) {
        throw new TestFailure($message);
    }
}

function assert_same(mixed $expected, mixed $actual, string $message = ''): void
{
    if ($expected !== $actual) {
        $message = $message !== '' ? $message . ': ' : '';
        throw new TestFailure($message . 'expected ' . var_export($expected, true) . ', got ' . var_export($actual, true));
    }
}

function assert_not_null(mixed $value, string $message = 'expected value not to be null'): void
{
    if ($value === null) {
        throw new TestFailure($message);
    }
}

function assert_null(mixed $value, string $message = 'expected value to be null'): void
{
    if ($value !== null) {
        throw new TestFailure($message . ', got ' . var_export($value, true));
    }
}

function assert_throws(callable $callback, ?string $messageContains = null): Throwable
{
    try {
        $callback();
    } catch (Throwable $throwable) {
        if ($messageContains !== null && !str_contains($throwable->getMessage(), $messageContains)) {
            throw new TestFailure(
                'expected exception message to contain ' . var_export($messageContains, true)
                . ', got ' . var_export($throwable->getMessage(), true)
            );
        }

        return $throwable;
    }

    throw new TestFailure('expected callable to throw');
}

function unique_name(string $prefix): string
{
    return $prefix . '-' . bin2hex(random_bytes(4));
}

function env_or_default(string $name, string $default): string
{
    $value = getenv($name);

    return $value === false || $value === '' ? $default : $value;
}

function server_host(): string
{
    return env_or_default('IGGY_HOST', '127.0.0.1');
}

function server_port(): int
{
    return (int) env_or_default('IGGY_PORT', '8090');
}

function wait_for_server(string $host, int $port, int $timeoutSeconds = 30): void
{
    $deadline = microtime(true) + $timeoutSeconds;
    $lastError = null;

    while (microtime(true) < $deadline) {
        $socket = @fsockopen($host, $port, $errno, $errstr, 1.0);
        if (is_resource($socket)) {
            fclose($socket);

            return;
        }

        $lastError = trim($errstr !== '' ? $errstr : (string) $errno);
        usleep(250_000);
    }

    throw new TestFailure("Iggy server was not reachable at {$host}:{$port}" . ($lastError !== null ? " ({$lastError})" : ''));
}

function new_client(): IggyClient
{
    $client = new IggyClient(server_host() . ':' . server_port());
    $client->connect();
    $client->loginUser(env_or_default('IGGY_USERNAME', 'iggy'), env_or_default('IGGY_PASSWORD', 'iggy'));

    return $client;
}

function new_connection_string_client(): IggyClient
{
    $host = server_host();
    $port = server_port();
    $username = rawurlencode(env_or_default('IGGY_USERNAME', 'iggy'));
    $password = rawurlencode(env_or_default('IGGY_PASSWORD', 'iggy'));

    $client = IggyClient::fromConnectionString("iggy+tcp://{$username}:{$password}@{$host}:{$port}");
    $client->connect();

    return $client;
}

function create_stream_and_topic(object $client, string $stream, string $topic, int $partitions = 1): void
{
    $client->createStream($stream);
    $client->createTopic($stream, $topic, $partitions, null, null, null, null);
}

function collect_payloads(array $messages): array
{
    return array_map(static fn (ReceiveMessage $message): string => $message->payload(), $messages);
}

function micros(int $seconds): int
{
    return $seconds * 1_000_000;
}
