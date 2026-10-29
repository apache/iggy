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

use Iggy\Client;
use Iggy\ClientCommitPolicy;
use Iggy\Exception\IggyException;
use Iggy\Exception\PendingOperationException;
use Iggy\PendingOperationsConfig;
use Iggy\PollOptions;
use Iggy\ServerCommitPolicy;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class RawCommandTest extends TestCase
{
    private const PING_CODE = 1;
    private const GET_STATS_CODE = 10;
    private const UNKNOWN_CODE = 60_000;
    private const CREATE_STREAM_CODE = 202;
    private const SERVER_REPLICA_ID = 0;
    private const UNKNOWN_REPLICA_ID = 9;
    private const INVALID_COMMAND_CODE = 3;
    private const INVALID_SIZE_BYTES_CODE = 80;
    private const INVALID_MESSAGES_COUNT_CODE = 4009;
    private const MAX_POLL_WAIT_MICROS = 600_000_000;

    #[TestDox('A raw ping command returns an empty successful response')]
    public function testRawPingReturnsEmptyResponse(): void
    {
        $client = new_client();

        $response = $client->sendBinaryRequest(self::PING_CODE, '');

        assert_same('', $response);
    }

    #[TestDox('A raw get-stats command returns a non-empty response')]
    public function testRawGetStatsReturnsNonEmptyResponse(): void
    {
        $client = new_client();

        $response = $client->sendBinaryRequest(self::GET_STATS_CODE, '');

        assert_true($response !== '', 'expected a non-empty stats response');
    }

    #[TestDox('A session-control code is rejected before reaching the server')]
    #[DataProvider('sessionControlCodes')]
    public function testRawSessionControlCodeIsRejected(int $code): void
    {
        $client = new_client();

        $throwable = assert_throws(static fn () => $client->sendBinaryRequest($code, ''));

        assert_instance_of(IggyException::class, $throwable);
    }

    #[TestDox('An unknown command code is rejected by the server')]
    public function testRawUnknownCodeIsRejectedByServer(): void
    {
        $client = new_client();

        $throwable = assert_throws(static fn () => $client->sendBinaryRequest(self::UNKNOWN_CODE, ''));

        assert_instance_of(IggyException::class, $throwable);
    }

    #[TestDox('A raw get-stats command sent to the server replica returns a non-empty response')]
    public function testRawGetStatsToTheServerReplicaReturnsNonEmptyResponse(): void
    {
        $client = new_client();

        $response = $client->sendBinaryRequestToReplica(self::GET_STATS_CODE, '', self::SERVER_REPLICA_ID);

        assert_true($response !== '', 'expected a non-empty stats response');
    }

    #[TestDox('A replica that cluster metadata does not name is rejected')]
    public function testRawCommandToAnUnknownReplicaIsRejected(): void
    {
        $client = new_client();

        $throwable = assert_throws(
            static fn () => $client->sendBinaryRequestToReplica(self::GET_STATS_CODE, '', self::UNKNOWN_REPLICA_ID)
        );

        assert_instance_of(IggyException::class, $throwable);
        assert_same(6, $throwable->getCode());
    }

    #[TestDox('A replicated command sent to a replica is rejected before reaching the server')]
    public function testRawReplicatedCommandToAReplicaIsRejected(): void
    {
        $client = new_client();

        $throwable = assert_throws(
            static fn () => $client->sendBinaryRequestToReplica(self::CREATE_STREAM_CODE, '', self::SERVER_REPLICA_ID)
        );

        assert_instance_of(IggyException::class, $throwable);
        assert_same(3, $throwable->getCode());
    }

    public static function sessionControlCodes(): array
    {
        return [[38], [39], [40], [44], [45]];
    }

    #[DataProvider('recoveryOperations')]
    public function testRecoveryPreservesExactUnsignedIdentity(string $method, string $operationId): void
    {
        $client = new Client('127.0.0.1:1');
        $error = assert_throws(static fn () => $client->$method($operationId));
        assert_instance_of(PendingOperationException::class, $error, $error->getMessage());
        assert_same($operationId, $error->operation_id);
        assert_same('not_retained', $error->kind);
        assert_same(14008, $error->getCode());
        assert_true(str_contains($error->getMessage(), $operationId));
        foreach (['message' => $error->getMessage(), 'code' => $error->getCode()] as $name => $value) {
            $property = new ReflectionProperty($error, $name);
            assert_true($property->isProtected());
            assert_same($value, $property->getValue($error));
            assert_throws(static fn () => $error->$name);
        }
        assert_throws(static function () use ($error): void {
            $error->operation_id = '0';
        });
        assert_throws(static function () use ($error): void {
            $error->kind = 'uncertain';
        });
    }

    public static function recoveryOperations(): array
    {
        return [
            ['resolveOperation', '9007199254741009'],
            ['resolveOperation', '18446744073709551615'],
            ['abandonOperation', '9007199254741009'],
            ['abandonOperation', '18446744073709551615'],
        ];
    }

    #[DataProvider('invalidOperationIds')]
    public function testRecoveryRejectsInvalidUnsignedIdentity(string $operationId): void
    {
        $client = new Client('127.0.0.1:1');
        foreach (['resolveOperation', 'abandonOperation'] as $method) {
            $error = assert_throws(static fn () => $client->$method($operationId), 'operation_id');
            assert_true(!($error instanceof PendingOperationException));
        }
    }

    public static function invalidOperationIds(): array
    {
        return [[''], ['-1'], ['+1'], [' 1'], ['1 '], ['1.0'], ['1e2'], ['18446744073709551616']];
    }

    public function testRecoveryRejectsNonpositiveTimeout(): void
    {
        $client = new Client('127.0.0.1:1');
        foreach ([0, -1] as $timeout) {
            assert_throws(static fn () => $client->resolveOperation('1', $timeout), 'timeout_micros');
        }
        foreach (['18446744073709551616', 1.5, true] as $invalid) {
            assert_throws(static fn () => $client->resolveOperation('1', $invalid), 'timeout_micros');
        }
    }

    public function testPendingOperationLimitsValidateAndConstructClient(): void
    {
        $defaults = new PendingOperationsConfig();
        assert_same(256, $defaults->max_operations);
        assert_same(64 * 1024 * 1024, $defaults->max_bytes);
        $limits = new PendingOperationsConfig(3, 4096);
        assert_same(3, $limits->max_operations);
        assert_same(4096, $limits->max_bytes);
        assert_instance_of(Client::class, new Client('127.0.0.1:1', $limits));
        assert_instance_of(Client::class, new Client('127.0.0.1:1', null));
        foreach ([new stdClass(), 1, 'limits', true, []] as $invalid) {
            assert_throws(static fn () => new Client('127.0.0.1:1', $invalid), 'pending_operations');
        }
        assert_throws(static function () use ($limits): void {
            $limits->max_operations = 4;
        });
        foreach ([0, -1] as $invalid) {
            assert_throws(static fn () => new PendingOperationsConfig($invalid, 4096), 'max_operations');
            assert_throws(static fn () => new PendingOperationsConfig(3, $invalid), 'max_bytes');
        }
        foreach (['18446744073709551616', 1.5, true] as $invalid) {
            assert_throws(static fn () => new PendingOperationsConfig($invalid), 'max_operations');
            assert_throws(static fn () => new PendingOperationsConfig(null, $invalid), 'max_bytes');
        }
    }

    public function testNullableUnsignedOptionsRejectInvalidValuesBeforeConnecting(): void
    {
        $client = new Client('127.0.0.1:1');
        $methods = [
            'createTopic' => [
                ['stream' => 's', 'name' => 't', 'partitions_count' => 1,
                    'compression_algorithm' => null, 'message_expiry_micros' => null,
                    'max_topic_size' => null, 'segment_size' => null, 'durability' => null,
                    'consumer_offset_durability' => null, 'messages_required_to_save' => null,
                    'size_of_messages_required_to_save' => null, 'preallocate_segments' => null],
                ['message_expiry_micros', 'max_topic_size', 'segment_size',
                    'messages_required_to_save', 'size_of_messages_required_to_save'],
            ],
            'consumerGroup' => [
                ['name' => 'g', 'stream' => 's', 'topic' => 't', 'polling_strategy' => null,
                    'poll_options' => null, 'client_commit' => null, 'prefetch_bytes' => null,
                    'prefetch_messages' => null, 'create_consumer_group_if_not_exists' => false,
                    'polling_retry_interval_micros' => null, 'init_retries' => null,
                    'init_retry_interval_micros' => null, 'allow_replay' => false],
                ['prefetch_bytes', 'prefetch_messages', 'polling_retry_interval_micros',
                    'init_retries', 'init_retry_interval_micros'],
            ],
            'reader' => [
                ['stream' => 's', 'topic' => 't', 'consumer' => 'c', 'partitions' => [0], 'start' => null,
                    'options' => null, 'prefetch_bytes' => null, 'prefetch_messages' => null],
                ['prefetch_bytes', 'prefetch_messages'],
            ],
            'groupReader' => [
                ['stream' => 's', 'topic' => 't', 'group' => 'g', 'start' => null, 'options' => null,
                    'prefetch_bytes' => null, 'prefetch_messages' => null],
                ['prefetch_bytes', 'prefetch_messages'],
            ],
        ];
        foreach ($methods as $method => [$required, $fields]) {
            foreach ($fields as $field) {
                foreach ([-1, 1.5, true, '18446744073709551616'] as $invalid) {
                    $arguments = array_replace($required, [$field => $invalid]);
                    assert_throws(static fn () => $client->$method(...$arguments), $field);
                }
            }
        }
        $u32Fields = [
            'consumerGroup' => ['prefetch_bytes', 'prefetch_messages', 'init_retries'],
            'reader' => ['prefetch_bytes', 'prefetch_messages'],
            'groupReader' => ['prefetch_bytes', 'prefetch_messages'],
        ];
        foreach ($u32Fields as $method => $fields) {
            foreach ($fields as $field) {
                $arguments = array_replace($methods[$method][0], [$field => 4294967296]);
                assert_throws(static fn () => $client->$method(...$arguments), $field);
            }
        }
        $arguments = array_replace($methods['createTopic'][0], ['messages_required_to_save' => 4294967296]);
        assert_throws(static fn () => $client->createTopic(...$arguments), 'messages_required_to_save');
    }

    public function testPollOptionsKeepDefaultsAndRefuseInvalidLimits(): void
    {
        $defaults = new PollOptions();
        assert_same(1000, $defaults->count);
        assert_same(0, $defaults->max_wait_micros);
        assert_same(1, $defaults->min_count);
        assert_same(1024 * 1024, $defaults->max_bytes);
        assert_same(ServerCommitPolicy::Disabled, $defaults->server_commit);

        $options = new PollOptions(10, 5_000, 2, 65_536, ServerCommitPolicy::OnRead);
        assert_same(10, $options->count);
        assert_same(5_000, $options->max_wait_micros);
        assert_same(2, $options->min_count);
        assert_same(65_536, $options->max_bytes);
        assert_same(ServerCommitPolicy::OnRead, $options->server_commit);

        foreach (['count', 'max_wait_micros', 'min_count', 'max_bytes'] as $field) {
            foreach ([-1, 1.5, true, '18446744073709551616'] as $invalid) {
                assert_throws(static fn () => new PollOptions(...[$field => $invalid]), $field);
            }
        }
        foreach (['count', 'min_count', 'max_bytes'] as $field) {
            assert_throws(static fn () => new PollOptions(...[$field => 4294967296]), $field);
        }
        assert_same(
            self::INVALID_MESSAGES_COUNT_CODE,
            assert_throws(static fn () => new PollOptions(count: 2, min_count: 3))->getCode(),
        );
        assert_same(
            self::INVALID_MESSAGES_COUNT_CODE,
            assert_throws(static fn () => new PollOptions(min_count: 0))->getCode(),
        );
        assert_same(
            self::INVALID_COMMAND_CODE,
            assert_throws(static fn () => new PollOptions(max_wait_micros: self::MAX_POLL_WAIT_MICROS + 1))->getCode(),
        );
        assert_same(
            self::INVALID_SIZE_BYTES_CODE,
            assert_throws(static fn () => new PollOptions(max_bytes: 1))->getCode(),
        );
    }

    public function testClientCommitPolicyRefusesInvalidIntervals(): void
    {
        foreach ([0, -1, 1.5, true, '18446744073709551616'] as $invalid) {
            assert_throws(static fn () => new ClientCommitPolicy($invalid), 'interval_micros');
        }
    }

    public function testReaderRefusesAnEmptyPartitionListBeforeConnecting(): void
    {
        $client = new Client('127.0.0.1:1');

        assert_same(
            self::INVALID_COMMAND_CODE,
            assert_throws(static fn () => $client->reader('s', 't', 'c', []))->getCode(),
        );
    }

    public function testConnectionStringBudgetsReachEveryBinaryTransport(): void
    {
        foreach (['tcp', 'quic', 'ws'] as $transport) {
            $address = "iggy+{$transport}://iggy:iggy@127.0.0.1:1";
            assert_instance_of(Client::class, Client::fromConnectionString(
                $address . '?max_pending_operations=3&max_pending_operation_bytes=4096',
            ));
            foreach (['max_pending_operations', 'max_pending_operation_bytes'] as $field) {
                assert_throws(static fn () => Client::fromConnectionString($address . "?{$field}=0"));
            }
        }
    }
}
