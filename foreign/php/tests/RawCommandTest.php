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

use Iggy\Exception\IggyException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class RawCommandTest extends TestCase
{
    private const PING_CODE = 1;
    private const GET_STATS_CODE = 10;
    private const UNKNOWN_CODE = 60_000;

    #[TestDox('A raw ping command returns an empty successful response')]
    public function testRawPingReturnsEmptyResponse(): void
    {
        $client = new_client();

        $response = $client->sendRawWithResponse(self::PING_CODE, '');

        assert_same('', $response);
    }

    #[TestDox('A raw get-stats command returns a non-empty response')]
    public function testRawGetStatsReturnsNonEmptyResponse(): void
    {
        $client = new_client();

        $response = $client->sendRawWithResponse(self::GET_STATS_CODE, '');

        assert_true($response !== '', 'expected a non-empty stats response');
    }

    #[TestDox('A session-control code is rejected before reaching the server')]
    #[DataProvider('sessionControlCodes')]
    public function testRawSessionControlCodeIsRejected(int $code): void
    {
        $client = new_client();

        $throwable = assert_throws(static fn () => $client->sendRawWithResponse($code, ''));

        assert_instance_of(IggyException::class, $throwable);
    }

    #[TestDox('An unknown command code is rejected by the server')]
    public function testRawUnknownCodeIsRejectedByServer(): void
    {
        $client = new_client();

        $throwable = assert_throws(static fn () => $client->sendRawWithResponse(self::UNKNOWN_CODE, ''));

        assert_instance_of(IggyException::class, $throwable);
    }

    public static function sessionControlCodes(): array
    {
        return [[38], [39], [40], [44], [45]];
    }
}
