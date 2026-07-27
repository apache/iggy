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
    private const NON_REPLICATED = 'non_replicated';
    private const REPLICATED = 'replicated';

    /** No server registers a handler for this code. */
    private const VENDOR_CODE = 60_001;

    #[TestDox('A raw ping command returns an empty successful response')]
    public function testRawPingReturnsEmptyResponse(): void
    {
        $client = new_client();

        $response = $client->sendBinaryRequest(self::NON_REPLICATED, self::PING_CODE, '');

        assert_same('', $response);
    }

    #[TestDox('A raw get-stats command returns a non-empty response')]
    public function testRawGetStatsReturnsNonEmptyResponse(): void
    {
        $client = new_client();

        $response = $client->sendBinaryRequest(self::NON_REPLICATED, self::GET_STATS_CODE, '');

        assert_true($response !== '', 'expected a non-empty stats response');
    }

    #[TestDox('A session-control code is rejected before reaching the server')]
    #[DataProvider('sessionControlCodes')]
    public function testRawSessionControlCodeIsRejected(string $kind, int $code): void
    {
        $client = new_client();

        $throwable = assert_throws(static fn () => $client->sendBinaryRequest($kind, $code, ''));

        assert_instance_of(IggyException::class, $throwable);
    }

    #[TestDox('A vendor command code is rejected and the connection stays usable')]
    public function testRawVendorCodeIsRejectedByServer(): void
    {
        $client = new_client();

        $throwable = assert_throws(
            static fn () => $client->sendBinaryRequest(self::NON_REPLICATED, self::VENDOR_CODE, '')
        );

        assert_instance_of(IggyException::class, $throwable);
        assert_same('', $client->sendBinaryRequest(self::NON_REPLICATED, self::PING_CODE, ''));
    }

    #[TestDox('A replicated vendor command code has no handler')]
    public function testRawReplicatedVendorCodeIsRejected(): void
    {
        $client = new_client();

        $throwable = assert_throws(
            static fn () => $client->sendBinaryRequest(self::REPLICATED, self::VENDOR_CODE, '')
        );

        assert_instance_of(IggyException::class, $throwable);
    }

    #[TestDox('A replicated declaration on a standard command is inert on classic framing')]
    public function testRawReplicatedDeclarationIsIgnoredOnClassicFraming(): void
    {
        $client = new_client();

        $response = $client->sendBinaryRequest(self::REPLICATED, self::GET_STATS_CODE, '');

        assert_true($response !== '', 'expected a non-empty stats response');
    }

    #[TestDox('An unknown request kind is rejected')]
    public function testRawUnknownKindIsRejected(): void
    {
        $client = new_client();

        $throwable = assert_throws(static fn () => $client->sendBinaryRequest('auto', self::PING_CODE, ''));

        assert_instance_of(IggyException::class, $throwable);
        // Distinct from the server's "invalid command", so a mistyped kind is
        // never mistaken for a rejected code.
        assert_true(
            str_contains($throwable->getMessage(), 'invalid binary request kind'),
            'expected the kind-specific rejection message'
        );
    }

    public static function sessionControlCodes(): array
    {
        $cases = [];
        foreach ([self::NON_REPLICATED, self::REPLICATED] as $kind) {
            foreach ([38, 39, 40, 44, 45] as $code) {
                $cases[] = [$kind, $code];
            }
        }

        return $cases;
    }
}
