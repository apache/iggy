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

/**
 * Endpoint and credentials the BDD suite runs against. The SDK suite keeps its own
 * defaults through bootstrap.php; here a dropped compose variable must abort the run
 * rather than connect to whatever happens to listen on the fallback address.
 */
final class TestEnvironment
{
    public static function serverAddress(): string
    {
        return self::env('IGGY_TCP_ADDRESS');
    }

    public static function rootUsername(): string
    {
        return self::env('IGGY_ROOT_USERNAME');
    }

    public static function rootPassword(): string
    {
        return self::env('IGGY_ROOT_PASSWORD');
    }

    private static function env(string $name): string
    {
        $value = getenv($name);

        if ($value === false || $value === '') {
            throw new RuntimeException("{$name} must be set; run the suite via scripts/run-bdd-tests.sh");
        }

        return $value;
    }
}
