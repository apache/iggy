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

use Iggy\Client as IggyClient;
use Iggy\Exception\AuthenticationException;
use Iggy\Exception\ConnectionException;
use Iggy\Exception\IggyException;
use Iggy\Exception\NotFoundException;
use Iggy\Exception\TransientException;
use PHPUnit\Framework\Attributes\TestDox;
use PHPUnit\Framework\TestCase;

final class ErrorTest extends TestCase
{
    #[TestDox('Connection configuration errors use the typed connection exception')]
    public function testConnectionConfigurationErrorsUseTypedException(): void
    {
        $throwable = assert_throws(static fn () => new IggyClient('127.0.0.1'));

        assert_instance_of(ConnectionException::class, $throwable);
        assert_instance_of(IggyException::class, $throwable);
    }

    #[TestDox('Authentication failures use the typed authentication exception')]
    public function testAuthenticationErrorsUseTypedException(): void
    {
        $client = new IggyClient(server_host() . ':' . server_port());
        $client->connect();

        $throwable = assert_throws(static fn () => $client->loginUser('iggy', 'wrong-password'));

        assert_instance_of(AuthenticationException::class, $throwable);
        assert_instance_of(IggyException::class, $throwable);
    }

    #[TestDox('Missing resources use the typed not-found exception')]
    public function testNotFoundErrorsUseTypedException(): void
    {
        $client = new_client();

        $throwable = assert_throws(static fn () => $client->deleteStream(unique_name('missing-stream')));

        assert_instance_of(NotFoundException::class, $throwable);
        assert_instance_of(IggyException::class, $throwable);
    }

    #[TestDox('Transient exceptions share the Iggy exception base class')]
    public function testTransientExceptionUsesIggyBase(): void
    {
        assert_true(is_subclass_of(TransientException::class, IggyException::class));
    }
}
