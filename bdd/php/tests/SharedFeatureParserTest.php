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

use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\TestCase;

final class SharedFeatureParserTest extends TestCase
{
    #[Group('stream-options')]
    public function testGivenTaggedScenariosWhenLoadingWithExclusionsShouldKeepSupportedCases(): void
    {
        $cases = SharedFeatureParser::load(
            __DIR__ . '/../../scenarios/stream_options.feature',
            ['@describe-options', '@message-headers'],
        );

        self::assertCount(4, $cases);
        self::assertArrayNotHasKey(
            'Describe stream options',
            $cases,
        );
        self::assertArrayNotHasKey(
            'Send and poll a message with headers on a keyed stream',
            $cases,
        );
    }
}
