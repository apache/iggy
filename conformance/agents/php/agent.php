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

// The conformance agent of the PHP SDK. It reads JSON requests from stdin, calls the SDK and
// writes one JSON answer per request to stdout. It never judges a result. See
// conformance/README.md for the protocol.

namespace Iggy\Conformance;

require __DIR__ . '/src/Protocol.php';
require __DIR__ . '/src/Arguments.php';
require __DIR__ . '/src/Canonical.php';
require __DIR__ . '/src/Metadata.php';
require __DIR__ . '/src/Calls.php';
require __DIR__ . '/src/Agent.php';

// A warning is a fault of the agent, so it fails the request instead of passing unnoticed.
set_error_handler(static function (int $severity, string $message, string $file, int $line): never {
    throw new \ErrorException($message, 0, $severity, $file, $line);
});

(new Agent())->serve();
