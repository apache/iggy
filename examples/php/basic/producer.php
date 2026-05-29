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

require_once __DIR__ . '/../src/common.php';

$stream = 'php-basic-stream';
$topic = 'php-basic-topic';
$client = iggy_client();

ensure_stream_and_topic($client, $stream, $topic);
send_payloads($client, $stream, $topic, [
    'basic-message-alpha',
    'basic-message-beta',
    'basic-message-gamma',
]);

echo "Sent 3 PHP basic messages", PHP_EOL;
