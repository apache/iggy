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

require __DIR__ . '/bootstrap.php';
require __DIR__ . '/IggySdkTest.php';
require __DIR__ . '/TlsTest.php';

if (!extension_loaded('iggy-php')) {
    fwrite(STDERR, "The iggy-php extension is not loaded.\n");
    fwrite(STDERR, "Use Homebrew PHP or pass -d extension=/path/to/libiggy_php.dylib.\n");
    exit(1);
}

$filter = $argv[1] ?? null;
$classes = [IggySdkTest::class, TlsTest::class];
$passed = 0;
$skipped = 0;
$failed = 0;

wait_for_server(server_host(), server_port());

foreach ($classes as $class) {
    $instance = new $class();
    $reflection = new ReflectionClass($class);

    foreach ($reflection->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
        if (!str_starts_with($method->getName(), 'test')) {
            continue;
        }

        $name = $class . '::' . $method->getName();
        if ($filter !== null && !str_contains($name, $filter)) {
            continue;
        }

        try {
            $method->invoke($instance);
            $passed++;
            fwrite(STDOUT, "PASS {$name}\n");
        } catch (ReflectionException $exception) {
            $failed++;
            fwrite(STDERR, "FAIL {$name}: {$exception->getMessage()}\n");
        } catch (Throwable $throwable) {
            $previous = $throwable instanceof ReflectionException ? $throwable->getPrevious() : null;
            $error = $previous ?? $throwable;

            if ($error instanceof SkippedTest) {
                $skipped++;
                fwrite(STDOUT, "SKIP {$name}: {$error->getMessage()}\n");
                continue;
            }

            $failed++;
            fwrite(STDERR, "FAIL {$name}: {$error->getMessage()}\n");
        }
    }
}

fwrite(STDOUT, "\n{$passed} passed, {$skipped} skipped, {$failed} failed\n");
exit($failed === 0 ? 0 : 1);
