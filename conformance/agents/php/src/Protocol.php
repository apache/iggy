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

namespace Iggy\Conformance;

use Iggy\Exception\IggyException;
use Iggy\Exception\PendingOperationException;
use Iggy\Exception\ReadException;

/** The SDK lacks a method, an argument value or a transport. */
final class Unsupported extends \RuntimeException
{
}

/** A fault of the agent itself, such as an argument that it cannot read. */
final class Failure extends \RuntimeException
{
}

/** Exactly one of a result, an SDK error, an unsupported call and a failure of the agent. */
final class Reply
{
    private function __construct(private readonly string $field, private readonly mixed $value)
    {
    }

    public static function result(mixed $value): self
    {
        return new self('result', $value);
    }

    public static function unsupported(string $reason): self
    {
        return new self('unsupported', $reason);
    }

    public static function failure(string $reason): self
    {
        return new self('failure', $reason);
    }

    /** The answer for whatever a call threw. */
    public static function thrown(\Throwable $thrown): self
    {
        return match (true) {
            $thrown instanceof Unsupported => self::unsupported($thrown->getMessage()),
            $thrown instanceof Failure => self::failure($thrown->getMessage()),
            $thrown instanceof IggyException && $thrown->getCode() > 0 => self::sdkError($thrown),
            $thrown instanceof IggyException => self::failure("an error without an Iggy code: {$thrown->getMessage()}"),
            default => self::failure($thrown::class . ": {$thrown->getMessage()}"),
        };
    }

    /** The JSON line that answers the request `id`. */
    public function line(int $id): string
    {
        return json(['id' => $id, $this->field => $this->value]);
    }

    private static function sdkError(IggyException $error): self
    {
        $answer = ['code' => $error->getCode()];
        // A failed read wraps the error of the call, which carries the operation.
        $pending = $error instanceof PendingOperationException ? $error : $error->getPrevious();
        if ($pending instanceof PendingOperationException) {
            $answer['operation_id'] = new Number($pending->operation_id);
            if ($pending->partition_id !== null) {
                $answer['partition_id'] = $pending->partition_id;
            }
        }
        if ($error instanceof ReadException && !isset($answer['partition_id']) && $error->partition_id !== null) {
            $answer['partition_id'] = $error->partition_id;
        }
        $answer['message'] = $error->getMessage();

        return new self('error', $answer);
    }
}

/**
 * An unsigned 64-bit number that the SDK gives as decimal text, because a PHP int stops at
 * PHP_INT_MAX. It is written as a JSON number.
 */
final class Number
{
    public function __construct(public readonly string $digits)
    {
    }
}

/** An unsigned 64-bit value of the SDK, an int or decimal text, as a JSON number. */
function u64(int|string $value): int|Number
{
    return is_int($value) ? $value : new Number($value);
}

/** The JSON text of a value. A list is an array, any other array and a stdClass an object. */
function json(mixed $value): string
{
    return match (true) {
        $value instanceof Number => $value->digits,
        $value instanceof \stdClass => json_object(get_object_vars($value)),
        is_array($value) && array_is_list($value) => '[' . implode(',', array_map(json(...), $value)) . ']',
        is_array($value) => json_object($value),
        default => json_encode($value, JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
    };
}

function json_object(array $fields): string
{
    $members = [];
    foreach ($fields as $name => $value) {
        $members[] = json((string) $name) . ':' . json($value);
    }

    return '{' . implode(',', $members) . '}';
}
