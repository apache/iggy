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

use Iggy\Client;
use Iggy\Consumer;
use Iggy\MessageIterator;
use Iggy\PartitionReader;
use Iggy\PendingPoll;

const PROTOCOL_VERSION = 1;

const CAPABILITIES = [
    'reader', 'consumer', 'resolve_operation', 'abandon_operation', 'raw_command', 'raw_command_replica',
    'describe_options', 'metadata_changes',
];

/** How often the loop looks at the calls that wait, while no request arrives. */
const WAIT_STEP_MICROS = 2_000;

/**
 * A call that ends later. PHP runs one thing at a time, so the loop asks each waiting task for
 * its answer between requests instead of blocking on it.
 */
interface Task
{
    /** The answer once the call ended, or null while it runs. */
    public function poll(): ?Reply;

    public function cancel(): void;
}

/** A poll that runs on the SDK's runtime, which cancels the request when it is dropped. */
final class PollTask implements Task
{
    public function __construct(private readonly PendingPoll $pending)
    {
    }

    public function poll(): ?Reply
    {
        try {
            $polled = $this->pending->wait(0);

            return $polled === null ? null : Reply::result(polled_messages($polled));
        } catch (\Throwable $thrown) {
            return Reply::thrown($thrown);
        }
    }

    public function cancel(): void
    {
        $this->pending->cancel();
    }
}

/** The next batch of a reader. A timeout of 0 takes a batch only if one already arrived. */
final class ReaderNext implements Task
{
    public function __construct(private readonly PartitionReader $reader)
    {
    }

    public function poll(): ?Reply
    {
        try {
            $polled = $this->reader->next(0);

            return $polled === null ? null : Reply::result(polled_messages($polled));
        } catch (\Throwable $thrown) {
            return Reply::thrown($thrown);
        }
    }

    /** A batch that arrives later stays in the reader for the next call. */
    public function cancel(): void
    {
    }
}

/** The next message of a consumer, read with an iterator that never waits. */
final class ConsumerNext implements Task
{
    public function __construct(private readonly MessageIterator $messages)
    {
    }

    public function poll(): ?Reply
    {
        try {
            $this->messages->next();
            $message = $this->messages->current();

            return $message === null ? null : Reply::result(received_message($message));
        } catch (\Throwable $thrown) {
            return Reply::thrown($thrown);
        }
    }

    /** A message that arrives later stays in the consumer for the next call. */
    public function cancel(): void
    {
    }
}

/** A call that ended before it could run in the background. */
final class Finished implements Task
{
    public function __construct(private readonly Reply $answer)
    {
    }

    public function poll(): Reply
    {
        return $this->answer;
    }

    public function cancel(): void
    {
    }
}

/** A consumer with the iterator that its next calls share. */
final class ConsumerEntity
{
    public readonly MessageIterator $messages;

    public function __construct(public readonly Consumer $consumer)
    {
        // An idle timeout of 0 lets each step of the iterator return at once.
        $this->messages = $consumer->iterMessages(0);
    }
}

final class Agent
{
    /** @var array<string, Client> */
    private array $clients = [];
    /** @var array<string, PartitionReader> */
    private array $readers = [];
    /** @var array<string, ConsumerEntity> */
    private array $consumers = [];
    /** @var array<string, Task> the started tasks by name */
    private array $tasks = [];
    /** @var array<int, Task> the tasks whose answer a request waits for, by request id */
    private array $waiting = [];

    /** Serves the requests on stdin until it closes. */
    public function serve(): void
    {
        $this->write(json(['hello' => [
            'sdk' => 'php',
            'sdk_version' => phpversion('iggy-php') ?: 'unknown',
            'protocol' => PROTOCOL_VERSION,
            'transports' => ['tcp'],
            'capabilities' => CAPABILITIES,
        ]]));
        // stream_select() sees only the bytes that PHP has not buffered yet, and a read takes
        // what has arrived without waiting for more.
        stream_set_read_buffer(STDIN, 0);
        stream_set_blocking(STDIN, false);
        $pending = '';
        while (true) {
            $this->answerFinished();
            $readable = [STDIN];
            $none = null;
            [$seconds, $micros] = $this->waiting === [] ? [null, null] : [0, WAIT_STEP_MICROS];
            if (stream_select($readable, $none, $none, $seconds, $micros) === 0) {
                continue;
            }
            $chunk = fread(STDIN, 1 << 16);
            if ($chunk === false || ($chunk === '' && feof(STDIN))) {
                break;
            }
            $pending .= $chunk;
            while (($end = strpos($pending, "\n")) !== false) {
                $line = substr($pending, 0, $end);
                $pending = substr($pending, $end + 1);
                if (trim($line) !== '') {
                    $this->receive($line);
                }
            }
        }
        $this->reset();
    }

    private function receive(string $line): void
    {
        try {
            $request = json_decode($line, true, flags: JSON_THROW_ON_ERROR | JSON_BIGINT_AS_STRING);
        } catch (\JsonException $error) {
            warn("cannot read the request {$line}: {$error->getMessage()}");

            return;
        }
        $id = $request['id'] ?? null;
        if (!is_int($id)) {
            warn("a request without an id: {$line}");

            return;
        }
        try {
            $answer = $this->handle($id, $request);
        } catch (\Throwable $thrown) {
            $answer = Reply::thrown($thrown);
        }
        if ($answer !== null) {
            $this->answer($id, $answer);
        }
    }

    /** The answer of a request, or null when a waiting task answers it later. */
    private function handle(int $id, array $request): ?Reply
    {
        return match ($request['op']) {
            'create' => $this->create($request['kind'], $request['entity'], $request['args'] ?? []),
            'call' => $this->call($id, $request['entity'], $request['method'], $request['args'] ?? [], $request['save_as'] ?? null),
            'start' => $this->start($request['task'], $request['entity'], $request['method'], $request['args'] ?? []),
            'await' => $this->wait($id, $this->takeTask($request['task'])),
            'cancel' => $this->cancel($this->takeTask($request['task'])),
            'close' => $this->close($request['entity']),
            'reset' => $this->reset(),
            default => Reply::failure("unknown op {$request['op']}"),
        };
    }

    private function create(string $kind, string $entity, array $args): Reply
    {
        if ($kind !== 'client') {
            throw new Unsupported("creating a {$kind} with create");
        }
        $args = fields($args, ['transport', 'address', 'login']);
        if ($args['transport'] !== 'tcp') {
            throw new Unsupported("the {$args['transport']} transport");
        }
        $client = new Client($args['address']);
        $client->connect();
        if ($args['login'] !== null) {
            $client->loginUser($args['login']['username'], $args['login']['password']);
        }
        $this->clients[$entity] = $client;

        return Reply::result(null);
    }

    private function call(int $id, string $entity, string $method, array $args, ?string $saveAs): ?Reply
    {
        if ($method === 'next') {
            return $this->wait($id, $this->next($entity, $args));
        }
        $calls = new Calls($this->clients[$entity] ?? throw new Failure("no client {$entity}"));
        if ($method === 'build_reader' || $method === 'build_consumer') {
            if ($saveAs === null) {
                throw new Failure("{$method} needs save_as");
            }
            if ($method === 'build_reader') {
                $this->readers[$saveAs] = $calls->buildReader($args);
            } else {
                $this->consumers[$saveAs] = new ConsumerEntity($calls->buildConsumer($args));
            }

            return Reply::result(null);
        }

        return Reply::result($calls->call($method, $args));
    }

    /**
     * Starts a task. A poll runs on the SDK's runtime and a next call waits in the loop. Every
     * other method of the SDK blocks PHP until it returns, so it cannot run in the background.
     */
    private function start(string $name, string $entity, string $method, array $args): Reply
    {
        $task = match ($method) {
            'next' => $this->next($entity, $args),
            'poll_messages' => $this->startPoll($entity, $args),
            default => throw new Unsupported("the SDK blocks in {$method}, so it cannot run in the background"),
        };
        if (isset($this->tasks[$name])) {
            $this->tasks[$name]->cancel();
        }
        $this->tasks[$name] = $task;

        return Reply::result(null);
    }

    private function startPoll(string $entity, array $args): Task
    {
        $calls = new Calls($this->clients[$entity] ?? throw new Failure("no client {$entity}"));
        try {
            return new PollTask($calls->startPoll($args));
        } catch (Unsupported $unsupported) {
            throw $unsupported;
        } catch (\Throwable $thrown) {
            // A start answers at once, and the await gives the error of the call.
            return new Finished(Reply::thrown($thrown));
        }
    }

    private function next(string $entity, array $args): Task
    {
        fields($args, []);
        if (isset($this->readers[$entity])) {
            return new ReaderNext($this->readers[$entity]);
        }
        if (isset($this->consumers[$entity])) {
            return new ConsumerNext($this->consumers[$entity]->messages);
        }
        throw new Failure("no reader or consumer {$entity}");
    }

    private function takeTask(string $name): Task
    {
        $task = $this->tasks[$name] ?? throw new Failure("no task {$name}");
        unset($this->tasks[$name]);

        return $task;
    }

    /** Leaves the answer of a request to a task, which the loop asks for it. */
    private function wait(int $id, Task $task): null
    {
        $this->waiting[$id] = $task;

        return null;
    }

    private function cancel(Task $task): Reply
    {
        $task->cancel();

        return Reply::result(null);
    }

    private function answerFinished(): void
    {
        foreach ($this->waiting as $id => $task) {
            $answer = $task->poll();
            if ($answer !== null) {
                unset($this->waiting[$id]);
                $this->answer($id, $answer);
            }
        }
    }

    private function close(string $entity): Reply
    {
        $consumer = $this->consumers[$entity] ?? null;
        $reader = $this->readers[$entity] ?? null;
        $client = $this->clients[$entity] ?? null;
        unset($this->consumers[$entity], $this->readers[$entity], $this->clients[$entity]);
        try {
            $consumer?->consumer->shutdown();
            $reader?->close();
            // The SDK has no shutdown of a client. A disconnect closes its connection, and
            // nothing connects it again.
            $client?->disconnect();
        } catch (\Throwable $thrown) {
            warn("cannot close {$entity}: {$thrown->getMessage()}");
        }

        return Reply::result(null);
    }

    /** Cancels every task and closes every entity, the clients last. */
    private function reset(): Reply
    {
        foreach ($this->tasks as $task) {
            $task->cancel();
        }
        foreach ($this->waiting as $id => $task) {
            $task->cancel();
            $this->answer($id, Reply::failure('the agent was reset'));
        }
        $this->tasks = [];
        $this->waiting = [];
        foreach ([...array_keys($this->consumers), ...array_keys($this->readers), ...array_keys($this->clients)] as $entity) {
            $this->close((string) $entity);
        }

        return Reply::result(null);
    }

    private function answer(int $id, Reply $answer): void
    {
        try {
            $line = $answer->line($id);
        } catch (\JsonException $error) {
            $line = Reply::failure("cannot encode the answer: {$error->getMessage()}")->line($id);
        }
        $this->write($line);
    }

    private function write(string $line): void
    {
        fwrite(STDOUT, $line . "\n");
        fflush(STDOUT);
    }
}

function warn(string $message): void
{
    fwrite(STDERR, "iggy-php-agent: {$message}\n");
}
