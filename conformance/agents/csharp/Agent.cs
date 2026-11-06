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

using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Iggy.Configuration;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;

namespace Apache.Iggy.ConformanceAgent;

/// <summary>The entities and the tasks of one agent.</summary>
internal sealed class Agent
{
    private readonly Dictionary<string, Entity> _entities = [];
    private readonly Lock _gate = new();
    private readonly Dictionary<string, RunningTask> _tasks = [];

    internal async Task<Reply> HandleAsync(Request request)
    {
        try
        {
            return request.Op switch
            {
                "create" when request.Kind == "client" => await CreateClientAsync(Name(request.Entity), request.Args),
                "create" => new Reply.Unsupported($"creating a {request.Kind} with create"),
                "call" => await CallAsync(Name(request.Entity), Name(request.Method), request.Args, request.SaveAs,
                    CancellationToken.None),
                "start" => Start(Name(request.Task), Name(request.Entity), Name(request.Method), request.Args),
                "await" => await AwaitAsync(Name(request.Task)),
                "cancel" => Cancel(Name(request.Task)),
                "close" => await CloseAsync(Name(request.Entity)),
                "reset" => await ResetAsync(),
                _ => new Reply.Failure($"unknown op {request.Op}")
            };
        }
        catch (Exception error)
        {
            return new Reply.Failure($"{request.Op}: {error.Message}");
        }
    }

    /// <summary>Cancels every task and closes every entity.</summary>
    internal async Task<Reply> ResetAsync()
    {
        List<RunningTask> tasks;
        List<string> entities;
        lock (_gate)
        {
            tasks = [.. _tasks.Values];
            _tasks.Clear();
            // Readers and consumers read through their clients, so they close first.
            entities = [.. _entities.OrderBy(entity => entity.Value is ClientEntity).Select(entity => entity.Key)];
        }

        foreach (var task in tasks)
        {
            task.Cancellation.Cancel();
        }

        foreach (var entity in entities)
        {
            await CloseAsync(entity);
        }

        return Reply.Null;
    }

    internal void Keep(string name, Entity entity)
    {
        lock (_gate)
        {
            _entities[name] = entity;
        }
    }

    private async Task<Reply> CreateClientAsync(string name, JsonElement rawArgs)
    {
        CreateClientArgs args;
        try
        {
            args = Arguments.Read<CreateClientArgs>(rawArgs);
        }
        catch (Exception error)
        {
            return new Reply.Failure($"create arguments: {error.Message}");
        }

        if (args.Transport != "tcp")
        {
            return new Reply.Unsupported($"the {args.Transport} transport");
        }

        var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = args.Address,
            Protocol = Protocol.Tcp
        });
        try
        {
            await client.ConnectAsync();
            if (args.Login is { } login)
            {
                await client.LoginUserAsync(login.Username, login.Password);
            }
        }
        catch (Exception error)
        {
            client.Dispose();
            return Reply.FromException(error);
        }

        Keep(name, new ClientEntity(client));
        return Reply.Null;
    }

    private async Task<Reply> CallAsync(string name, string method, JsonElement args, string? saveAs,
        CancellationToken token)
    {
        Entity? entity;
        lock (_gate)
        {
            entity = _entities.GetValueOrDefault(name);
        }

        return entity switch
        {
            ClientEntity client => await Calls.CallAsync(this, client.Client, method, args, saveAs, token),
            ReaderEntity reader when method == "next" => await NextAsync(args, async () =>
                Canonical.Node(PolledMessagesResult.From(await reader.NextAsync(token)))),
            ConsumerEntity consumer when method == "next" => await NextAsync(args, async () =>
                await consumer.NextAsync(token) is { } received
                    ? Canonical.Node(ReceivedMessageResult.From(received))
                    : throw new InvalidOperationException("The consumer stream ended.")),
            null => new Reply.Failure($"no entity {name}"),
            _ => new Reply.Failure($"{method} is not a method of {name}")
        };
    }

    private static async Task<Reply> NextAsync(JsonElement args, Func<Task<JsonNode?>> next)
    {
        try
        {
            Arguments.Read<NoArgs>(args);
        }
        catch (Exception error)
        {
            return new Reply.Failure($"arguments: {error.Message}");
        }

        try
        {
            return new Reply.Result(await next());
        }
        catch (Exception error)
        {
            return Reply.FromException(error);
        }
    }

    private Reply Start(string task, string entity, string method, JsonElement args)
    {
        var cancellation = new CancellationTokenSource();
        var answer = Task.Run(() => CallAsync(entity, method, args, null, cancellation.Token));
        RunningTask? previous;
        lock (_gate)
        {
            _tasks.Remove(task, out previous);
            _tasks[task] = new RunningTask(cancellation, answer);
        }

        previous?.Cancellation.Cancel();
        return Reply.Null;
    }

    private async Task<Reply> AwaitAsync(string task)
    {
        RunningTask? running;
        lock (_gate)
        {
            _tasks.Remove(task, out running);
        }

        return running is null ? new Reply.Failure($"no task {task}") : await running.Answer;
    }

    private Reply Cancel(string task)
    {
        RunningTask? running;
        lock (_gate)
        {
            _tasks.Remove(task, out running);
        }

        if (running is null)
        {
            return new Reply.Failure($"no task {task}");
        }

        running.Cancellation.Cancel();
        return Reply.Null;
    }

    private async Task<Reply> CloseAsync(string name)
    {
        Entity? entity;
        lock (_gate)
        {
            _entities.Remove(name, out entity);
        }

        if (entity is not null)
        {
            try
            {
                await entity.CloseAsync();
            }
            catch (Exception error)
            {
                Log.Warn($"Cannot close {name}: {error.Message}");
            }
        }

        return Reply.Null;
    }

    private static string Name(string? name)
    {
        return name ?? throw new InvalidOperationException("The request lacks a name.");
    }

    private sealed record RunningTask(CancellationTokenSource Cancellation, Task<Reply> Answer);
}

internal abstract class Entity
{
    internal abstract ValueTask CloseAsync();
}

internal sealed class ClientEntity(IIggyClient client) : Entity
{
    internal IIggyClient Client => client;

    internal override ValueTask CloseAsync()
    {
        client.Dispose();
        return ValueTask.CompletedTask;
    }
}

internal sealed class ReaderEntity(PartitionReader reader) : Entity
{
    // The reader takes one caller of NextAsync at a time.
    private readonly SemaphoreSlim _turn = new(1, 1);

    internal async Task<PolledMessages> NextAsync(CancellationToken token)
    {
        await _turn.WaitAsync(token);
        try
        {
            return await reader.NextAsync(token);
        }
        finally
        {
            _turn.Release();
        }
    }

    internal override ValueTask CloseAsync()
    {
        return reader.CloseAsync();
    }
}

internal sealed class ConsumerEntity(IggyConsumer consumer) : Entity
{
    private readonly IAsyncEnumerator<ReceivedMessage> _messages = consumer.ReceiveAsync().GetAsyncEnumerator();
    private readonly SemaphoreSlim _turn = new(1, 1);
    private Task<bool>? _pending;

    /// <summary>The next message, or null when the consumer stream ended.</summary>
    internal async Task<ReceivedMessage?> NextAsync(CancellationToken token)
    {
        await _turn.WaitAsync(token);
        try
        {
            // A canceled call leaves its wait to the next call, so a canceled call loses no message.
            _pending ??= _messages.MoveNextAsync().AsTask();
            var moved = await _pending.WaitAsync(token);
            _pending = null;
            return moved ? _messages.Current : null;
        }
        finally
        {
            _turn.Release();
        }
    }

    internal override async ValueTask CloseAsync()
    {
        try
        {
            await consumer.ShutdownAsync();
        }
        finally
        {
            // The shut down consumer ends the wait of a canceled call, which must end before the stream is disposed.
            await _turn.WaitAsync();
            if (_pending is { } pending)
            {
                await ((Task)pending).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }

            await _messages.DisposeAsync();
            _turn.Release();
        }
    }
}
