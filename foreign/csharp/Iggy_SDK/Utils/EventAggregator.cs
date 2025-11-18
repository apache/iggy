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

using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Utils;


internal class EventAggregator<T>
{
    private readonly List<Func<T, Task>> _subscribers = new();
    private readonly object _lock = new();
    private readonly ILogger<EventAggregator<T>> _logger;

    public EventAggregator(ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger<EventAggregator<T>>();
    }

    public void Subscribe(Func<T, Task> handler)
    {
        lock (_lock)
        {
            _subscribers.Add(handler);
        }
    }

    public void Unsubscribe(Func<T, Task> handler)
    {
        lock (_lock)
        {
            _subscribers.Remove(handler);
        }
    }

    public void Publish(T message)
    {
        _ = PublishInternal(message);
    }

    public void Clear()
    {
        lock (_lock)
        {
            _subscribers.Clear();
        }
    }

    private async Task PublishInternal(T message)
    {
        try
        {
            if (message is null)
            {
                return;
            }

            List<Func<T, Task>> handlers;
            lock (_lock)
            {
                handlers = _subscribers.ToList();
            }

            var tasks = handlers.Select(async handler =>
            {
                try
                {
                    await handler(message);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process event of type {Type}", handler.GetType().Name);
                }
            });
            await Task.WhenAll(tasks);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to publish event of type {Type}", typeof(T).Name);
        }
    }
}
