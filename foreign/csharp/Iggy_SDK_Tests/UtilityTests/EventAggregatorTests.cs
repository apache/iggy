// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using Apache.Iggy.Utils;
using Microsoft.Extensions.Logging.Abstractions;

namespace Apache.Iggy.Tests.UtilityTests;

public class EventAggregatorTests
{
    [Fact]
    public void Subscribe_ShouldExecuteCallback()
    {
        var executed = false;
        var executedEventValue = string.Empty;

        var eventAggregator = new EventAggregator<string>(NullLoggerFactory.Instance);
        eventAggregator.Subscribe(e =>
        {
            executed = true;
            executedEventValue = e;
            return Task.CompletedTask;
        });

        eventAggregator.Publish("test");

        Assert.True(executed);
        Assert.Equal("test", executedEventValue);
    }

    [Fact]
    public void Subscribe_MultipleCallback_ShouldExecuteAll()
    {
        var executed_1 = false;
        var executedEventValue_1 = string.Empty;
        var executed_2 = false;
        var executedEventValue_2 = string.Empty;

        var eventAggregator = new EventAggregator<string>(NullLoggerFactory.Instance);
        eventAggregator.Subscribe(e =>
        {
            executed_1 = true;
            executedEventValue_1 = e;
            return Task.CompletedTask;
        });

        eventAggregator.Subscribe(e =>
        {
            executed_2 = true;
            executedEventValue_2 = e;
            return Task.CompletedTask;
        });

        eventAggregator.Publish("test");

        Assert.True(executed_1);
        Assert.Equal("test", executedEventValue_1);
        Assert.True(executed_2);
        Assert.Equal("test", executedEventValue_2);
    }

    [Fact]
    public void Unsubscribe_ShouldNotExecuteCallback()
    {
        var executed_1 = false;
        var executedEventValue_1 = string.Empty;
        var executed_2 = false;
        var executedEventValue_2 = string.Empty;

        var eventAggregator = new EventAggregator<string>(NullLoggerFactory.Instance);
        Func<string, Task> handler_1 = e =>
        {
            executed_1 = true;
            executedEventValue_1 = e;
            return Task.CompletedTask;
        };

        eventAggregator.Subscribe(handler_1);
        eventAggregator.Subscribe(e =>
        {
            executed_2 = true;
            executedEventValue_2 = e;
            return Task.CompletedTask;
        });

        eventAggregator.Unsubscribe(handler_1);

        eventAggregator.Publish("test");

        Assert.False(executed_1);
        Assert.Equal(string.Empty, executedEventValue_1);
        Assert.True(executed_2);
        Assert.Equal("test", executedEventValue_2);
    }

    [Fact]
    public void Unsubscribe_NotSubscribed_ShouldNotThrow()
    {
        var executed = false;
        var executedEventValue = string.Empty;

        var eventAggregator = new EventAggregator<string>(NullLoggerFactory.Instance);
        eventAggregator.Unsubscribe(e =>
        {
            executed = true;
            executedEventValue = e;
            return Task.CompletedTask;
        });

        Assert.False(executed);
        Assert.Equal(string.Empty, executedEventValue);
    }

    [Fact]
    public void Callback_WithException_ShouldNotThrow()
    {
        var executed = false;
        var executedEventValue = string.Empty;

        var eventAggregator = new EventAggregator<string>(NullLoggerFactory.Instance);
        eventAggregator.Subscribe(e =>
        {
            executed = true;
            executedEventValue = e;
            return Task.CompletedTask;
        });

        eventAggregator.Subscribe(e => throw new Exception("Test exception"));

        eventAggregator.Publish("test");

        Assert.True(executed);
        Assert.Equal("test", executedEventValue);
    }

    [Fact]
    public async Task Publish_Should_Run_In_Background()
    {
        var executed = false;
        var executedEventValue = string.Empty;

        var eventAggregator = new EventAggregator<string>(NullLoggerFactory.Instance);
        eventAggregator.Subscribe(async e =>
        {
            await Task.Delay(400);
            executed = true;
            executedEventValue = e;
        });

        eventAggregator.Publish("test");

        await Task.Delay(200);

        Assert.False(executed);
        Assert.Equal(string.Empty, executedEventValue);

        await Task.Delay(400);
        Assert.True(executed);
        Assert.Equal("test", executedEventValue);
    }
}
