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

namespace Apache.Iggy.Tests.Integrations.Helpers;

public static class Eventually
{
    private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     Polls until the read satisfies the condition. Some server work commits ahead of the state it changes -
    ///     server-ng applies a purge by advancing a generation that its reconciler acts on a tick later - so the
    ///     first read after an acknowledged command can still show the old value.
    /// </summary>
    public static async Task<T> ReadAsync<T>(Func<Task<T>> read, Func<T, bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var value = await read();
            if (condition(value) || DateTime.UtcNow >= deadline)
            {
                return value;
            }

            await Task.Delay(PollInterval);
        }
    }
}
