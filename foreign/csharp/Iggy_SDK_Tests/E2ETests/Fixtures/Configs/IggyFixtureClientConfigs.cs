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

using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;

namespace Apache.Iggy.Tests.E2ETests.Fixtures.Configs;

public static class IggyFixtureClientMessagingSettings
{
    public static Action<MessagePollingSettings> PollingSettings { get; set; } = options =>
    {
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.StoreOffsetStrategy = StoreOffset.WhenMessagesAreReceived;
    };
    public static Action<MessageBatchingSettings> BatchingSettings { get; set; } = options =>
    {
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 4096;
    };
    
    public static Action<MessageBatchingSettings> BatchingSettingsSendFixture { get; set; } = options =>
    {
        options.Enabled = false;
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 4096;
    };

    public static Action<MessageBatchingSettings> BatchingSettingsFetchFixture { get; set; } = options =>
    {
        options.Enabled = false;
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 8912;
    };

    public static Action<MessageBatchingSettings> BatchingSettingsPollMessagesFixture { get; set; } = options =>
    {
        options.Enabled = true;
        options.Interval = TimeSpan.FromMilliseconds(99);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 8912;
    };
}