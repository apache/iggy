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

namespace Apache.Iggy.Tests.Utils.Groups;

internal static class ConsumerGroupFactory
{
    internal static (uint id, uint membersCount, uint partitionsCount, string name) CreateConsumerGroupResponseFields()
    {
        var id1 = (uint)Random.Shared.Next(1, 10);
        var membersCount1 = (uint)Random.Shared.Next(1, 10);
        var partitionsCount1 = (uint)Random.Shared.Next(1, 10);
        var name = Utility.RandomString(69);
        return (id1, membersCount1, partitionsCount1, name);
    }
}