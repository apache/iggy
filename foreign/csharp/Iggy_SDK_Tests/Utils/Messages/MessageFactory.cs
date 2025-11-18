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

using System.Text;

namespace Apache.Iggy.Tests.Utils.Messages;

internal static class MessageFactory
{
    internal static (ulong offset, ulong timestamp, Guid guid, int headersLength, uint checkSum, byte[] payload)
        CreateMessageResponseFields()
    {
        var offset = (ulong)Random.Shared.Next(6, 69);
        var timestamp = (ulong)Random.Shared.Next(420, 69420);
        var guid = Guid.NewGuid();
        var checkSum = (uint)Random.Shared.Next(42069, 69042);
        var bytes = Encoding.UTF8.GetBytes(Utility.RandomString(Random.Shared.Next(6, 69)));
        var headersLength = Random.Shared.Next(1, 69);
        return (offset, timestamp, guid, headersLength, checkSum, bytes);
    }
}

internal class DummyObject
{
    public required int Id { get; set; }
    public required string Text { get; set; }
}
