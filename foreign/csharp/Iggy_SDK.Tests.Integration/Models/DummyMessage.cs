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

using System.Buffers.Binary;
using System.Text;

namespace Apache.Iggy.Tests.Integrations.Models;

public sealed class DummyMessage
{
    public int Id { get; set; }
    public required string Text { get; set; }

    internal static Func<byte[], DummyMessage> DeserializeDummyMessage
        => bytes =>
        {
            var id = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan()[..4]);
            var textLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan()[4..8]);
            var text = Encoding.UTF8.GetString(bytes.AsSpan()[8..(8 + textLength)]);
            return new DummyMessage
            {
                Id = id,
                Text = text
            };
        };

    internal byte[] SerializeDummyMessage()
    {
        var bytes = new byte[4 + 4 + Text.Length];
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan()[..4], Id);
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan()[4..8], Text.Length);
        Encoding.UTF8.GetBytes(Text).CopyTo(bytes.AsSpan()[8..]);
        return bytes;
    }
}
