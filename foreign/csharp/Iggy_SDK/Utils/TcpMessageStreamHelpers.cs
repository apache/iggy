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

using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Iggy.Enums;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Utils;

internal static class TcpMessageStreamHelpers
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void CreatePayload(Span<byte> result, Span<byte> message, int command)
    {
        var messageLength = message.Length + 4;
        BinaryPrimitives.WriteInt32LittleEndian(result[..4], messageLength);
        BinaryPrimitives.WriteInt32LittleEndian(result[4..8], command);
        message.CopyTo(result[8..]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static (int Status, int Length) GetResponseLengthAndStatus(Span<byte> buffer)
    {
        var status = BinaryPrimitives.ReadInt32LittleEndian(buffer[..4]);
        var length = BinaryPrimitives.ReadInt32LittleEndian(buffer[4..]);

        return (status, length);
    }

    internal static int CalculateMessageBytesCount(IList<Message> messages)
    {
        return messages switch
        {
            Message[] messagesArray => CalculateMessageBytesCountArray(messagesArray),
            List<Message> messagesList => CalculateMessageBytesCountList(messagesList),
            _ => messages.Sum(msg => 16 + 56 + msg.Payload.Length + 4 +
                                     (msg.UserHeaders?.Sum(header =>
                                         4 + header.Key.Value.Length + 1 + 4 + header.Value.Value.Length) ?? 0))
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static byte[] GetBytesFromIdentifier(Identifier identifier)
    {
        Span<byte> bytes = stackalloc byte[2 + identifier.Length];
        bytes[0] = identifier.Kind switch
        {
            IdKind.Numeric => 1,
            IdKind.String => 2,
            _ => throw new ArgumentOutOfRangeException()
        };
        bytes[1] = (byte)identifier.Length;
        for (var i = 0; i < identifier.Length; i++)
        {
            bytes[i + 2] = identifier.Value[i];
        }

        return bytes.ToArray();
    }

    private static int CalculateMessageBytesCountArray(Message[] messages)
    {
        ref var start = ref MemoryMarshal.GetArrayDataReference(messages);
        ref var end = ref Unsafe.Add(ref start, messages.Length);
        var msgBytesSum = 0;
        while (Unsafe.IsAddressLessThan(ref start, ref end))
        {
            if (start.UserHeaders is not null)
            {
                msgBytesSum += start.Payload.Length + 16 + 56;
                foreach (var (headerKey, headerValue) in start.UserHeaders)
                {
                    msgBytesSum += 4 + headerKey.Value.Length + 1 + 4 + headerValue.Value.Length;
                }
            }
            else
            {
                msgBytesSum += start.Payload.Length + 16 + 56;
            }

            start = ref Unsafe.Add(ref start, 1);
        }

        return msgBytesSum;
    }

    private static int CalculateMessageBytesCountList(List<Message> messages)
    {
        Span<Message> messagesSpan = CollectionsMarshal.AsSpan(messages);
        ref var start = ref MemoryMarshal.GetReference(messagesSpan);
        ref var end = ref Unsafe.Add(ref start, messagesSpan.Length);
        var msgBytesSum = 0;
        while (Unsafe.IsAddressLessThan(ref start, ref end))
        {
            if (start.UserHeaders is not null)
            {
                msgBytesSum += start.Payload.Length + 16 + 56;
                foreach (var (headerKey, headerValue) in start.UserHeaders)
                {
                    msgBytesSum += 4 + headerKey.Value.Length + 1 + 4 + headerValue.Value.Length;
                }
            }
            else
            {
                msgBytesSum += start.Payload.Length + 16 + 56;
            }

            start = ref Unsafe.Add(ref start, 1);
        }

        return msgBytesSum;
    }
}