using Iggy_SDK.Enums;
using Iggy_SDK.Messages;
using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Iggy_SDK.Utils;

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
        var status = GetResponseStatus(buffer);
        var length = BinaryPrimitives.ReadInt32LittleEndian(buffer[4..]);

        return (status, length);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int GetResponseStatus(Span<byte> buffer) =>
        BinaryPrimitives.ReadInt32LittleEndian(buffer[..4]);

    internal static int CalculateMessageBytesCount(IList<Message> messages)
    {
        return messages switch
        {
            Message[] messagesArray => CalculateMessageBytesCountArray(messagesArray),
            List<Message> messagesList => CalculateMessageBytesCountList(messagesList),
            _ => messages.Sum(msg => 16 + 4 + msg.Payload.Length + 4 +
                                     (msg.Headers?.Sum(header =>
                                         4 + header.Key.Value.Length + 1 + 4 + header.Value.Value.Length) ?? 0)
            )
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
        for (int i = 0; i < identifier.Length; i++)
        {
            bytes[i + 2] = identifier.Value[i];
        }

        return bytes.ToArray();
    }
    
    private static int CalculateMessageBytesCountArray(Message[] messages)
    {
        ref var start = ref MemoryMarshal.GetArrayDataReference(messages);
        ref var end = ref Unsafe.Add(ref start, messages.Length);
        int msgBytesSum = 0;
        while (Unsafe.IsAddressLessThan(ref start, ref end))
        {
            if (start.Headers is not null)
            {
                msgBytesSum += start.Payload.Length + 16 + 4 + 4;
                foreach (var (headerKey, headerValue) in start.Headers)
                {
                    msgBytesSum += 4 + headerKey.Value.Length + 1 + 4 + headerValue.Value.Length;
                }
            }
            else
            {
                msgBytesSum += start.Payload.Length + 16 + 4 + 4;
            }

            start = ref Unsafe.Add(ref start, 1);
        }

        return msgBytesSum;
    }

    private static int CalculateMessageBytesCountList(List<Message> messages)
    {
        var messagesSpan = CollectionsMarshal.AsSpan(messages);
        ref var start = ref MemoryMarshal.GetReference(messagesSpan);
        ref var end = ref Unsafe.Add(ref start, messagesSpan.Length);
        var msgBytesSum = 0;
        while (Unsafe.IsAddressLessThan(ref start, ref end))
        {
            if (start.Headers is not null)
            {
                msgBytesSum += start.Payload.Length + 16 + 4 + 4;
                foreach (var (headerKey, headerValue) in start.Headers)
                {
                    msgBytesSum += 4 + headerKey.Value.Length + 1 + 4 + headerValue.Value.Length;
                }
            }
            else
            {
                msgBytesSum += start.Payload.Length + 16 + 4 + 4;
            }

            start = ref Unsafe.Add(ref start, 1);
        }

        return msgBytesSum;
    }
}