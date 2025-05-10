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

using Iggy_SDK.ConnectionStream;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Contracts.Tcp;
using Iggy_SDK.Exceptions;
using Iggy_SDK.Utils;
using System.Buffers;
using System.Text;

namespace Iggy_SDK.MessagesDispatcher;

internal class TcpMessageInvoker : IMessageInvoker
{
    private readonly IConnectionStream _stream;
    public TcpMessageInvoker(IConnectionStream stream)
    {
        _stream = stream;
    }
    public async Task SendMessagesAsync(MessageSendRequest request,
        CancellationToken token = default)
    {
        var messages = request.Messages;
        var streamTopicIdLength = 2 + request.StreamId.Length + 2 + request.TopicId.Length;
        var messageBufferSize = TcpMessageStreamHelpers.CalculateMessageBytesCount(messages)
                       + request.Partitioning.Length + streamTopicIdLength + 2;
        var payloadBufferSize = messageBufferSize + 4 + BufferSizes.InitialBytesLength;

        var messageBuffer = MemoryPool<byte>.Shared.Rent(messageBufferSize);
        var payloadBuffer = MemoryPool<byte>.Shared.Rent(payloadBufferSize);
        var responseBuffer = MemoryPool<byte>.Shared.Rent(BufferSizes.ExpectedResponseSize);
        try
        {
            TcpContracts.CreateMessage(messageBuffer.Memory.Span[..messageBufferSize], request.StreamId, request.TopicId,
                request.Partitioning,
                messages);
            TcpMessageStreamHelpers.CreatePayload(payloadBuffer.Memory.Span[..payloadBufferSize], 
                messageBuffer.Memory.Span[..messageBufferSize], CommandCodes.SEND_MESSAGES_CODE);

            await _stream.SendAsync(payloadBuffer.Memory[..payloadBufferSize], token);
            await _stream.FlushAsync(token);
            await _stream.ReadAsync(responseBuffer.Memory, token);

            var response = TcpMessageStreamHelpers.GetResponseLengthAndStatus(responseBuffer.Memory.Span);
            if (response.Status != 0)
            {
                var errorBuffer = new byte[response.Length];
                await _stream.ReadAsync(errorBuffer, token);
                throw new InvalidResponseException(Encoding.UTF8.GetString(errorBuffer));
            }
        }
        finally
        {
            messageBuffer.Dispose();
            payloadBuffer.Dispose();
        }
    }
}