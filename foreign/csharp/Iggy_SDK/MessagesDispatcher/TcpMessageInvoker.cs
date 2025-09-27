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

using System.Buffers;
using System.Text;
using Apache.Iggy.ConnectionStream;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;

namespace Apache.Iggy.MessagesDispatcher;

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
        IList<Message> messages = request.Messages;
        // StreamId, TopicId, Partitioning, message count, metadata field
        var metadataLength = 2 + request.StreamId.Length + 2 + request.TopicId.Length
                             + 2 + request.Partitioning.Length + 4 + 4;
        var messageBufferSize = TcpMessageStreamHelpers.CalculateMessageBytesCount(messages)
                                + metadataLength;
        var payloadBufferSize = messageBufferSize + 4 + BufferSizes.INITIAL_BYTES_LENGTH;

        IMemoryOwner<byte> messageBuffer = MemoryPool<byte>.Shared.Rent(messageBufferSize);
        IMemoryOwner<byte> payloadBuffer = MemoryPool<byte>.Shared.Rent(payloadBufferSize);
        IMemoryOwner<byte> responseBuffer = MemoryPool<byte>.Shared.Rent(BufferSizes.EXPECTED_RESPONSE_SIZE);
        try
        {
            TcpContracts.CreateMessage(messageBuffer.Memory.Span[..messageBufferSize], request.StreamId,
                request.TopicId, request.Partitioning, messages);

            TcpMessageStreamHelpers.CreatePayload(payloadBuffer.Memory.Span[..payloadBufferSize],
                messageBuffer.Memory.Span[..messageBufferSize], CommandCodes.SEND_MESSAGES_CODE);

            await _stream.SendAsync(payloadBuffer.Memory[..payloadBufferSize], token);
            await _stream.FlushAsync(token);
            var readed = await _stream.ReadAsync(responseBuffer.Memory, token);

            if (readed == 0)
            {
                throw new InvalidResponseException("No response received from the server.");
            }

            var response = TcpMessageStreamHelpers.GetResponseLengthAndStatus(responseBuffer.Memory.Span);
            if (response.Status != 0)
            {
                if (response.Length == 0)
                {
                    throw new InvalidResponseException($"Invalid response status code: {response.Status}");
                }

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
