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
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Encryption;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Applies an <see cref="IMessageEncryptor" /> to a message in place, replacing the payload (and any user
///     headers) with ciphertext. The ciphertext is a freshly allocated buffer the message owns, so any pooled
///     payload memory can be released once encryption has run. Idempotent: a message already marked
///     <see cref="Message.Encrypted" /> is left untouched, so a re-sent failure snapshot is never double-encrypted.
/// </summary>
internal static class PublisherEncryption
{
    public static void Encrypt(Message message, IMessageEncryptor encryptor)
    {
        if (message.Encrypted)
        {
            return;
        }

        // Stage into locals and mutate the message only after every encryptor call succeeded: a throw halfway
        // must not leave a ciphertext payload with Encrypted still false, or a retried send would double-encrypt.
        ReadOnlyMemory<byte> payload = encryptor.Encrypt(message.Payload.Span);

        ReadOnlyMemory<byte> rawUserHeaders = default;
        var encryptedHeaders = message.UserHeaders is { Count: > 0 };
        if (encryptedHeaders)
        {
            var length = TcpContracts.HeadersByteLength(message.UserHeaders!);
            var scratch = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                TcpContracts.WriteHeadersTo(scratch.AsSpan(0, length), message.UserHeaders!);
                rawUserHeaders = encryptor.Encrypt(scratch.AsSpan(0, length));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(scratch, true);
            }
        }

        message.Payload = payload;
        message.Header = message.Header with { PayloadLength = payload.Length };

        if (encryptedHeaders)
        {
            message.RawUserHeaders = rawUserHeaders;
            message.Header = message.Header with { UserHeadersLength = rawUserHeaders.Length };
            message.UserHeaders = null;
        }

        message.Encrypted = true;
    }

    public static void EncryptAll(IList<Message> messages, IMessageEncryptor encryptor)
    {
        foreach (var message in messages)
        {
            Encrypt(message, encryptor);
        }
    }
}
