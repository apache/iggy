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

using Apache.Iggy.Encryption;
using Apache.Iggy.Headers;
using Apache.Iggy.Messages;
using Apache.Iggy.Publishers;

namespace Apache.Iggy.Tests.PublisherTests;

public class PublisherEncryptionTests
{
    private static AesMessageEncryptor CreateEncryptor()
    {
        return new AesMessageEncryptor(AesMessageEncryptor.GenerateKey());
    }

    [Fact]
    public void Encrypt_MarksMessageEncrypted_AndSyncsHeaderPayloadLength()
    {
        var encryptor = CreateEncryptor();
        var message = new Message(Guid.NewGuid(), "payload"u8.ToArray());

        PublisherEncryption.Encrypt(message, encryptor);

        Assert.True(message.Encrypted);
        Assert.Equal(message.Payload.Length, message.Header.PayloadLength);
        Assert.Equal("payload"u8.ToArray(), encryptor.Decrypt(message.Payload.Span));
    }

    [Fact]
    public void Encrypt_SkipsAlreadyEncryptedMessage()
    {
        var encryptor = CreateEncryptor();
        var message = new Message(Guid.NewGuid(), "payload"u8.ToArray());

        PublisherEncryption.Encrypt(message, encryptor);
        var ciphertext = message.Payload;

        PublisherEncryption.Encrypt(message, encryptor);

        Assert.True(ciphertext.Span.SequenceEqual(message.Payload.Span));
        Assert.Equal("payload"u8.ToArray(), encryptor.Decrypt(message.Payload.Span));
    }

    [Fact]
    public void Encrypt_MovesUserHeadersToEncryptedRawForm()
    {
        var encryptor = CreateEncryptor();
        var headers = new Dictionary<HeaderKey, HeaderValue>
        {
            { new HeaderKey { Kind = HeaderKind.String, Value = "key"u8.ToArray() }, new HeaderValue { Kind = HeaderKind.String, Value = "value"u8.ToArray() } }
        };
        var message = new Message(Guid.NewGuid(), "payload"u8.ToArray(), headers);

        PublisherEncryption.Encrypt(message, encryptor);

        Assert.Null(message.UserHeaders);
        Assert.False(message.RawUserHeaders.IsEmpty);
        Assert.True(message.Encrypted);
    }
}
