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

using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Encryption;
using Apache.Iggy.Headers;
namespace Apache.Iggy.Tests.MapperTests;

public class HeaderEncryptionTests
{
    [Fact]
    public void Headers_should_survive_encrypt_decrypt_roundtrip()
    {
        var key = Encryption.AesMessageEncryptor.GenerateKey();
        var encryptor = new Encryption.AesMessageEncryptor(key);

        var originalHeaders = new Dictionary<Headers.HeaderKey, Headers.HeaderValue>
        {
            { new Headers.HeaderKey { Kind = Headers.HeaderKind.String, Value = "batch"u8.ToArray() }, new Headers.HeaderValue { Kind = Headers.HeaderKind.Uint64, Value = BitConverter.GetBytes(1UL) } },
            { new Headers.HeaderKey { Kind = Headers.HeaderKind.String, Value = "type"u8.ToArray() }, new Headers.HeaderValue { Kind = Headers.HeaderKind.String, Value = "test-message"u8.ToArray() } },
            { new Headers.HeaderKey { Kind = Headers.HeaderKind.String, Value = "encrypted"u8.ToArray() }, new Headers.HeaderValue { Kind = Headers.HeaderKind.Bool, Value = [1] } },
        };

        var headerBytes = Contracts.Tcp.TcpContracts.GetHeadersBytes(originalHeaders);
        Assert.NotEmpty(headerBytes);

        var encrypted = encryptor.Encrypt(headerBytes);
        Assert.NotEqual(headerBytes, encrypted);
        Assert.True(encrypted.Length > headerBytes.Length);

        // Encrypted bytes must not parse as valid headers
        var parsed = Mappers.BinaryMapper.TryMapHeaders(encrypted);
        Assert.Null(parsed);

        var decrypted = encryptor.Decrypt(encrypted);
        Assert.Equal(headerBytes, decrypted);

        var roundTripped = Mappers.BinaryMapper.MapHeaders(decrypted);
        Assert.Equal(originalHeaders.Count, roundTripped.Count);

        foreach (var (key2, value) in originalHeaders)
        {
            Assert.True(roundTripped.ContainsKey(key2));
            Assert.Equal(value.Kind, roundTripped[key2].Kind);
            Assert.Equal(value.Value, roundTripped[key2].Value);
        }
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_invalid_first_byte()
    {
        var random = new byte[64];
        Random.Shared.NextBytes(random);
        random[0] = 0;

        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(random));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_first_byte_above_range()
    {
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(new byte[] { 16, 0, 0, 0 }));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_empty_payload()
    {
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders([]));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_truncated_key_length()
    {
        // Valid header kind byte but not enough bytes for key length
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(new byte[] { 2 }));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_zero_key_length()
    {
        // Valid kind, then key length = 0 (invalid)
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(new byte[] { 2, 0, 0, 0, 0 }));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_key_length_exceeding_payload()
    {
        // Valid kind, key length = 100 but only a few bytes remain
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(new byte[] { 2, 100, 0, 0, 0, 1, 2 }));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_truncated_value_kind()
    {
        // Valid kind(2=String), key_len=1, key='A', then no value kind byte
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(new byte[] { 2, 1, 0, 0, 0, 65 }));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_invalid_value_kind()
    {
        // Valid kind(2), key_len=1, key='A', invalid value kind=0
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(new byte[] { 2, 1, 0, 0, 0, 65, 0 }));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_truncated_value_length()
    {
        // Valid kind(2), key_len=1, key='A', valid value kind(2), then not enough for value length
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(new byte[] { 2, 1, 0, 0, 0, 65, 2, 1 }));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_zero_value_length()
    {
        // Valid kind(2), key_len=1, key='A', valid value kind(2), value_len=0 (invalid)
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(new byte[] { 2, 1, 0, 0, 0, 65, 2, 0, 0, 0, 0 }));
    }

    [Fact]
    public void TryMapHeaders_returns_null_on_value_length_exceeding_payload()
    {
        // Valid kind(2), key_len=1, key='A', valid value kind(2), value_len=100 but not enough bytes
        Assert.Null(Mappers.BinaryMapper.TryMapHeaders(new byte[] { 2, 1, 0, 0, 0, 65, 2, 100, 0, 0, 0 }));
    }

    [Fact]
    public void TryMapHeaders_returns_valid_headers_on_plaintext()
    {
        var headers = new Dictionary<Headers.HeaderKey, Headers.HeaderValue>
        {
            { new Headers.HeaderKey { Kind = Headers.HeaderKind.String, Value = "key"u8.ToArray() }, new Headers.HeaderValue { Kind = Headers.HeaderKind.String, Value = "value"u8.ToArray() } },
        };

        var bytes = Contracts.Tcp.TcpContracts.GetHeadersBytes(headers);
        var result = Mappers.BinaryMapper.TryMapHeaders(bytes);

        Assert.NotNull(result);
        Assert.Single(result);
    }

    [Fact]
    public void TryMapHeaders_returns_valid_for_all_header_kinds()
    {
        var headers = new Dictionary<Headers.HeaderKey, Headers.HeaderValue>
        {
            { new Headers.HeaderKey { Kind = Headers.HeaderKind.Raw, Value = "k1"u8.ToArray() }, new Headers.HeaderValue { Kind = Headers.HeaderKind.Raw, Value = [0xFF] } },
            { new Headers.HeaderKey { Kind = Headers.HeaderKind.String, Value = "k2"u8.ToArray() }, new Headers.HeaderValue { Kind = Headers.HeaderKind.Bool, Value = [1] } },
            { new Headers.HeaderKey { Kind = Headers.HeaderKind.String, Value = "k3"u8.ToArray() }, new Headers.HeaderValue { Kind = Headers.HeaderKind.Int32, Value = BitConverter.GetBytes(42) } },
            { new Headers.HeaderKey { Kind = Headers.HeaderKind.String, Value = "k4"u8.ToArray() }, new Headers.HeaderValue { Kind = Headers.HeaderKind.Uint64, Value = BitConverter.GetBytes(99UL) } },
            { new Headers.HeaderKey { Kind = Headers.HeaderKind.String, Value = "k5"u8.ToArray() }, new Headers.HeaderValue { Kind = Headers.HeaderKind.Double, Value = BitConverter.GetBytes(3.14) } },
        };

        var bytes = Contracts.Tcp.TcpContracts.GetHeadersBytes(headers);
        var result = Mappers.BinaryMapper.TryMapHeaders(bytes);

        Assert.NotNull(result);
        Assert.Equal(5, result.Count);
    }
}
