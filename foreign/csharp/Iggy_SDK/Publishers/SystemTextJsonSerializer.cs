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
using System.Collections.Concurrent;
using System.Text.Json;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Default <see cref="ISerializer{T}" /> backed by <c>System.Text.Json</c>, writing JSON directly into the
///     supplied buffer via <see cref="Utf8JsonWriter" />.
/// </summary>
/// <typeparam name="T">The type to serialize.</typeparam>
public sealed class SystemTextJsonSerializer<T> : ISerializer<T>
{
    private readonly ConcurrentQueue<Utf8JsonWriter> _writerPool = new();

    private readonly JsonSerializerOptions? _options;
    private readonly JsonWriterOptions _writerOptions;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SystemTextJsonSerializer{T}" /> class.
    /// </summary>
    /// <param name="options">Optional serializer options; the System.Text.Json defaults are used when null.</param>
    public SystemTextJsonSerializer(JsonSerializerOptions? options = null)
    {
        _options = options;
        _writerOptions = options is null
            ? default
            : new JsonWriterOptions
            {
                Indented = options.WriteIndented,
                Encoder = options.Encoder,
                MaxDepth = options.MaxDepth,
#if NET9_0_OR_GREATER
                IndentCharacter = options.IndentCharacter,
                IndentSize = options.IndentSize,
                NewLine = options.NewLine
#endif
            };
    }

    /// <inheritdoc />
    public void Serialize(T data, IBufferWriter<byte> writer)
    {
        if (!_writerPool.TryDequeue(out var json))
        {
            json = new Utf8JsonWriter(writer, _writerOptions);
        }
        else
        {
            json.Reset(writer);
        }

        try
        {
            JsonSerializer.Serialize(json, data, _options);
        }
        finally
        {
            _writerPool.Enqueue(json);
        }
    }
}
