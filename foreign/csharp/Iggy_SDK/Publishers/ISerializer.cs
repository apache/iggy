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

namespace Apache.Iggy.Publishers;

/// <summary>
///     Serializes an instance of <typeparamref name="T" /> into a buffer supplied by the publisher.
///     <see cref="SystemTextJsonSerializer{T}" /> is the default implementation.
/// </summary>
/// <typeparam name="T">The source type for serialization.</typeparam>
/// <remarks>
///     Implementations must write the complete payload and call <see cref="IBufferWriter{T}.Advance" /> for every
///     byte produced. A serializer that throws fails the direct send it was called from; a background send drops
///     the offending unit and reports it (with the original values) through
///     <see cref="IggyPublisher.SubscribeOnBackgroundError" />, keeping the rest of the batch and the processor
///     loop alive.
/// </remarks>
public interface ISerializer<in T>
{
    /// <summary>
    ///     Serializes <paramref name="data" /> into <paramref name="writer" />.
    /// </summary>
    void Serialize(T data, IBufferWriter<byte> writer);
}
