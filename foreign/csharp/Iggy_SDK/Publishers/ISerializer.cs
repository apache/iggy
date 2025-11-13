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

namespace Apache.Iggy.Publishers;

/// <summary>
///     Interface for serializing objects of type T to byte arrays.
///     <para>
///         No type constraints are enforced on T to provide maximum flexibility.
///         Implementations are responsible for ensuring that the provided type can be properly serialized.
///     </para>
/// </summary>
/// <typeparam name="T">
///     The source type for serialization. Can be any type - reference or value type, nullable or non-nullable.
///     The serializer implementation must be able to handle the specific type provided.
/// </typeparam>
/// <remarks>
///     Implementations should throw appropriate exceptions (e.g., <see cref="System.NotSupportedException" />,
///     <see cref="System.ArgumentException" />, or <see cref="System.InvalidOperationException" />)
///     if the provided data cannot be serialized. These exceptions will be caught and logged by
///     <see cref="IggyPublisher{T}" /> during message creation.
/// </remarks>
public interface ISerializer<in T>
{
    /// <summary>
    ///     Serializes an instance of type T into a byte array.
    /// </summary>
    /// <param name="data">The object to serialize. May be null depending on the serializer implementation.</param>
    /// <returns>A byte array representing the serialized data.</returns>
    /// <exception cref="System.NotSupportedException">
    ///     Thrown when the serializer does not support the provided type or value.
    /// </exception>
    /// <exception cref="System.ArgumentException">
    ///     Thrown when the data cannot be serialized due to invalid content.
    /// </exception>
    /// <exception cref="System.InvalidOperationException">
    ///     Thrown when the serialization operation fails due to state issues.
    /// </exception>
    byte[] Serialize(T data);
}
