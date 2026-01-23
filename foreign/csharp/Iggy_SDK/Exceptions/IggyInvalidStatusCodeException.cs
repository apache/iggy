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

using Apache.Iggy.Enums;

namespace Apache.Iggy.Exceptions;

/// <summary>
///     Exception thrown when the status code returned by the server is not valid.
///     If status code is not in IggyErrorCode enum, ErrorCode will be set to Error.
/// </summary>
public sealed class IggyInvalidStatusCodeException : Exception
{
    /// <summary>
    ///     Status code returned by the server.
    /// </summary>
    public int StatusCode { get; init; }

    /// <summary>
    ///     Error code as <see cref="IggyErrorCode" /> enum.
    /// </summary>
    public IggyErrorCode ErrorCode { get; init; }

    /// <summary>
    ///     Checks if the error indicates that a resource was not found.
    /// </summary>
    public bool IsNotFound => ErrorCode.IsNotFound();

    /// <summary>
    ///     Checks if the error indicates that a resource already exists.
    /// </summary>
    public bool IsAlreadyExists => ErrorCode.IsAlreadyExists();

    /// <summary>
    ///     Checks if the error is related to authentication.
    /// </summary>
    public bool IsAuthenticationError => ErrorCode.IsAuthenticationError();

    /// <summary>
    ///     Checks if the error is related to connection issues.
    /// </summary>
    public bool IsConnectionError => ErrorCode.IsConnectionError();

    /// <summary>
    ///     Initializes a new instance of the <see cref="IggyInvalidStatusCodeException" /> class.
    /// </summary>
    /// <param name="statusCode">Iggy status code</param>
    public IggyInvalidStatusCodeException(int statusCode)
        : this(statusCode, IggyErrorCodeExtensions.FromStatusCode(statusCode))
    {
    }

    internal IggyInvalidStatusCodeException(int statusCode, IggyErrorCode errorCode)
        : base($"Invalid status code: {statusCode} ({errorCode.ToString()})")
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
    }

    internal IggyInvalidStatusCodeException(int statusCode, string message) : base(message)
    {
        StatusCode = statusCode;
        ErrorCode = IggyErrorCodeExtensions.FromStatusCode(statusCode);
    }
}
