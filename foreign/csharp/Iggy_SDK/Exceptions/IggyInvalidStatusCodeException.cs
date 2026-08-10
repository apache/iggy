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

namespace Apache.Iggy.Exceptions;

/// <summary>
///     Exception thrown when the status code returned by the server is not valid.
/// </summary>
public sealed class IggyInvalidStatusCodeException : Exception
{
    /// <summary>
    ///     Status code returned by the server.
    /// </summary>
    public int StatusCode { get; }

    /// <summary>
    ///     Whether the status code was reported by the server rather than raised by the client. The two share one
    ///     code space, and only a server verdict may drive retry or failover: a locally raised code says nothing
    ///     about what the cluster did with the request.
    /// </summary>
    public bool FromServer { get; }

    internal IggyInvalidStatusCodeException(int statusCode, string message, bool fromServer = false) : base(message)
    {
        StatusCode = statusCode;
        FromServer = fromServer;
    }
}
