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

namespace Apache.Iggy.Tools.ErrorCodeGenerator;

/// <summary>
///     Defines error categories based on error name patterns.
/// </summary>
public static class ErrorCategories
{
    public static readonly Dictionary<string, Func<string, bool>> Categories = new()
    {
        ["IsNotFound"] = name =>
            name.Contains("NotFound") ||
            name == "ResourceNotFound" ||
            name == "StateFileNotFound",
        ["IsAlreadyExists"] = name =>
            name.Contains("AlreadyExists"),
        ["IsAuthenticationError"] = name =>
            name is "Unauthenticated" or "InvalidCredentials" or "InvalidUsername" or "InvalidPassword"
                or "InvalidPersonalAccessToken" or "PersonalAccessTokenExpired" or "JwtMissing"
                or "InvalidAccessToken" or "AccessTokenMissing",
        ["IsConnectionError"] = name =>
            name is "Disconnected" or "CannotEstablishConnection" or "StaleClient" or "TcpError"
                or "QuicError" or "NotConnected" or "ClientShutdown" or "ConnectionClosed"
                or "CannotSendMessagesDueToClientDisconnection" or "BackgroundWorkerDisconnected" ||
            name.StartsWith("WebSocket"),
        ["IsStreamError"] = name =>
            name.Contains("Stream") && !name.Contains("Connection"),
        ["IsTopicError"] = name =>
            name.Contains("Topic") || name is "InvalidReplicationFactor" or "InvalidPartitionsCount"
                or "TooManyPartitions",
        ["IsConsumerGroupError"] = name =>
            name.Contains("ConsumerGroup"),
        ["IsMessageError"] = name =>
            (name.Contains("Message") && !name.Contains("Batch")) ||
            (name.Contains("Header") && !name.Contains("Batch")) ||
            name is "TooManyMessages" or "EmptyMessagePayload" or "InvalidKeyValueLength",
        ["IsUserError"] = name =>
            name is "InvalidUsername" or "InvalidPassword" or "InvalidUserStatus" or "UserAlreadyExists"
                or "UserInactive" or "CannotDeleteUser" or "CannotChangePermissions" or "UsersLimitReached",
        ["IsPersonalAccessTokenError"] = name =>
            name.Contains("PersonalAccessToken"),
        ["IsTlsError"] = name =>
            name.Contains("Tls") || name == "FailedToAddCertificate",
        ["IsValidationError"] = name =>
            name.StartsWith("Invalid") &&
            !name.Contains("Token") &&
            !name.Contains("Credentials"),
        ["IsLimitReached"] = name =>
            name.Contains("LimitReached") || name.Contains("TooMany") || name.Contains("TooBig")
            || name is "TopicFull" or "BackgroundSendBufferFull" or "BackgroundSendBufferOverflow"
    };
}
