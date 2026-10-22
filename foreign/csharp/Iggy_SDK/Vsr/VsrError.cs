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

using Apache.Iggy.Exceptions;

namespace Apache.Iggy.Vsr;

/// <summary>
///     Server error codes the VSR paths raise locally or surface from the wire. Values match
///     <c>core/common/src/error/iggy_error.rs</c>; the server shares this code space across the reply
///     status word, the committed result section and the eviction mapping.
/// </summary>
internal static class VsrError
{
    internal const int INVALID_COMMAND = 3;
    internal const int INVALID_FORMAT = 4;
    internal const int FEATURE_UNAVAILABLE = 5;
    internal const int INVALID_IDENTIFIER = 6;
    internal const int STALE_CLIENT = 30;
    internal const int UNAUTHENTICATED = 40;
    internal const int UNAUTHORIZED = 41;
    internal const int INVALID_CREDENTIALS = 42;
    internal const int INVALID_USERNAME = 43;
    internal const int INVALID_PASSWORD = 44;
    internal const int USER_INACTIVE = 47;
    internal const int PERSONAL_ACCESS_TOKEN_ALREADY_EXISTS = 51;
    internal const int INVALID_PERSONAL_ACCESS_TOKEN = 53;
    internal const int PERSONAL_ACCESS_TOKEN_EXPIRED = 54;
    internal const int TRANSIENT_NOT_COMMITTED = 57;
    internal const int TRANSIENT_NOT_ACCEPTED = 58;
    internal const int INVALID_ENCRYPTION_KEY = 70;
    internal const int INVALID_SIZE_BYTES = 80;
    internal const int REQUEST_TOO_OLD = 85;
    internal const int HISTORY_UNAVAILABLE = 87;
    internal const int LIFECYCLE_BUSY = 88;
    internal const int NOT_PRIMARY = 89;
    internal const int EMPTY_RESPONSE = 304;
    internal const int STREAM_ID_NOT_FOUND = 1009;
    internal const int STREAM_NAME_NOT_FOUND = 1010;
    internal const int INVALID_STREAM_NAME = 1013;
    internal const int TOPIC_ID_NOT_FOUND = 2010;
    internal const int TOPIC_NAME_NOT_FOUND = 2011;
    internal const int INVALID_TOPIC_NAME = 2014;
    internal const int CONSUMER_GROUP_MEMBER_NOT_FOUND = 5006;
    internal const int PARTITION_NOT_FOUND = 3007;
    internal const int CONSUMER_GROUP_PARTITION_NOT_OWNED = 5009;
    internal const int TOO_MANY_CONSUMER_GROUPS = 5010;
    internal const int UNSUPPORTED_OPTION_KEY = 4041;
    internal const int OPTION_NOT_UPDATABLE = 4103;
    internal const int OPTION_NOT_RESETTABLE = 4104;
    internal const int READ_ONLY_OPTION = 4105;
    internal const int CANNOT_MODIFY_ROOT_USER = 4108;
    internal const int INCOMPATIBLE_PROTOCOL_VERSION = 14003;
    internal const int SESSION_MISMATCH = 14004;
    internal const int REQUEST_ID_EXHAUSTED = 14005;

    /// <summary>A failure the client raised itself, before or instead of a server verdict.</summary>
    internal static IggyInvalidStatusCodeException Exception(int code, string message, Exception? cause = null)
    {
        return new IggyInvalidStatusCodeException(code, message, innerException: cause);
    }

    /// <summary>
    ///     A verdict the server reported, on the reply status word or in an eviction frame. Retry and failover act
    ///     on server verdicts, these and <see cref="FromResultSection" /> ones, plus the TRANSIENT_NOT_ACCEPTED the
    ///     routed poll path raises itself when a primary does not admit it.
    /// </summary>
    internal static IggyInvalidStatusCodeException FromServer(int code, string message)
    {
        return new IggyInvalidStatusCodeException(code, message, true);
    }

    /// <summary>A verdict the server reported in the result section of a reply.</summary>
    internal static IggyInvalidStatusCodeException FromResultSection(int code, string message)
    {
        return new IggyInvalidStatusCodeException(code, message, true) { FromResultSection = true };
    }
}
