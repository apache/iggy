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

namespace Apache.Iggy.Enums;

/// <summary>Who chose the key that encrypts a stream's data at rest.</summary>
public enum StreamEncryptionSource
{
    /// <summary>The stream is stored in plaintext.</summary>
    None,

    /// <summary>The client supplied the key when it created the stream.</summary>
    Stream,

    /// <summary>The server generated the key because its encryption was enabled.</summary>
    Server
}
