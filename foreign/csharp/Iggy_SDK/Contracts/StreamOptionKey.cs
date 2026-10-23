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

namespace Apache.Iggy.Contracts;

/// <summary>
///     The client-visible option keys of a stream, in catalog order. Names a key to reset in a
///     <see cref="StreamUpdate" />; no stream key is resettable yet, so a reset is refused before it is sent.
/// </summary>
public enum StreamOptionKey
{
    /// <summary><c>name</c>, updatable.</summary>
    Name,

    /// <summary><c>encryption_key</c>, create-only and write-only.</summary>
    EncryptionKey,

    /// <summary><c>encryption_key_source</c>, read-only.</summary>
    EncryptionKeySource,

    /// <summary><c>encryption_key_fingerprint</c>, read-only.</summary>
    EncryptionKeyFingerprint,

    /// <summary><c>encryption_kek_fingerprint</c>, read-only.</summary>
    EncryptionKekFingerprint
}
