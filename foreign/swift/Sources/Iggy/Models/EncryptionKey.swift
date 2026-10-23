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

import Crypto
import Foundation

/// A stream's own data encryption key: exactly 32 random bytes, sent with
/// ``StreamCreateOptions/encryptionKey``. Never derive one from a password,
/// since its fingerprint is public to anyone who can read the stream.
public enum EncryptionKey {
    /// Byte length of a stream encryption key.
    public static let length = 32

    private static let fingerprintLabel = Array("iggy/stream-key-fingerprint/v1".utf8)
    private static let fingerprintLength = 8

    /// The fingerprint reads report in ``StreamInfo/encryptionKeyFingerprint``
    /// for a stream encrypted under `key`: the first 8 bytes of a labelled
    /// SHA-256 of the key, as 16 lowercase hex characters. It names the key
    /// without revealing it.
    public static func fingerprint(_ key: Data) throws -> String {
        try validate(key)
        var hasher = SHA256()
        hasher.update(data: fingerprintLabel)
        hasher.update(data: key)
        return hasher.finalize().prefix(fingerprintLength).map { String($0, radix: 16).leftPadded(to: 2) }.joined()
    }

    /// ``fingerprint(_:)`` of a key given as base64, decoded by
    /// ``decode(base64:)``.
    public static func fingerprint(base64 key: String) throws -> String {
        try fingerprint(decode(base64: key))
    }

    /// Decodes a key from canonical standard padded base64 of exactly 32
    /// bytes. Whitespace, the URL-safe alphabet, missing padding and
    /// noncanonical trailing bits are refused, as the server refuses them.
    public static func decode(base64 text: String) throws -> Data {
        guard let key = Data(base64Encoded: text), key.base64EncodedString() == text else {
            throw IggyError(.invalidEncryptionKey, context: "stream encryption key must be canonical standard padded base64")
        }
        try validate(key)
        return key
    }

    static func validate(_ key: Data) throws {
        guard key.count == length else {
            throw IggyError(.invalidEncryptionKey, context: "stream encryption key must be \(length) bytes, got \(key.count)")
        }
    }
}
