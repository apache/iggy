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

import Foundation

/// Keys of the stream option catalog a client can name, in catalog order.
public enum StreamOptionKey: String, Sendable, Hashable, CaseIterable {
    /// The stream's name. Updatable, never resettable.
    case name
    /// The stream's own data key. Create-only and write-only.
    case encryptionKey = "encryption_key"
    /// Where the data key came from. Read-only.
    case encryptionKeySource = "encryption_key_source"
    /// Fingerprint of the data key. Read-only.
    case encryptionKeyFingerprint = "encryption_key_fingerprint"
    /// Fingerprints of the server keys that wrap the data key. Read-only.
    case encryptionKekFingerprint = "encryption_kek_fingerprint"
}

/// Where a stream's data key came from, as reads report it.
public enum StreamEncryptionSource: String, Sendable, Hashable, CaseIterable {
    /// The client sent the key with the create.
    case stream
    /// The server generated the key, because encryption was enabled when the
    /// stream was created.
    case server
    /// The stream has no key and stays plaintext for life. `none` on the wire.
    case plaintext = "none"
}

/// Options of a stream being created.
///
/// ``encryptionKey`` gives the stream its own data key: exactly 32 random
/// bytes, ``EncryptionKey/decode(base64:)`` reads one from base64. The key
/// travels inside the create request over whatever transport the client
/// uses, so protecting it in transit with TLS is the caller's choice. The
/// server stores it wrapped and never returns it: reads report only
/// ``StreamInfo/encryptionKeySource`` and the fingerprints. Without a key, a
/// server with encryption enabled generates one for the stream.
///
/// `raw` carries keys this SDK has no typed field for, as strings the server
/// parses. The SDK's own descriptions of the value never show the key.
public struct StreamCreateOptions: Sendable, Hashable {
    public var encryptionKey: Data?
    public var raw: [String: String]

    public init(encryptionKey: Data? = nil, raw: [String: String] = [:]) {
        self.encryptionKey = encryptionKey
        self.raw = raw
    }

    func toResourceOptions() throws -> ResourceOptions {
        var options = try rawOptions(raw)
        if let encryptionKey {
            try EncryptionKey.validate(encryptionKey)
            options[StreamOptionKey.encryptionKey.rawValue] = .explicit(try .raw(Array(encryptionKey)))
        }
        return options
    }

    func encode() throws -> [UInt8] {
        try OptionsBlock.encode(try toResourceOptions())
    }
}

/// Options of a stream being updated. Absent keys keep their stored values.
public struct StreamUpdateOptions: Sendable, Hashable {
    /// Nil keeps the stream's name.
    public var name: String?
    /// Keys to return to their defaults. No stream key is resettable yet, so
    /// a non-empty set is refused before anything is sent.
    public var reset: Set<StreamOptionKey>
    /// Updatable keys newer than this SDK, as strings the server parses.
    public var raw: [String: String]
    /// Reset names newer than this SDK. The server alone decides whether a
    /// patch carrying raw keys or names is admitted.
    public var rawReset: Set<String>

    public init(name: String? = nil, reset: Set<StreamOptionKey> = [], raw: [String: String] = [:], rawReset: Set<String> = []) {
        self.name = name
        self.reset = reset
        self.raw = raw
        self.rawReset = rawReset
    }

    func toResourceOptions() throws -> ResourceOptions {
        var options = try rawOptions(raw)
        if let name {
            options[StreamOptionKey.name.rawValue] = .explicit(try columnValue(name, refusal: .invalidStreamName))
        }
        return options
    }

    func toPatch() throws -> OptionsPatch {
        try OptionsPatch(typed: name == nil ? [] : [.name], raw: raw, reset: reset, rawReset: rawReset, values: toResourceOptions)
    }
}

extension StreamOptionKey: CatalogOptionKey {
    var flags: OptionFlags {
        switch self {
        case .name: .updatable
        case .encryptionKey: [.secret, .critical]
        case .encryptionKeySource, .encryptionKeyFingerprint, .encryptionKekFingerprint: .readOnly
        }
    }
}

extension StreamCreateOptions: CustomStringConvertible, CustomDebugStringConvertible, CustomReflectable {
    public var description: String {
        "StreamCreateOptions(encryptionKey: \(encryptionKey == nil ? "nil" : redacted), raw: \(redactedRaw))"
    }

    public var debugDescription: String {
        description
    }

    public var customMirror: Mirror {
        Mirror(self, children: ["encryptionKey": encryptionKey.map { _ in redacted } as Any, "raw": redactedRaw], displayStyle: .struct)
    }

    private var redacted: String { "<redacted>" }

    private var redactedRaw: [String: String] {
        raw.reduce(into: [:]) { shown, entry in
            shown[entry.key] = entry.key == StreamOptionKey.encryptionKey.rawValue ? redacted : entry.value
        }
    }
}

extension StreamInfo {
    /// Where the stream's data key came from. Nil when the server reported
    /// a source this SDK does not know.
    public var encryptionKeySource: StreamEncryptionSource? {
        streamEncryptionSource(options)
    }

    /// Fingerprint of the stream's data key, nil for a plaintext stream.
    /// Compare it with ``EncryptionKey/fingerprint(_:)`` of a key.
    public var encryptionKeyFingerprint: String? {
        options[StreamOptionKey.encryptionKeyFingerprint.rawValue]?.value.stringValue
    }

    /// Fingerprints of the server keys wrapping the data key. More than one
    /// while the server's key is being rotated, none for a plaintext stream.
    public var encryptionKekFingerprints: [String] {
        kekFingerprints(options)
    }
}

extension StreamDetails {
    /// Where the stream's data key came from. Nil when the server reported
    /// a source this SDK does not know.
    public var encryptionKeySource: StreamEncryptionSource? {
        streamEncryptionSource(options)
    }

    /// Fingerprint of the stream's data key, nil for a plaintext stream.
    /// Compare it with ``EncryptionKey/fingerprint(_:)`` of a key.
    public var encryptionKeyFingerprint: String? {
        options[StreamOptionKey.encryptionKeyFingerprint.rawValue]?.value.stringValue
    }

    /// Fingerprints of the server keys wrapping the data key. More than one
    /// while the server's key is being rotated, none for a plaintext stream.
    public var encryptionKekFingerprints: [String] {
        kekFingerprints(options)
    }
}

private func streamEncryptionSource(_ options: ResourceOptions) -> StreamEncryptionSource? {
    options[StreamOptionKey.encryptionKeySource.rawValue]?.value.stringValue.flatMap(StreamEncryptionSource.init(rawValue:))
}

private func kekFingerprints(_ options: ResourceOptions) -> [String] {
    options[StreamOptionKey.encryptionKekFingerprint.rawValue]?.value.stringValue?.split(separator: ",").map(String.init) ?? []
}
