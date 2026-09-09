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

/// Packed semantic version of the binary protocol: ten bits per component,
/// `major << 20 | minor << 10 | patch`. Integer order equals semver order.
public struct ProtocolVersion: Sendable, Hashable, Comparable, CustomStringConvertible {
    public let packed: UInt32

    public init(packed: UInt32) {
        self.packed = packed
    }

    public init(major: UInt32, minor: UInt32, patch: UInt32) {
        precondition(major < 1024 && minor < 1024 && patch < 1024, "semver component exceeds 10-bit packing range")
        packed = (major << 20) | (minor << 10) | patch
    }

    /// The protocol version this SDK implements, tracking the
    /// `iggy_binary_protocol` crate it was ported from.
    public static let current = ProtocolVersion(major: 0, minor: 11, patch: 0)

    public var major: UInt32 { packed >> 20 }
    public var minor: UInt32 { (packed >> 10) & 0x3FF }
    public var patch: UInt32 { packed & 0x3FF }

    public var description: String { "\(major).\(minor).\(patch)" }

    public static func < (lhs: ProtocolVersion, rhs: ProtocolVersion) -> Bool {
        lhs.packed < rhs.packed
    }
}
