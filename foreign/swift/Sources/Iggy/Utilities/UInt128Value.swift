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

/// An unsigned 128-bit integer split into two 64-bit halves.
///
/// Message identifiers and 128-bit user headers travel as `u128` on the wire.
/// The standard library's `UInt128` is only available from macOS 15 / iOS 18,
/// so the SDK carries its own representation that works on every supported
/// platform. `low` holds the least significant 64 bits.
public struct UInt128Value: Sendable, Hashable, Codable {
    public var low: UInt64
    public var high: UInt64

    public init(low: UInt64, high: UInt64) {
        self.low = low
        self.high = high
    }

    public init(_ value: UInt64) {
        self.init(low: value, high: 0)
    }

    /// The 16 bytes of a UUID interpreted as a little-endian integer, which is
    /// how the Rust SDK derives message identifiers from `Uuid::to_u128_le`.
    public init(uuid: UUID) {
        let bytes = withUnsafeBytes(of: uuid.uuid) { Array($0) }
        low = UInt64(littleEndianBytes: bytes[0..<8])
        high = UInt64(littleEndianBytes: bytes[8..<16])
    }

    public static let zero = UInt128Value(low: 0, high: 0)

    /// A random identifier, never zero.
    public static func random() -> UInt128Value {
        var value = UInt128Value(uuid: UUID())
        if value == .zero {
            value.low = 1
        }
        return value
    }

    public var isZero: Bool { low == 0 && high == 0 }

    /// Little-endian wire bytes.
    public var littleEndianBytes: [UInt8] {
        low.littleEndianBytes + high.littleEndianBytes
    }

    public init(littleEndianBytes bytes: ArraySlice<UInt8>) {
        precondition(bytes.count == 16, "a 128-bit value needs exactly 16 bytes")
        let start = bytes.startIndex
        low = UInt64(littleEndianBytes: bytes[start..<start + 8])
        high = UInt64(littleEndianBytes: bytes[start + 8..<start + 16])
    }

    /// The value reinterpreted as a UUID (bytes in little-endian order).
    public var uuid: UUID {
        let bytes = littleEndianBytes
        return UUID(
            uuid: (
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
            ))
    }
}

extension UInt128Value: ExpressibleByIntegerLiteral {
    public init(integerLiteral value: UInt64) {
        self.init(value)
    }
}

extension UInt128Value: Comparable {
    public static func < (lhs: UInt128Value, rhs: UInt128Value) -> Bool {
        if lhs.high != rhs.high {
            return lhs.high < rhs.high
        }
        return lhs.low < rhs.low
    }
}

extension UInt128Value: CustomStringConvertible {
    /// Decimal rendering, matching how the Rust SDK prints a `u128`.
    public var description: String {
        if high == 0 {
            return String(low)
        }
        var digits: [UInt8] = []
        var currentHigh = high
        var currentLow = low
        while currentHigh != 0 || currentLow != 0 {
            let (quotientHigh, remainderHigh) = currentHigh.quotientAndRemainder(dividingBy: 10)
            let (quotientLow, remainderLow) = UInt64.divide(high: remainderHigh, low: currentLow, by: 10)
            digits.append(UInt8(remainderLow))
            currentHigh = quotientHigh
            currentLow = quotientLow
        }
        return String(digits.reversed().map { Character(String($0)) })
    }

    /// Parses the decimal rendering produced by ``description``.
    public init?(_ text: String) {
        guard !text.isEmpty, text.utf8.allSatisfy({ $0 >= 48 && $0 <= 57 }) else {
            return nil
        }
        var value = UInt128Value.zero
        for digit in text.utf8 {
            let (mulHigh, mulLow, mulOverflow) = value.multipliedBy10()
            if mulOverflow {
                return nil
            }
            let (low, carry) = mulLow.addingReportingOverflow(UInt64(digit - 48))
            let (high, overflow) = mulHigh.addingReportingOverflow(carry ? 1 : 0)
            if overflow {
                return nil
            }
            value = UInt128Value(low: low, high: high)
        }
        self = value
    }

    private func multipliedBy10() -> (high: UInt64, low: UInt64, overflow: Bool) {
        let (lowHigh, lowLow) = low.multipliedFullWidth(by: 10)
        let (highHigh, highLow) = high.multipliedFullWidth(by: 10)
        let (newHigh, carry) = highLow.addingReportingOverflow(lowHigh)
        return (newHigh, lowLow, highHigh != 0 || carry)
    }
}

extension UInt64 {
    /// Divides the 128-bit value `high:low` by `divisor`, assuming the quotient
    /// fits in 64 bits (`high < divisor`).
    fileprivate static func divide(high: UInt64, low: UInt64, by divisor: UInt64) -> (quotient: UInt64, remainder: UInt64) {
        divisor.dividingFullWidth((high: high, low: low))
    }
}

/// Identifier of a message, unique within a partition.
public typealias MessageID = UInt128Value
