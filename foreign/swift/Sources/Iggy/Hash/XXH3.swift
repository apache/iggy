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

/// XXH3 64-bit hash with the default secret and a zero seed.
///
/// Every message frame and batch on the wire is checksummed with this hash,
/// and the server verifies the values, so the implementation has to match the
/// reference bit for bit. It is checked against vectors produced by the Rust
/// `twox-hash` crate in the test suite.
enum XXH3 {
    private static let prime32_1: UInt64 = 0x9E37_79B1
    private static let prime32_2: UInt64 = 0x85EB_CA77
    private static let prime32_3: UInt64 = 0xC2B2_AE3D
    private static let prime64_1: UInt64 = 0x9E37_79B1_85EB_CA87
    private static let prime64_2: UInt64 = 0xC2B2_AE3D_27D4_EB4F
    private static let prime64_3: UInt64 = 0x1656_67B1_9E37_79F9
    private static let prime64_4: UInt64 = 0x85EB_CA77_C2B2_AE63
    private static let prime64_5: UInt64 = 0x27D4_EB2F_1656_67C5
    private static let primeMX1: UInt64 = 0x1656_6791_9E37_79F9
    private static let primeMX2: UInt64 = 0x9FB2_1C65_1E98_DF25

    private static let stripeLength = 64
    private static let secretConsumeRate = 8
    private static let accumulatorCount = 8
    private static let secretLength = 192
    private static let stripesPerBlock = (secretLength - stripeLength) / secretConsumeRate
    private static let blockLength = stripeLength * stripesPerBlock
    private static let secretLastAccumulatorStart = 7
    private static let secretMergeAccumulatorsStart = 11
    private static let midsizeStartOffset = 3
    private static let midsizeLastOffset = 17
    private static let secretSizeMin = 136

    private static let secret: [UInt8] = [
        0xb8, 0xfe, 0x6c, 0x39, 0x23, 0xa4, 0x4b, 0xbe, 0x7c, 0x01, 0x81, 0x2c, 0xf7, 0x21, 0xad, 0x1c,
        0xde, 0xd4, 0x6d, 0xe9, 0x83, 0x90, 0x97, 0xdb, 0x72, 0x40, 0xa4, 0xa4, 0xb7, 0xb3, 0x67, 0x1f,
        0xcb, 0x79, 0xe6, 0x4e, 0xcc, 0xc0, 0xe5, 0x78, 0x82, 0x5a, 0xd0, 0x7d, 0xcc, 0xff, 0x72, 0x21,
        0xb8, 0x08, 0x46, 0x74, 0xf7, 0x43, 0x24, 0x8e, 0xe0, 0x35, 0x90, 0xe6, 0x81, 0x3a, 0x26, 0x4c,
        0x3c, 0x28, 0x52, 0xbb, 0x91, 0xc3, 0x00, 0xcb, 0x88, 0xd0, 0x65, 0x8b, 0x1b, 0x53, 0x2e, 0xa3,
        0x71, 0x64, 0x48, 0x97, 0xa2, 0x0d, 0xf9, 0x4e, 0x38, 0x19, 0xef, 0x46, 0xa9, 0xde, 0xac, 0xd8,
        0xa8, 0xfa, 0x76, 0x3f, 0xe3, 0x9c, 0x34, 0x3f, 0xf9, 0xdc, 0xbb, 0xc7, 0xc7, 0x0b, 0x4f, 0x1d,
        0x8a, 0x51, 0xe0, 0x4b, 0xcd, 0xb4, 0x59, 0x31, 0xc8, 0x9f, 0x7e, 0xc9, 0xd9, 0x78, 0x73, 0x64,
        0xea, 0xc5, 0xac, 0x83, 0x34, 0xd3, 0xeb, 0xc3, 0xc5, 0x81, 0xa0, 0xff, 0xfa, 0x13, 0x63, 0xeb,
        0x17, 0x0d, 0xdd, 0x51, 0xb7, 0xf0, 0xda, 0x49, 0xd3, 0x16, 0x55, 0x26, 0x29, 0xd4, 0x68, 0x9e,
        0x2b, 0x16, 0xbe, 0x58, 0x7d, 0x47, 0xa1, 0xfc, 0x8f, 0xf8, 0xb8, 0xd1, 0x7a, 0xd0, 0x31, 0xce,
        0x45, 0xcb, 0x3a, 0x8f, 0x95, 0x16, 0x04, 0x28, 0xaf, 0xd7, 0xfb, 0xca, 0xbb, 0x4b, 0x40, 0x7e,
    ]

    static func hash64(_ bytes: [UInt8]) -> UInt64 {
        bytes.withUnsafeBytes { hash64($0) }
    }

    static func hash64(_ bytes: ArraySlice<UInt8>) -> UInt64 {
        bytes.withUnsafeBytes { hash64($0) }
    }

    static func hash64(_ input: UnsafeRawBufferPointer) -> UInt64 {
        secret.withUnsafeBytes { secret in
            let length = input.count
            switch length {
            case 0:
                return avalanche64(read64(secret, 56) ^ read64(secret, 64))
            case 1...3:
                return hash1to3(input, secret)
            case 4...8:
                return hash4to8(input, secret)
            case 9...16:
                return hash9to16(input, secret)
            case 17...128:
                return hash17to128(input, secret)
            case 129...240:
                return hash129to240(input, secret)
            default:
                return hashLong(input, secret)
            }
        }
    }

    @inline(__always)
    private static func read32(_ buffer: UnsafeRawBufferPointer, _ offset: Int) -> UInt32 {
        UInt32(littleEndian: buffer.loadUnaligned(fromByteOffset: offset, as: UInt32.self))
    }

    @inline(__always)
    private static func read64(_ buffer: UnsafeRawBufferPointer, _ offset: Int) -> UInt64 {
        UInt64(littleEndian: buffer.loadUnaligned(fromByteOffset: offset, as: UInt64.self))
    }

    @inline(__always)
    private static func rotl(_ value: UInt64, _ count: UInt64) -> UInt64 {
        (value << count) | (value >> (64 - count))
    }

    @inline(__always)
    private static func mul128Fold64(_ lhs: UInt64, _ rhs: UInt64) -> UInt64 {
        let product = lhs.multipliedFullWidth(by: rhs)
        return product.high ^ product.low
    }

    @inline(__always)
    private static func avalanche64(_ input: UInt64) -> UInt64 {
        var hash = input
        hash ^= hash >> 33
        hash &*= prime64_2
        hash ^= hash >> 29
        hash &*= prime64_3
        hash ^= hash >> 32
        return hash
    }

    @inline(__always)
    private static func avalanche(_ input: UInt64) -> UInt64 {
        var hash = input
        hash ^= hash >> 37
        hash &*= primeMX1
        hash ^= hash >> 32
        return hash
    }

    @inline(__always)
    private static func rrmxmx(_ input: UInt64, _ length: Int) -> UInt64 {
        var hash = input
        hash ^= rotl(hash, 49) ^ rotl(hash, 24)
        hash &*= primeMX2
        hash ^= (hash >> 35) &+ UInt64(length)
        hash &*= primeMX2
        return hash ^ (hash >> 28)
    }

    private static func hash1to3(_ input: UnsafeRawBufferPointer, _ secret: UnsafeRawBufferPointer) -> UInt64 {
        let length = input.count
        let byte1 = UInt32(input[0])
        let byte2 = UInt32(input[length >> 1])
        let byte3 = UInt32(input[length - 1])
        let combined = (byte1 << 16) | (byte2 << 24) | byte3 | (UInt32(length) << 8)
        let bitflip = UInt64(read32(secret, 0) ^ read32(secret, 4))
        return avalanche64(UInt64(combined) ^ bitflip)
    }

    private static func hash4to8(_ input: UnsafeRawBufferPointer, _ secret: UnsafeRawBufferPointer) -> UInt64 {
        let length = input.count
        let input1 = read32(input, 0)
        let input2 = read32(input, length - 4)
        let bitflip = read64(secret, 8) ^ read64(secret, 16)
        let input64 = UInt64(input2) &+ (UInt64(input1) << 32)
        return rrmxmx(input64 ^ bitflip, length)
    }

    private static func hash9to16(_ input: UnsafeRawBufferPointer, _ secret: UnsafeRawBufferPointer) -> UInt64 {
        let length = input.count
        let bitflip1 = read64(secret, 24) ^ read64(secret, 32)
        let bitflip2 = read64(secret, 40) ^ read64(secret, 48)
        let inputLow = read64(input, 0) ^ bitflip1
        let inputHigh = read64(input, length - 8) ^ bitflip2
        let accumulator = UInt64(length) &+ inputLow.byteSwapped &+ inputHigh &+ mul128Fold64(inputLow, inputHigh)
        return avalanche(accumulator)
    }

    @inline(__always)
    private static func mix16(_ input: UnsafeRawBufferPointer, _ inputOffset: Int, _ secret: UnsafeRawBufferPointer, _ secretOffset: Int) -> UInt64 {
        let inputLow = read64(input, inputOffset)
        let inputHigh = read64(input, inputOffset + 8)
        return mul128Fold64(inputLow ^ read64(secret, secretOffset), inputHigh ^ read64(secret, secretOffset + 8))
    }

    private static func hash17to128(_ input: UnsafeRawBufferPointer, _ secret: UnsafeRawBufferPointer) -> UInt64 {
        let length = input.count
        var accumulator = UInt64(length) &* prime64_1
        if length > 32 {
            if length > 64 {
                if length > 96 {
                    accumulator &+= mix16(input, 48, secret, 96)
                    accumulator &+= mix16(input, length - 64, secret, 112)
                }
                accumulator &+= mix16(input, 32, secret, 64)
                accumulator &+= mix16(input, length - 48, secret, 80)
            }
            accumulator &+= mix16(input, 16, secret, 32)
            accumulator &+= mix16(input, length - 32, secret, 48)
        }
        accumulator &+= mix16(input, 0, secret, 0)
        accumulator &+= mix16(input, length - 16, secret, 16)
        return avalanche(accumulator)
    }

    private static func hash129to240(_ input: UnsafeRawBufferPointer, _ secret: UnsafeRawBufferPointer) -> UInt64 {
        let length = input.count
        var accumulator = UInt64(length) &* prime64_1
        let rounds = length / 16
        for round in 0..<8 {
            accumulator &+= mix16(input, 16 * round, secret, 16 * round)
        }
        accumulator = avalanche(accumulator)
        for round in 8..<rounds {
            accumulator &+= mix16(input, 16 * round, secret, 16 * (round - 8) + midsizeStartOffset)
        }
        accumulator &+= mix16(input, length - 16, secret, secretSizeMin - midsizeLastOffset)
        return avalanche(accumulator)
    }

    private static func hashLong(_ input: UnsafeRawBufferPointer, _ secret: UnsafeRawBufferPointer) -> UInt64 {
        let length = input.count
        var accumulators: [UInt64] = [prime32_3, prime64_1, prime64_2, prime64_3, prime64_4, prime32_2, prime64_5, prime32_1]
        let blocks = (length - 1) / blockLength
        accumulators.withUnsafeMutableBufferPointer { accumulators in
            for block in 0..<blocks {
                accumulate(accumulators, input, block * blockLength, secret, 0, stripes: stripesPerBlock)
                scramble(accumulators, secret, secretLength - stripeLength)
            }
            let stripes = ((length - 1) - blockLength * blocks) / stripeLength
            accumulate(accumulators, input, blocks * blockLength, secret, 0, stripes: stripes)
            accumulate512(accumulators, input, length - stripeLength, secret, secretLength - stripeLength - secretLastAccumulatorStart)
        }
        return mergeAccumulators(accumulators, secret, secretMergeAccumulatorsStart, start: UInt64(length) &* prime64_1)
    }

    @inline(__always)
    private static func accumulate512(
        _ accumulators: UnsafeMutableBufferPointer<UInt64>, _ input: UnsafeRawBufferPointer, _ inputOffset: Int, _ secret: UnsafeRawBufferPointer,
        _ secretOffset: Int
    ) {
        for lane in 0..<accumulatorCount {
            let dataValue = read64(input, inputOffset + 8 * lane)
            let dataKey = dataValue ^ read64(secret, secretOffset + 8 * lane)
            accumulators[lane ^ 1] &+= dataValue
            accumulators[lane] &+= UInt64(UInt32(truncatingIfNeeded: dataKey)) &* (dataKey >> 32)
        }
    }

    @inline(__always)
    private static func accumulate(
        _ accumulators: UnsafeMutableBufferPointer<UInt64>, _ input: UnsafeRawBufferPointer, _ inputOffset: Int, _ secret: UnsafeRawBufferPointer,
        _ secretOffset: Int, stripes: Int
    ) {
        for stripe in 0..<stripes {
            accumulate512(accumulators, input, inputOffset + stripe * stripeLength, secret, secretOffset + stripe * secretConsumeRate)
        }
    }

    @inline(__always)
    private static func scramble(_ accumulators: UnsafeMutableBufferPointer<UInt64>, _ secret: UnsafeRawBufferPointer, _ secretOffset: Int) {
        for lane in 0..<accumulatorCount {
            let key = read64(secret, secretOffset + 8 * lane)
            var value = accumulators[lane]
            value ^= value >> 47
            value ^= key
            value &*= prime32_1
            accumulators[lane] = value
        }
    }

    private static func mergeAccumulators(_ accumulators: [UInt64], _ secret: UnsafeRawBufferPointer, _ secretOffset: Int, start: UInt64) -> UInt64 {
        var result = start
        for pair in 0..<4 {
            let low = accumulators[2 * pair] ^ read64(secret, secretOffset + 16 * pair)
            let high = accumulators[2 * pair + 1] ^ read64(secret, secretOffset + 16 * pair + 8)
            result &+= mul128Fold64(low, high)
        }
        return avalanche(result)
    }
}
