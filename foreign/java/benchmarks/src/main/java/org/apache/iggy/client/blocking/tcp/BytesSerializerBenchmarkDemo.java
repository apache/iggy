/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.client.blocking.tcp;

import io.netty.buffer.ByteBuf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

/**
 * Simple demo benchmark showing JMH usage for BytesSerializer.
 * <p>
 * This is a minimal example without external dependencies (no test containers, no external servers).
 * It only benchmarks pure serialization logic.
 * <p>
 * NOTE: Full integration benchmarks with real network I/O require different infrastructure
 * and are not suitable for JMH microbenchmarking.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BytesSerializerBenchmarkDemo {

    @Param({"100", "10000", "1000000"})
    private long value;

    private BigInteger testBigInteger;

    @Setup(Level.Trial)
    public void setup() {
        testBigInteger = BigInteger.valueOf(value);
    }

    /**
     * Benchmark BigInteger to U64 conversion.
     * This is used internally for offset, timestamp, checksum serialization.
     */
    @Benchmark
    public ByteBuf toBytesAsU64() {
        return BytesSerializer.toBytesAsU64(testBigInteger);
    }

    /**
     * Benchmark BigInteger to U128 conversion.
     * This is used for message ID serialization.
     */
    @Benchmark
    public ByteBuf toBytesAsU128() {
        return BytesSerializer.toBytesAsU128(testBigInteger);
    }
}
