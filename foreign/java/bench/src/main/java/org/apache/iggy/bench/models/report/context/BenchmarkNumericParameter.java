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

package org.apache.iggy.bench.models.report.context;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public record BenchmarkNumericParameter(int min, int max) {

    public BenchmarkNumericParameter {
        if (min < 0) {
            throw new IllegalArgumentException("min cannot be negative");
        }
        if (max < min) {
            throw new IllegalArgumentException("max cannot be less than min");
        }
    }

    public static BenchmarkNumericParameter ofValue(int value) {
        return new BenchmarkNumericParameter(value, value);
    }

    @JsonValue
    public Object jsonValue() {
        return isFixed() ? min : min + ".." + max;
    }

    @JsonCreator
    public static BenchmarkNumericParameter fromJson(Object value) {
        if (value instanceof Number number) {
            return ofValue(number.intValue());
        }

        if (value instanceof String stringValue) {
            String[] parts = stringValue.split("\\.\\.", -1);
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid range format: " + stringValue);
            }

            int min = Integer.parseInt(parts[0]);
            int max = Integer.parseInt(parts[1]);
            return new BenchmarkNumericParameter(min, max);
        }

        throw new IllegalArgumentException("Unsupported benchmark numeric parameter value: " + value);
    }

    public boolean isFixed() {
        return min == max;
    }
}
