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

package org.apache.iggy.exception;

import java.util.Optional;

/** A local recovery outcome carrying the exact unsigned wire operation id. */
public final class IggyOperationException extends IggyServerException {
    private final long operationId;

    public IggyOperationException(IggyErrorCode code, long operationId) {
        super(
                code,
                code.getCode(),
                "Operation " + Long.toUnsignedString(operationId),
                Optional.empty(),
                Optional.empty());
        this.operationId = operationId;
    }

    /** Returns the u64 wire bits. Negative values are valid operation ids. */
    public long getOperationId() {
        return operationId;
    }
}
