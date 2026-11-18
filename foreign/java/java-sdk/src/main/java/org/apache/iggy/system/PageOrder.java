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

package org.apache.iggy.system;

/** The order of a list page. */
public enum PageOrder {
    /** The numeric ids. */
    ID(0, "id"),
    /**
     * The creation order. Entities created at the same time keep the order of their ids. Partitions and consumer
     * groups walk by id in this order too, because their ids follow creation.
     */
    CREATED(1, "created");

    private final int code;
    private final String value;

    PageOrder(int code, String value) {
        this.code = code;
        this.value = value;
    }

    public int asCode() {
        return code;
    }

    /** The name of the order in an HTTP query. */
    public String value() {
        return value;
    }
}
