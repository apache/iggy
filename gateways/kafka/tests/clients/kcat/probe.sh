#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Client matrix probe for kcat (librdkafka 1.7, the official image). Speaks the probe contract in
# ../README.md. kcat's producer sets headers per invocation, so each record is its own `kcat -P`;
# it reports no delivery offsets, so ACK lines carry `-`. Its balanced consumer (`-G`) commits on
# close what it handed out, and it cannot print committed offsets, so there are no COMMITTED lines.
set -u
: "${BOOTSTRAP:?}" "${TOPIC:?}" "${GROUP:?}"
COUNT=${COUNT:-12}

produce() {
    phase=$1
    first=$2
    index=$first
    while [ "$index" -lt $((first + COUNT)) ]; do
        n=$(printf '%03d' "$index")
        if printf 'k%s:v%s' "$n" "$n" |
            kcat -P -b "$BOOTSTRAP" -t "$TOPIC" -K: -H "idx=$index" -H src=matrix \
                -X client.id=matrix-kcat 2>/dev/null; then
            echo "ACK $phase - -"
        else
            echo "NACK $phase kcat_exit"
        fi
        index=$((index + 1))
    done
}

consume() {
    phase=$1
    # -c exits after COUNT records; librdkafka commits the offsets it stored on close.
    # Topics come last: in -G mode every argument after the group is a topic.
    timeout 45 kcat -b "$BOOTSTRAP" -c "$COUNT" -q \
        -X client.id=matrix-kcat -X auto.offset.reset=earliest \
        -f "REC $phase %p %o %k %s %h\n" -G "$GROUP" "$TOPIC" 2>/dev/null |
        sed 's/ $/ -/'
}

produce produce 0
consume consume
produce produce2 "$COUNT"
consume resume
echo DONE
