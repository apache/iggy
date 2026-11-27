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

"""Client matrix probe for confluent-kafka-python (librdkafka 2.x).

Speaks the probe contract in ../README.md: it prints ACK, NACK, REC, COMMITTED and DONE lines on
stdout, and client_matrix_tests.rs turns them into facts. Modes:

- ``scenario``: produce a batch, consume it in a group, commit, close; produce a second batch,
  open a new consumer in the same group and read what it resumes with.
- ``produce``: produce one batch (``BATCH``, default 1), with ``VALUES=json`` for JSON values.
- ``read``: read every partition of ``TOPIC`` from the beginning to its end, without a group.
- ``committed``: print the committed offsets of ``GROUP`` on ``TOPIC``.

``read`` and ``committed`` label their lines with ``PHASE`` (default: the mode's name). Keys,
values and headers are percent-escaped for ``%``, space and newline; an empty one prints ``%``.
"""

import json
import os
import sys
import time

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition

BOOTSTRAP = os.environ["BOOTSTRAP"]
TOPIC = os.environ["TOPIC"]
GROUP = os.environ.get("GROUP", "")
COUNT = int(os.environ.get("COUNT", "12"))
DEADLINE_S = float(os.environ.get("DEADLINE_S", "45"))
PHASE = os.environ.get("PHASE", "")


def log(message):
    print(message, file=sys.stderr, flush=True)


def out(line):
    print(line, flush=True)


def text(raw):
    """A key, value or header as one word: event lines are split on spaces."""
    if raw is None:
        return "-"
    decoded = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw)
    return decoded.replace("%", "%25").replace(" ", "%20").replace("\n", "%0A") or "%"


def headers_text(headers):
    if not headers:
        return "-"
    return ",".join(f"{name}={text(value)}" for name, value in headers)


def record(index, json_values):
    key = f"k{index:03d}"
    if json_values:
        value = json.dumps({"i": index, "v": f"v{index:03d}"}, separators=(",", ":"))
    else:
        value = f"v{index:03d}"
    return key, value, [("idx", str(index)), ("src", "matrix")]


def produce(phase, batch, json_values=False):
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "matrix-python"})

    def delivered(error, message):
        if error is not None:
            out(f"NACK {phase} {error.name()}")
        else:
            out(f"ACK {phase} {message.partition()} {message.offset()}")

    first = (batch - 1) * COUNT
    for index in range(first, first + COUNT):
        key, value, headers = record(index, json_values)
        producer.produce(TOPIC, key=key, value=value, headers=headers, on_delivery=delivered)
        producer.poll(0)
    remaining = producer.flush(DEADLINE_S)
    for _ in range(remaining):
        out(f"NACK {phase} flush_timeout")


def consumer(group, extra=None):
    config = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group,
        "client.id": "matrix-python",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    config.update(extra or {})
    return Consumer(config)


def emit(phase, message):
    out(
        f"REC {phase} {message.partition()} {message.offset()} {text(message.key())} "
        f"{text(message.value())} {headers_text(message.headers())}"
    )


def consume(phase):
    """Reads COUNT records in GROUP, commits them synchronously, prints the committed offsets."""
    member = consumer(GROUP)
    member.subscribe([TOPIC])
    seen = 0
    deadline = time.monotonic() + DEADLINE_S
    while seen < COUNT and time.monotonic() < deadline:
        message = member.poll(0.5)
        if message is None:
            continue
        if message.error():
            log(f"{phase}: {message.error()}")
            continue
        emit(phase, message)
        seen += 1
    try:
        member.commit(asynchronous=False)
    except Exception as error:  # noqa: BLE001 - recorded as a fact, never raised
        out(f"NACK {phase}.commit {type(error).__name__}")
    committed(member, phase)
    member.close()


def partitions(client):
    metadata = client.list_topics(TOPIC, timeout=DEADLINE_S)
    return sorted(metadata.topics[TOPIC].partitions)


def committed(member, phase):
    wanted = [TopicPartition(TOPIC, partition) for partition in partitions(member)]
    for tp in member.committed(wanted, timeout=DEADLINE_S):
        out(f"COMMITTED {phase} {tp.partition} {tp.offset if tp.offset >= 0 else -1}")


def read_all():
    reader = consumer(f"matrix-read-{os.getpid()}")
    ends = {}
    for partition in partitions(reader):
        low, high = reader.get_watermark_offsets(
            TopicPartition(TOPIC, partition), timeout=DEADLINE_S
        )
        if high > low:
            ends[partition] = high
    reader.assign([TopicPartition(TOPIC, partition, 0) for partition in ends])
    deadline = time.monotonic() + DEADLINE_S
    while ends and time.monotonic() < deadline:
        message = reader.poll(0.5)
        if message is None:
            continue
        if message.error():
            if message.error().code() != KafkaError._PARTITION_EOF:
                log(f"read: {message.error()}")
            continue
        emit(PHASE or "read", message)
        if message.offset() + 1 >= ends.get(message.partition(), 0):
            ends.pop(message.partition(), None)
    reader.close()


def group_committed():
    admin_side = consumer(GROUP)
    committed(admin_side, PHASE or "committed")
    admin_side.close()


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "scenario"
    if mode == "scenario":
        produce("produce", 1)
        consume("consume")
        produce("produce2", 2)
        consume("resume")
    elif mode == "produce":
        produce(
            PHASE or "produce",
            int(os.environ.get("BATCH", "1")),
            os.environ.get("VALUES") == "json",
        )
    elif mode == "read":
        read_all()
    elif mode == "committed":
        group_committed()
    else:
        raise SystemExit(f"unknown mode {mode}")
    out("DONE")


if __name__ == "__main__":
    main()
