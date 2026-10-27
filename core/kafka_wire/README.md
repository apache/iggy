# Kafka wire (`iggy_kafka_wire`)

The runtime-free part of Apache Iggy's Kafka support: everything a host of the Kafka protocol
needs that touches neither a socket nor an async runtime. The separate gateway
(`gateways/kafka`) is the first host.

What lives here:

- `api`: API key and error code constants, and the version-range type.
- `bounds_guard`: the pre-decode walk that rejects a frame whose declared lengths exceed its
  bytes, before `kafka_protocol` can allocate from them.
- `codec`: encode and decode helpers over `kafka_protocol`, with the trailing-bytes check.
- `header`: request header version detection.
- `records`: Kafka record batch to Iggy message conversion, with bounded decompression.
- `sasl` and `acl`: response encoders for the SASL and `DescribeAcls` APIs.
- `dedup` and `wire_len`: small helpers for name lists and compact length prefixes.

Policy stays with the host: which API versions it accepts, how it answers, and when it closes a
connection.
