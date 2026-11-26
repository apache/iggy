# Apache Iggy Roadmap

This roadmap describes what the project is working on, for people planning
against Iggy's direction rather than following the issue tracker day to day.

Every item below has an open tracking issue, linked from its entry. Work
without one is listed under "Under discussion". There are no delivery dates.

## Roadmap

### Clustering with Viewstamped Replication

Tracking issue: [#1914](https://github.com/apache/iggy/issues/1914)

In progress. The replication work is merged to the default branch, and the
SDK migrations to the VSR wire protocol are merged across Go, Java, PHP, C++,
Node, Python and C#.

### Kafka gateway

Tracking issue: [#3560](https://github.com/apache/iggy/issues/3560),
an umbrella issue with 18 sub-issues.

Early. A foundation stub is in the tree at `gateways/kafka`; no API serves
real data yet. The bridge core, wire APIs, consumer groups, admin APIs, SASL
authentication and a migration guide are all open.

### Connector ecosystem

Tracking issue: [#2753](https://github.com/apache/iggy/issues/2753)

In progress. The native Rust runtime carries 15 sinks and 4 sources on the
default branch:

- Sinks: ClickHouse, Delta, Doris, Elasticsearch, HTTP, Iceberg, InfluxDB,
  Meilisearch, MongoDB, PostgreSQL, Quickwit, Redshift, S3, Stdout, SurrealDB
- Sources: Elasticsearch, InfluxDB, PostgreSQL, Random

Not all of them are in a stable release yet.

Named open work includes a MongoDB source to complete that family
([#2739](https://github.com/apache/iggy/issues/2739)), JDBC and MySQL sink
and source ([#2500](https://github.com/apache/iggy/issues/2500)), and the
Avro ([#1846](https://github.com/apache/iggy/issues/1846)) and BSON
([#1847](https://github.com/apache/iggy/issues/1847)) codecs. Codecs and
transforms live in the connector SDK rather than as connectors of their own,
and the Flink integration is a separate Java external processor, not part of
the Rust connector runtime.

## Under discussion

Raised in release notes and blog posts, with no tracking issue yet:

- Server core optimisations: kTLS, DirectIO, continued NUMA tuning
- SDK evolution toward a sans-IO architecture
- C++ SDK feature parity
- Web UI enhancements
- Agentic AI and expanded A2A protocol support
