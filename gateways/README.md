# Apache Iggy Gateways

Protocol gateways that let existing clients talk to Iggy without changing the core server wire surface.

| Gateway | Issue | Description |
| --------- | ------- | ------------- |
| [kafka](kafka/) | [#3421](https://github.com/apache/iggy/issues/3421) | Kafka wire protocol TCP listener (port 9093) |

Each gateway is a separate workspace crate under `gateways/<name>/`.
