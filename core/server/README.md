# Apache Iggy Server

This is the core server component of Apache Iggy. You can run it directly with `cargo run --bin iggy-server --release` or use the Docker image `apache/iggy:latest` (the `edge` tag is for the latest development version).

The configuration file is located at [core/configs/server.toml](https://github.com/apache/iggy/blob/master/core/configs/server.toml). You can customize the server settings by modifying this file or by using environment variables e.g. `IGGY_TCP_ADDRESS=0.0.0.0:8090`.

![Server](../../assets/server.png)

![Architecture](../../assets/iggy_architecture.png)
