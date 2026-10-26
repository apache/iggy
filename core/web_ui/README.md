# Apache Iggy Web UI

A [Dioxus](https://dioxuslabs.com/) single-page app for browsing an Iggy
server over its HTTP API. It compiles to `wasm32-unknown-unknown` and is
built with the Dioxus CLI, `dx`.

## Prerequisites

```bash
rustup target add wasm32-unknown-unknown
cargo install dioxus-cli --version 0.7.10 --locked
```

Keep the `dx` version equal to the `dioxus` version in `Cargo.lock`.

## Release bundle

```bash
./scripts/build-web-ui.sh
```

The bundle lands in `core/web_ui/dist`, with every asset path under `/ui`.
The script clears dx's staging directory first, because dx copies every
previously hashed asset into the bundle.

## Development

Start a server, then serve the UI with hot reload from this directory:

```bash
cargo run --bin iggy-server -- --with-default-root-credentials
cd core/web_ui
IGGY_WEB_UI_API_URL=http://localhost:3000 dx serve
```

Open <http://localhost:8080/ui/> and sign in as `iggy` / `iggy`.

`IGGY_WEB_UI_API_URL` is read at build time and prefixes every API request.
Left unset, requests go to the origin that served the UI, which is what an
embedded build wants. The server's default CORS settings allow any origin, so
the dev server on another port can reach it.

## Tests

```bash
cargo test -p iggy-web-ui
```

The tests run on the host. Besides the formatting helpers, they serialize
`iggy_common` types and decode them into the UI's mirrors in `src/models.rs`,
so a renamed server field fails here instead of in the browser.
