# Apache Iggy Web UI

This project hosts the web user interface for Apache Iggy. The web UI is built using SvelteKit.

![Web](../assets/web_ui.png)

## Overview

The Iggy Web UI provides a user-friendly panel for managing various aspects of the Iggy platform, including streams, topics, partitions, and more.

The [docker image](https://hub.docker.com/r/apache/iggy-web-ui) is available, and can be fetched via `docker pull apache/iggy-web-ui:edge`.

## Tooling

- Node.js: use a version supported by the current frontend toolchain, `^20.19.0 || ^22.13.0 || >=24`.
- Package manager: `npm`
- `pnpm` and `yarn` are not part of the supported workflow for this package. CI, Docker builds, and the committed lockfile use `npm`.

### Getting Started

1. **Run Iggy server first**

   ```sh
   docker pull apache/iggy:edge
   ```

   ```sh
   docker run --security-opt seccomp=unconfined -p 3000:3000 -p 8090:8090 \
     -e IGGY_ROOT_USERNAME=iggy -e IGGY_ROOT_PASSWORD=iggy \
     -e IGGY_HTTP_ADDRESS=0.0.0.0:3000 -e IGGY_TCP_ADDRESS=0.0.0.0:8090 \
     -e IGGY_NODE_ADVERTISED_ADDRESS=localhost apache/iggy:edge
   ```

2. **Clone the repository:**

   ```sh
   git clone https://github.com/apache/iggy.git
   ```

3. **Build the project:**

   ```sh
   cd iggy/web
   npm ci
   ```

4. **Run the project:**

   ```sh
   npm run dev
   ```

   **To expose port in local network run:**

   ```sh
   npm run dev -- --host --port 3333
   ```

   Set `PUBLIC_IGGY_API_URL` in `web/.env` to the HTTP address reachable by your browser, for example:

   ```sh
   PUBLIC_IGGY_API_URL=http://127.0.0.1:3000
   ```

   This applies to both container and source-built servers. Use the host-published HTTP port for a container.

## Roadmap

- [x] Authorization
- [x] Allow manual interval setting for auto-refresh
- [x] Introduce dark mode
- [x] Enable CRUD operations on Streams
- [x] Enable CRUD operations on Topics
- [ ] Messages management
- [ ] General UI improvements
- [ ] Permission management (in progress)
- [x] GitHub actions CI/CD
- [x] Publish to Dockerhub as separate Image

## Contributing

Feel free to contribute to the project. Your feedback and contributions are highly appreciated!
