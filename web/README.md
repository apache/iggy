# Apache Iggy Web UI

This project hosts the web user interface for Apache Iggy. The web UI is built using SvelteKit.

![Web](../assets/web_ui.png)

## Overview

The Iggy Web UI provides a user-friendly panel for managing various aspects of the Iggy platform, including streams, topics, partitions, and more.

The [docker image](https://hub.docker.com/r/apache/iggy-web-ui) is available, and can be fetched via `docker pull apache/iggy-web-ui`.

### Getting Started

1. **Run Iggy server first**

   ```sh
   docker pull apache/iggy:latest
   ```

   ```sh
   docker run -p 3000:3000 -p 8090:8090 apache/iggy:latest
   ```

2. **Clone the repository:**

   ```sh
   git clone https://github.com/apache/iggy.git
   ```

3. **Build the project:**

   ```sh
   cd web
   npm install
   ```

4. **Run the project:**

   ```sh
   npm run dev
   ```

   **To expose port in local network run:**

   ```sh
   npm run dev -- --host --port 3333
   ```

   **If Iggy server was run using cargo directly we need to change PUBLIC_IGGY_API_URL env in web ui root folder to:**

   ```sh
   PUBLIC_IGGY_API_URL=http://127.0.0.1:3000
   ```

   **instead of**

   ```sh
   PUBLIC_IGGY_API_URL=http://localhost:3000
   ```

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
