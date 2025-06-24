# Apache Iggy (Incubating) Web UI

This proejcts hosts the web user interface for Apache Iggy. The web UI is built using SvelteKit.

![iggy](static/signIn.png)
![iggy](static/stats.png)
![iggy](static/permissions.png)
![iggy](static/streams.png)
![iggy](static/streamsLight.png)

## Overview

The Iggy Web UI provides a user-friendly panel for managing various aspects of the Iggy platform, including streams, topics, partitions, and more.

### Getting Started

1. **Run Iggy server first**

   ```sh
   docker pull apache/iggy:latest
   ```

   ```sh
   docker run -p 3000:3000 -p 8090:8090 apache/iggy:latest
   ```

1. **Clone the repository:**

   ```sh
   git clone https://github.com/apache/iggy.git
   ```

1. **Build the project:**

   ```sh
   cd iggy-web-ui
   npm install
   ```

1. **Run the project:**

   ```sh
   npm run dev
   ```

   **To expose port in local network run:**

   ```sh
   npm run dev -- --host --port 3333
   ```

   **If Iggy server was run using cargo directly we need to change PUBLIC_IGGY_API_URL env in web ui root folder to:**

   ```sh
   PUBLIC_IGGY_API_URL=http://0.0.0.0:3000
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
- [ ] GitHub actions CI/CD
- [ ] Publish to Dockerhub as separate Image

## Contributing

Feel free to contribute to the project. Your feedback and contributions are highly appreciated!
