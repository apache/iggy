# Iggy Node.js SDK Examples

This directory contains examples demonstrating how to use the [Iggy Node.js SDK](https://github.com/iggy-rs/iggy/tree/master/foreign/node).

## Prerequisites

- [Node.js](https://nodejs.org/) (v18 or later)
- An running Iggy server instance. You can run it with `docker-compose up` from the root of the `iggy` repository.

## Installation

1.  Navigate to this directory (`examples/nodejs`).
2.  Install the dependencies:

    ```sh
    npm install
    ```

## Running the Examples

### JavaScript

To run any of the JavaScript examples, use the `node` command followed by the path to the example's `main.js` file.

For example, to run the `getting-started` example:

```bash
node getting-started/main.js
```

### TypeScript

To run the TypeScript examples, first ensure you have installed the development dependencies (`npm install`). Then, use the `npm run start:ts` command followed by the path to the example's `main.ts` file.

For example, to run the `getting-started` TypeScript example:

```sh
npm run start:ts -- getting-started/main.ts
```

## Examples

- **getting-started**: A basic example that shows how to connect to the server, create a stream and topic, send messages, and poll them.
