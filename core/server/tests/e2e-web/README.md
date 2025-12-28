# Apache Iggy Web UI Tests

## Prerequisites

Our tests rely on the "apache/iggy:local" image to be available, and so it should
be built and tagged accordingly. With [docker](https://docs.docker.com/engine/install/),
we can achieve this with (from the workspace root):

```console
docker build -t apache/iggy:local -f core/server/Dockerfile .
```

We are getting the app rendered in the Google Chrome browser and using its
web driver, so these binaries should be in the path:

- [google-chrome](https://www.google.com/chrome/)
- [chromedriver](https://googlechromelabs.github.io/chrome-for-testing/#stable)

## Running test suite

Make sure the web driver is running:

```sh
chromedriver --port=4444
```

Run the end-to-end test suite with:

```sh
E2E_TEST=true cargo t --release --test e2e-web
```

To run tests in headless mode, hit:

```sh
E2E_TEST=true E2E_TEST_HEADLESS=true cargo t --release --test e2e-web
```

Note, that if the web driver is listening on a different port (e.g. 4444 port is
taken, or you got multiple drivers running to test different engines), you will
need to provide `WEBDRIVER_PORT` to the otherwise the same command, i.e.:

```sh
WEBDRIVER_PORT=<port> E2E_TEST=true cargo t --release --test e2e-web
```
