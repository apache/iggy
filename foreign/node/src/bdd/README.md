
# Node.js bdd test

Node.js bdd test are run via cucumber-js.
scenario are located at [/bdd/scenarios](../../../../bdd/scenarios)

## env var

the bdd test suite has no defaults and fails when any of these is missing:

- `IGGY_TCP_ADDRESS="host:port"` - server address
- `IGGY_ROOT_USERNAME` / `IGGY_ROOT_PASSWORD` - root credentials

## Run (recommended)

from the repository root run

```bash
./scripts/run-bdd-tests.sh node
```

the script starts the server and sets every variable above, so none of them have
to be exported by hand. see [/bdd/README.md](../../../../bdd/README.md) for the
sdk and feature matrix.

## Run locally

for iterating against a server you started yourself. note: bdd test expect an
iggy-server started with the same root credentials

from [/foreign/node](../../) run

```bash
npm ci # if not already done
IGGY_TCP_ADDRESS=127.0.0.1:8090 IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
  npm run test:bdd
```
