
# Node.js bdd test

Node.js bdd test are run via cucumber-js.
scenario are located at [/bdd/scenarios](../../../../bdd/scenarios)

## env var

the bdd test suite has no defaults and fails when any of these is missing:

- `IGGY_TCP_ADDRESS="host:port"` - server address
- `IGGY_ROOT_USERNAME` / `IGGY_ROOT_PASSWORD` - root credentials
- `IGGY_TCP_ADDRESS_NO_QUORUM="host:port"` - cluster node that cannot reach quorum, read only by the session contract scenarios

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

The stream options feature also needs the server to run with encryption enabled and a config key (`IGGY_ENCRYPTION_ENABLED=true`, `IGGY_ENCRYPTION_KEY`), as `scripts/run-bdd-tests.sh` starts it.

The metadata changes feature needs the same key and `IGGY_METADATA_CHANGE_LOG_RESOURCE_CAPACITY=64`, so its expiry scenario can push a held cursor out of the change log.

from [/foreign/node](../../) run

```bash
npm ci # if not already done
IGGY_TCP_ADDRESS=127.0.0.1:8090 IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
  npm run test:bdd
```
