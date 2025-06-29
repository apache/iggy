
# Node.js bdd test

Node.js bdd test are run via cucumber-js.
scenario are located at [/bdd/scenarios](../../../../bdd/scenarios)

## env var

use env var `IGGY_TCP_ADDRESS="host:port"` to set expected server address for bdd test suite.

## Run via docker

see [/bdd/README.md](../../../../bdd/README.md)

## Run locally

note: bdd test expect an iggy-server at tcp://127.0.0.1:8090

from [/foreign/node](../../) run

```bash
npm ci # if not already done
npm run test:bdd
```
