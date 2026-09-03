# csharp bdd test

Scenario are located at [/bdd/scenarios](../../../bdd/scenarios)

## env var

the bdd test suite has no defaults and fails when any of these is missing:

- `IGGY_TCP_ADDRESS="host:port"` - server address
- `IGGY_ROOT_USERNAME` / `IGGY_ROOT_PASSWORD` - root credentials
- `IGGY_TCP_ADDRESS_LEADER` / `IGGY_TCP_ADDRESS_FOLLOWER` - cluster addresses, leader redirection scenarios only

## Run (recommended)

from the repository root run

```bash
./scripts/run-bdd-tests.sh csharp
```

the script starts the server, brings up the leader and follower for the cluster
scenarios, and sets every variable above, so none of them have to be exported by
hand. see [/bdd/README.md](../../../bdd/README.md) for the sdk and feature
matrix.

## Run locally

for iterating against a server you started yourself. note: bdd test expect an
iggy-server started with the same root credentials

from [/foreign/csharp/Iggy_SDK.Tests.BDD](.) run

```bash
IGGY_TCP_ADDRESS=127.0.0.1:8090 IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
  dotnet test
```

## Troubleshooting

Sometimes tests might be run twice or have errors during build.
It's because link to .feature files and problem with generated code.
To fix it, run `dotnet clean`
