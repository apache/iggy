# csharp bdd test

Scenario are located at [/bdd/scenarios](../../../bdd/scenarios)

## env var

use env var `IGGY_TCP_ADDRESS="host:port"` to set expected server address for bdd test suite.

## Run via docker

see [/bdd/README.md](../../../bdd/README.md)

## Run locally

note: bdd test expect an iggy-server at tcp://127.0.0.1:8090

from [/foreign/csharp/Iggy_SDK.Tests.BDD](.) run

```bash
dotnet test
```

## Troubleshooting

Sometimes tests might be run twice or have errors during build.
It's because link to .feature files and problem with generated code.
To fix it, run `dotnet clean`
