// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// The conformance agent of the C# SDK. It reads JSON requests from stdin, calls the SDK and writes one JSON answer
// per request to stdout. It never judges a result. See conformance/README.md for the protocol.

using System.Reflection;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Iggy.ConformanceAgent;
using Apache.Iggy.IggyClient;

const int protocolVersion = 1;
string[] capabilities =
[
    "reader", "consumer", "resolve_operation", "abandon_operation", "raw_command", "raw_command_replica",
    "describe_options", "metadata_changes", "connection_info"
];

var output = new AnswerWriter(Console.OpenStandardOutput());
output.Write(new JsonObject
{
    ["hello"] = new JsonObject
    {
        ["sdk"] = "csharp",
        ["sdk_version"] = SdkVersion(),
        ["protocol"] = protocolVersion,
        ["transports"] = new JsonArray("tcp"),
        ["capabilities"] = JsonSerializer.SerializeToNode(capabilities)
    }
});

var agent = new Agent();
var running = new List<Task>();
using var input = new StreamReader(Console.OpenStandardInput(), new UTF8Encoding(false));
while (await input.ReadLineAsync() is { } line)
{
    if (string.IsNullOrWhiteSpace(line))
    {
        continue;
    }

    Request request;
    try
    {
        request = JsonSerializer.Deserialize<Request>(line, Request.Json)
                  ?? throw new JsonException("The request is null.");
    }
    catch (JsonException error)
    {
        Log.Warn($"Cannot read the request {line}: {error.Message}");
        continue;
    }

    // Each request runs on its own task, so a long call never delays a reset or a cancellation behind it.
    running.RemoveAll(task => task.IsCompleted);
    running.Add(Task.Run(async () => output.Write((await agent.HandleAsync(request)).Line(request.Id))));
}

await agent.ResetAsync();
await Task.WhenAll(running);

// The SDK keeps its own version text internal, so the agent reads the same assembly attribute.
static string SdkVersion()
{
    var version = typeof(IIggyClient).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()
        ?.InformationalVersion ?? "unknown";
    return version.Split('+')[0];
}
