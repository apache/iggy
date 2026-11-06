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

using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Iggy.Consumers;
using Apache.Iggy.Exceptions;

namespace Apache.Iggy.ConformanceAgent;

/// <summary>One request line of the orchestrator.</summary>
internal sealed record Request(
    ulong Id,
    string Op,
    string? Kind = null,
    string? Entity = null,
    string? Method = null,
    JsonElement Args = default,
    string? SaveAs = null,
    string? Task = null)
{
    internal static readonly JsonSerializerOptions Json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };
}

/// <summary>Exactly one of a result, an SDK error, an unsupported call and a failure of the agent itself.</summary>
internal abstract record Reply
{
    // The SDK keeps these codes internal. The values are those of IggyError in core/common.
    private const uint OperationUncertain = 14006;
    private const uint OperationNotRetained = 14008;

    internal static readonly Reply Null = new Result(null);

    internal JsonObject Line(ulong id)
    {
        var line = new JsonObject { ["id"] = id };
        switch (this)
        {
            case Result result:
                line["result"] = result.Value;
                break;
            case Error error:
                var answer = new JsonObject { ["code"] = error.Code };
                if (error.OperationId is { } operationId)
                {
                    answer["operation_id"] = operationId;
                }

                if (error.PartitionId is { } partitionId)
                {
                    answer["partition_id"] = partitionId;
                }

                answer["message"] = error.Message;
                line["error"] = answer;
                break;
            case Unsupported unsupported:
                line["unsupported"] = unsupported.Reason;
                break;
            case Failure failure:
                line["failure"] = failure.Reason;
                break;
        }

        return line;
    }

    /// <summary>
    ///     The answer for an exception of the SDK. An SDK exception that carries no Iggy code is code 0, and any other
    ///     exception is a failure of the agent. A feature that the SDK lacks is unsupported.
    /// </summary>
    internal static Reply FromException(Exception exception)
    {
        uint? readPartition = null;
        if (exception is ReadException { InnerException: { } cause } read)
        {
            readPartition = read.PartitionId;
            exception = cause;
        }

        var message = exception.Message;
        return exception switch
        {
            VsrPartitionLaneBlockedException blocked =>
                new Error((uint)blocked.StatusCode, blocked.OperationId, blocked.PartitionId, message),
            VsrLaneBlockedException blocked =>
                new Error((uint)blocked.StatusCode, blocked.OperationId, readPartition, message),
            VsrFetchProgressUncertainException uncertain =>
                new Error((uint)uncertain.StatusCode, uncertain.OperationId, readPartition, message),
            VsrRequestOutcomeUnknownException uncertain =>
                new Error(OperationUncertain, uncertain.OperationId, readPartition, message),
            VsrOperationNotRetainedException notRetained =>
                new Error(OperationNotRetained, notRetained.OperationId, readPartition, message),
            IggyInvalidStatusCodeException status => new Error((uint)status.StatusCode, null, readPartition, message),
            FeatureUnavailableException => new Unsupported(message),
            // Code 0 belongs to no IggyError, so the test fails and the skip list can name it.
            _ when exception.GetType().Namespace?.StartsWith("Apache.Iggy", StringComparison.Ordinal) == true =>
                new Error(0, null, readPartition, $"{exception.GetType().Name}: {message}"),
            _ => new Failure($"an error without an Iggy code: {exception.GetType().Name}: {message}")
        };
    }

    internal sealed record Result(JsonNode? Value) : Reply;

    internal sealed record Error(uint Code, ulong? OperationId, uint? PartitionId, string Message) : Reply;

    internal sealed record Unsupported(string Reason) : Reply;

    internal sealed record Failure(string Reason) : Reply;
}

/// <summary>Writes whole answer lines to stdout, one at a time.</summary>
internal sealed class AnswerWriter(Stream stdout)
{
    private readonly Lock _gate = new();
    private readonly StreamWriter _writer = new(stdout, new UTF8Encoding(false)) { AutoFlush = true };

    internal void Write(JsonNode line)
    {
        var text = line.ToJsonString() + "\n";
        lock (_gate)
        {
            _writer.Write(text);
        }
    }
}

/// <summary>The log of the agent, on stderr, because stdout carries the protocol.</summary>
internal static class Log
{
    internal static void Warn(string message)
    {
        Console.Error.WriteLine($"warn: {message}");
    }
}
