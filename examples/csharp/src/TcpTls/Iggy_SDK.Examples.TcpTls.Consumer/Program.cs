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

// TCP/TLS Consumer Example
//
// Demonstrates consuming messages over a TLS-encrypted TCP connection.
//
// Prerequisites:
//   Start the Iggy server with TLS enabled:
//     IGGY_TCP_TLS_ENABLED=true
//     IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem
//     IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem

using System.Text;
using Apache.Iggy;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging;

const string streamName = "tls-stream";
const string topicName = "tls-topic";
const uint partitionId = 0;
const uint batchesLimit = 5;

var loggerFactory = LoggerFactory.Create(b => { b.AddConsole(); });
var logger = loggerFactory.CreateLogger<Program>();

// Configure the client with TLS.
// TlsSettings.Enabled    = true activates TLS on the TCP transport
// TlsSettings.Hostname   = the expected server hostname for certificate verification
// TlsSettings.CertificatePath = path to CA or server certificate PEM file
var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
{
    BaseAddress = "127.0.0.1:8090",
    Protocol = Protocol.Tcp,
    LoggerFactory = loggerFactory,
    TlsSettings = new TlsSettings
    {
        Enabled = true,
        Hostname = "localhost",
        CertificatePath = "core/certs/iggy_ca_cert.pem"
    }
});

await client.ConnectAsync();
await client.LoginUserAsync("iggy", "iggy");
logger.LogInformation("Connected and logged in over TLS.");

await ConsumeMessages();

async Task ConsumeMessages()
{
    var interval = TimeSpan.FromMilliseconds(500);
    logger.LogInformation(
        "Messages will be consumed from stream: {StreamName}, topic: {TopicName}, partition: {PartitionId} with interval {Interval}.",
        streamName, topicName, partitionId, interval);

    var offset = 0ul;
    uint messagesPerBatch = 10;
    var consumedBatches = 0;
    var consumer = Consumer.New(1);
    while (true)
    {
        if (consumedBatches == batchesLimit)
        {
            logger.LogInformation("Consumed {ConsumedBatches} batches of messages, exiting.", consumedBatches);
            return;
        }

        var polledMessages = await client.PollMessagesAsync(
            Identifier.String(streamName),
            Identifier.String(topicName),
            partitionId,
            consumer,
            PollingStrategy.Offset(offset),
            messagesPerBatch,
            false);

        if (!polledMessages.Messages.Any())
        {
            logger.LogInformation("No messages found.");
            await Task.Delay(interval);
            continue;
        }

        offset += (ulong)polledMessages.Messages.Count;
        foreach (var message in polledMessages.Messages)
        {
            var payload = Encoding.UTF8.GetString(message.Payload);
            logger.LogInformation("Handling message at offset: {Offset}, payload: {Payload}...",
                message.Header.Offset, payload);
        }
        consumedBatches++;
        await Task.Delay(interval);
    }
}
