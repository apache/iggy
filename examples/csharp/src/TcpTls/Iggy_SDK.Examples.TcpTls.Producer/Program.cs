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

// TCP/TLS Producer Example
//
// Demonstrates producing messages over a TLS-encrypted TCP connection.
//
// Prerequisites:
//   Start the Iggy server with TLS enabled:
//     IGGY_TCP_TLS_ENABLED=true
//     IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem
//     IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem

using System.Text;
using Apache.Iggy;
using Apache.Iggy.Configuration;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Factory;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

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

await InitSystem();
await ProduceMessages();

async Task InitSystem()
{
    try
    {
        await client.CreateStreamAsync(streamName);
        logger.LogInformation("Stream was created.");
    }
    catch (Exception ex) when (ex is InvalidResponseException or IggyInvalidStatusCodeException)
    {
        logger.LogWarning("Stream already exists and will not be created again.");
    }

    try
    {
        await client.CreateTopicAsync(
            Identifier.String(streamName),
            topicName,
            1,
            CompressionAlgorithm.None
        );
        logger.LogInformation("Topic was created.");
    }
    catch (Exception ex) when (ex is InvalidResponseException or IggyInvalidStatusCodeException)
    {
        logger.LogWarning("Topic already exists and will not be created again.");
    }
}

async Task ProduceMessages()
{
    var interval = TimeSpan.FromMilliseconds(500);
    logger.LogInformation(
        "Messages will be sent to stream: {StreamName}, topic: {TopicName}, partition: {PartitionId} with interval {Interval}.",
        streamName, topicName, partitionId, interval);

    var currentId = 0;
    var messagesPerBatch = 10;
    var sentBatches = 0;
    var partitioning = Partitioning.PartitionId((int)partitionId);
    while (true)
    {
        if (sentBatches == batchesLimit)
        {
            logger.LogInformation("Sent {SentBatches} batches of messages, exiting.", sentBatches);
            return;
        }

        var payloads = Enumerable
            .Range(currentId, messagesPerBatch)
            .Aggregate(new List<string>(), (list, next) =>
            {
                list.Add($"message-{next}");
                return list;
            });

        var messages = payloads
            .Select(payload => new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(payload)))
            .ToList();

        await client.SendMessagesAsync(
            Identifier.String(streamName),
            Identifier.String(topicName),
            partitioning,
            messages);

        currentId += messagesPerBatch;
        sentBatches++;
        logger.LogInformation("Sent messages: {Messages}.", payloads);

        await Task.Delay(interval);
    }
}
