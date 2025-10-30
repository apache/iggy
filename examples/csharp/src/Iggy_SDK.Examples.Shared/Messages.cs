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

using System.Text.Json;
using System.Text.Json.Serialization;

namespace Iggy_SDK.Examples.Shared;

public class Envelope
{
    public const string ORDER_CREATED_TYPE = "order_created";
    public const string ORDER_CONFIRMED_TYPE = "order_confirmed";
    public const string ORDER_REJECTED_TYPE = "order_rejected";

    public string MessageType { get; }
    public string Payload { get; }

    public Envelope(string messageType, ISerializableMessage payload)
    {
        MessageType = messageType;
        Payload = payload.ToJson();
    }

    [JsonConstructor]
    public Envelope(string messageType, string payload)
    {
        MessageType = messageType;
        Payload = payload;
    }

    public string ToJson()
    {
        return JsonSerializer.Serialize(this);
    }
}

public interface ISerializableMessage
{
    string MessageType { get; }
    string ToJson();
    string ToJsonEnvelope();
}

public record OrderCreated(
    ulong OrderId,
    string CurrencyPair,
    double Price,
    double Quantity,
    string Side,
    DateTimeOffset Timestamp) : ISerializableMessage
{
    public string MessageType => Envelope.ORDER_CREATED_TYPE;

    public string ToJson()
    {
        return JsonSerializer.Serialize(this);
    }

    public string ToJsonEnvelope()
    {
        return new Envelope(MessageType, this).ToJson();
    }
}

public record OrderConfirmed(ulong OrderId, double Price, DateTimeOffset Timestamp) : ISerializableMessage
{
    public string MessageType => Envelope.ORDER_CONFIRMED_TYPE;

    public string ToJson()
    {
        return JsonSerializer.Serialize(this);
    }

    public string ToJsonEnvelope()
    {
        return new Envelope(MessageType, this).ToJson();
    }
}

public record OrderRejected(ulong OrderId, DateTimeOffset Timestamp, string Reason) : ISerializableMessage
{
    public string MessageType => Envelope.ORDER_REJECTED_TYPE;

    public string ToJson()
    {
        return JsonSerializer.Serialize(this);
    }

    public string ToJsonEnvelope()
    {
        return new Envelope(MessageType, this).ToJson();
    }
}
