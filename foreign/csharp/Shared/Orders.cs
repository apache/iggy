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

namespace Apache.Iggy.Shared;

public class OrderCreated : ISerializableMessage
{
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    public required int Id { get; init; }
    public required string CurrencyPair { get; init; }
    public required double Price { get; init; }
    public required double Quantity { get; init; }
    public required string Side { get; init; }
    public required ulong Timestamp { get; init; }

    public OrderCreated()
    {
        _jsonSerializerOptions = new JsonSerializerOptions();
        _jsonSerializerOptions.WriteIndented = true;
    }

    public string ToJson()
    {
        var envelope = new Envelope();
        var env = envelope.New("order_created", this);
        return JsonSerializer.Serialize(env, _jsonSerializerOptions);
    }

    public byte[] ToBytes()
    {
        var envelope = new Envelope();
        var env = envelope.New("order_created", this);
        var json = JsonSerializer.Serialize(env, _jsonSerializerOptions);
        return Encoding.UTF8.GetBytes(json);
    }

    public Envelope ToEnvelope()
    {
        var envelope = new Envelope();
        var env = envelope.New("order_created", this);
        return env;
    }

    private string ToJsonPrint()
    {
        return JsonSerializer.Serialize(this, _jsonSerializerOptions);
    }

    public override string ToString()
    {
        return $"OrderCreated {ToJsonPrint()}";
    }
}

public class OrderConfirmed : ISerializableMessage
{
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    public required int Id { get; init; }
    public required double Price { get; init; }
    public required ulong Timestamp { get; init; }

    public OrderConfirmed()
    {
        _jsonSerializerOptions = new JsonSerializerOptions();
        _jsonSerializerOptions.WriteIndented = true;
    }

    public string ToJson()
    {
        var envelope = new Envelope();
        var env = envelope.New("order_confirmed", this);
        return JsonSerializer.Serialize(env, _jsonSerializerOptions);
    }

    public byte[] ToBytes()
    {
        var envelope = new Envelope();
        var env = envelope.New("order_confirmed", this);
        var json = JsonSerializer.Serialize(env, _jsonSerializerOptions);
        return Encoding.UTF8.GetBytes(json);
    }

    public Envelope ToEnvelope()
    {
        var envelope = new Envelope();
        var env = envelope.New("order_confirmed", this);
        return env;
    }

    private string ToJsonPrint()
    {
        return JsonSerializer.Serialize(this, _jsonSerializerOptions);
    }

    public override string ToString()
    {
        return $"OrderConfirmed {ToJsonPrint()}";
    }
}

public class OrderRejected : ISerializableMessage
{
    private readonly JsonSerializerOptions _jsonSerializerOptions;

    public required int Id { get; init; }
    public required ulong Timestamp { get; init; }
    public required string Reason { get; init; }

    public OrderRejected()
    {
        _jsonSerializerOptions = new JsonSerializerOptions();
        _jsonSerializerOptions.WriteIndented = true;
    }

    public string ToJson()
    {
        var envelope = new Envelope();
        var env = envelope.New("order_rejected", this);
        return JsonSerializer.Serialize(env, _jsonSerializerOptions);
    }

    public Envelope ToEnvelope()
    {
        var envelope = new Envelope();
        var env = envelope.New("order_rejected", this);
        return env;
    }

    public byte[] ToBytes()
    {
        var envelope = new Envelope();
        var env = envelope.New("order_rejected", this);
        var json = JsonSerializer.Serialize(env, _jsonSerializerOptions);
        return Encoding.UTF8.GetBytes(json);
    }

    private string ToJsonPrint()
    {
        return JsonSerializer.Serialize(this, _jsonSerializerOptions);
    }

    public override string ToString()
    {
        return $"OrderRejected {ToJsonPrint()}";
    }
}