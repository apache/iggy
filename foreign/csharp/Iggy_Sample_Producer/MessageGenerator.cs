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

using Apache.Iggy.Shared;

namespace Apache.Iggy.Producer;

public static class MessageGenerator
{
    private static int OrderCreatedId;
    private static int OrderConfirmedId;
    private static int OrderRejectedId;

    public static ISerializableMessage GenerateMessage()
    {
        return Random.Shared.Next(1, 4) switch
        {
            1 => GenerateOrderRejectedMessage(),
            2 => GenerateOrderConfirmedMessage(),
            3 => GenerateOrderCreatedMessage(),
            _ => GenerateOrderCreatedMessage()
        };
    }

    private static ISerializableMessage GenerateOrderCreatedMessage()
    {
        return new OrderCreated
        {
            Id = OrderCreatedId++,
            CurrencyPair = Random.Shared.Next(1, 3) switch
            {
                1 => "BTC/USDT",
                2 => "ETH/USDT",
                _ => "LTC/USDT"
            },
            Price = Random.Shared.Next(69, 420),
            Quantity = Random.Shared.Next(69, 420),
            Side = Random.Shared.Next(1, 2) switch
            {
                1 => "Buy",
                _ => "Sell"
            },
            Timestamp = (ulong)Random.Shared.Next(420, 69420)
        };
    }

    private static ISerializableMessage GenerateOrderConfirmedMessage()
    {
        return new OrderConfirmed
        {
            Id = OrderConfirmedId++,
            Price = Random.Shared.Next(69, 420),
            Timestamp = (ulong)Random.Shared.Next(420, 69420)
        };
    }

    private static ISerializableMessage GenerateOrderRejectedMessage()
    {
        return new OrderRejected
        {
            Id = OrderRejectedId++,
            Timestamp = (ulong)Random.Shared.Next(421, 69420),
            Reason = Random.Shared.Next(1, 3) switch
            {
                1 => "Cancelled by user",
                2 => "Insufficient funds",
                _ => "Other"
            }
        };
    }
}