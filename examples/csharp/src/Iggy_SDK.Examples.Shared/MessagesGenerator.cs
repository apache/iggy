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

using System.Diagnostics;

namespace Iggy_SDK.Examples.Shared;

public class MessagesGenerator
{
    private ulong _orderId;

    private static string[] CurrencyPairs => ["EUR/USD", "EUR/GBP", "USD/GBP", "EUR/PLN", "USD/PLN"];

    public ISerializableMessage Generate()
    {
        return (Random.Shared.Next() % 3) switch
        {
            0 => GenerateOrderCreated(),
            1 => GenerateOrderConfirmed(),
            2 => GenerateOrderRejected(),
            _ => throw new UnreachableException()
        };
    }

    private OrderCreated GenerateOrderCreated()
    {
        _orderId++;
        return new OrderCreated(
            _orderId,
            CurrencyPairs[Random.Shared.Next(0, CurrencyPairs.Length)],
            Random.Shared.NextDouble() * 990.0 + 10.0,
            Random.Shared.NextDouble() * 0.9 + 0.1,
            Random.Shared.Next() % 2 == 1 ? "buy" : "sell",
            DateTimeOffset.UtcNow
        );
    }

    private OrderConfirmed GenerateOrderConfirmed()
    {
        return new OrderConfirmed(
            _orderId,
            Random.Shared.NextDouble() * 990.0 + 10.0,
            DateTimeOffset.UtcNow
        );
    }

    private OrderRejected GenerateOrderRejected()
    {
        return new OrderRejected(
            _orderId,
            DateTimeOffset.UtcNow,
            Random.Shared.Next() % 2 == 1 ? "cancelled_by_user" : "other"
        );
    }
}
