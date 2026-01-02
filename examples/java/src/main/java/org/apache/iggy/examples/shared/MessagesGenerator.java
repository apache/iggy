/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.examples.shared;

import org.apache.iggy.examples.shared.Messages.OrderConfirmed;
import org.apache.iggy.examples.shared.Messages.OrderCreated;
import org.apache.iggy.examples.shared.Messages.OrderRejected;
import org.apache.iggy.examples.shared.Messages.SerializableMessage;

import java.time.Instant;
import java.util.random.RandomGenerator;
import java.util.random.RandomGeneratorFactory;

public final class MessagesGenerator {

    private static final String[] CURRENCY_PAIRS = {"EUR/USD", "EUR/GBP", "USD/GBP", "EUR/PLN", "USD/PLN"};
    private final RandomGenerator rng = RandomGeneratorFactory.getDefault().create();
    private long orderId = 0L;

    public MessagesGenerator() {}

    public SerializableMessage generate() {
        int choice = rng.nextInt(3);
        return switch (choice) {
            case 0 -> generateOrderCreated();
            case 1 -> generateOrderConfirmed();
            case 2 -> generateOrderRejected();
            default -> throw new IllegalStateException("Unexpected message type");
        };
    }

    public SerializableMessage generateOrderCreated() {
        orderId += 1;
        String currencyPair = CURRENCY_PAIRS[rng.nextInt(CURRENCY_PAIRS.length)];
        double price = rng.nextDouble(10.0, 1000.0);
        double quantity = rng.nextDouble(0.1, 1.0);
        String side = rng.nextInt(2) == 0 ? "buy" : "sell";

        return new OrderCreated(orderId, currencyPair, price, quantity, side, Instant.now());
    }

    private SerializableMessage generateOrderConfirmed() {
        orderId += 1;
        double price = rng.nextDouble(10.0, 1000.0);
        return new OrderConfirmed(orderId, price, Instant.now());
    }

    private SerializableMessage generateOrderRejected() {
        orderId += 1;
        String reason = rng.nextInt(2) == 0 ? "cancelled_by_user" : "other";
        return new OrderRejected(orderId, Instant.now(), reason);
    }
}
