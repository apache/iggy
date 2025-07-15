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

package main

import (
	"math/rand"
	"time"

	sharedDemoContracts "github.com/apache/iggy/foreign/go/samples/shared"
)

type MessageGenerator struct {
	OrderCreatedId   int
	OrderConfirmedId int
	OrderRejectedId  int
	rng              *rand.Rand
}

func NewMessageGenerator() *MessageGenerator {
	return &MessageGenerator{
		rng: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (gen *MessageGenerator) GenerateMessage() sharedDemoContracts.ISerializableMessage {
	switch gen.rng.Intn(3) {
	case 0:
		return gen.GenerateOrderRejectedMessage()
	case 1:
		return gen.GenerateOrderConfirmedMessage()
	default:
		return gen.GenerateOrderCreatedMessage()
	}
}

func (gen *MessageGenerator) GenerateOrderCreatedMessage() *sharedDemoContracts.OrderCreated {
	gen.OrderCreatedId++
	currencyPairs := []string{"BTC/USDT", "ETH/USDT", "LTC/USDT"}
	sides := []string{"Buy", "Sell"}

	return &sharedDemoContracts.OrderCreated{
		Id:           gen.OrderCreatedId,
		CurrencyPair: currencyPairs[gen.rng.Intn(len(currencyPairs))],
		Price:        float64(gen.rng.Intn(352) + 69),
		Quantity:     float64(gen.rng.Intn(352) + 69),
		Side:         sides[gen.rng.Intn(len(sides))],
		Timestamp:    uint64(gen.rng.Intn(69201) + 420),
	}
}

func (gen *MessageGenerator) GenerateOrderConfirmedMessage() *sharedDemoContracts.OrderConfirmed {
	gen.OrderConfirmedId++

	return &sharedDemoContracts.OrderConfirmed{
		Id:        gen.OrderConfirmedId,
		Price:     float64(gen.rng.Intn(352) + 69),
		Timestamp: uint64(gen.rng.Intn(69201) + 420),
	}
}

func (gen *MessageGenerator) GenerateOrderRejectedMessage() *sharedDemoContracts.OrderRejected {
	gen.OrderRejectedId++
	reasons := []string{"Cancelled by user", "Insufficient funds", "Other"}

	return &sharedDemoContracts.OrderRejected{
		Id:        gen.OrderRejectedId,
		Timestamp: uint64(gen.rng.Intn(68999) + 421),
		Reason:    reasons[gen.rng.Intn(len(reasons))],
	}
}
