package main

import (
	"math/rand"
	"time"

	sharedDemoContracts "github.com/iggy-rs/iggy-go-client/samples/shared"
)

type MessageGenerator struct {
	OrderCreatedId   int
	OrderConfirmedId int
	OrderRejectedId  int
}

func NewMessageGenerator() *MessageGenerator {
	return &MessageGenerator{}
}

func (gen *MessageGenerator) GenerateMessage() sharedDemoContracts.ISerializableMessage {
	switch rand.Intn(3) {
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
		CurrencyPair: currencyPairs[rand.Intn(len(currencyPairs))],
		Price:        float64(rand.Intn(352) + 69),
		Quantity:     float64(rand.Intn(352) + 69),
		Side:         sides[rand.Intn(len(sides))],
		Timestamp:    uint64(rand.Intn(69201) + 420),
	}
}

func (gen *MessageGenerator) GenerateOrderConfirmedMessage() *sharedDemoContracts.OrderConfirmed {
	gen.OrderConfirmedId++

	return &sharedDemoContracts.OrderConfirmed{
		Id:        gen.OrderConfirmedId,
		Price:     float64(rand.Intn(352) + 69),
		Timestamp: uint64(rand.Intn(69201) + 420),
	}
}

func (gen *MessageGenerator) GenerateOrderRejectedMessage() *sharedDemoContracts.OrderRejected {
	gen.OrderRejectedId++
	reasons := []string{"Cancelled by user", "Insufficient funds", "Other"}

	return &sharedDemoContracts.OrderRejected{
		Id:        gen.OrderRejectedId,
		Timestamp: uint64(rand.Intn(68999) + 421),
		Reason:    reasons[rand.Intn(len(reasons))],
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
