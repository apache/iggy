package sharedDemoContracts

import "encoding/json"

type ISerializableMessage interface {
	ToJson() string
	ToBytes() []byte
}

type Envelope struct {
	MessageType string `json:"message_type"`
	Payload     string `json:"payload"`
}

func (env *Envelope) New(messageType string, payload ISerializableMessage) *Envelope {
	jsonPayload, _ := json.Marshal(payload)
	return &Envelope{
		MessageType: messageType,
		Payload:     string(jsonPayload),
	}
}

type OrderCreated struct {
	Id           int     `json:"id"`
	CurrencyPair string  `json:"currency_pair"`
	Price        float64 `json:"price"`
	Quantity     float64 `json:"quantity"`
	Side         string  `json:"side"`
	Timestamp    uint64  `json:"timestamp"`
}

func (order *OrderCreated) ToJson() string {
	envelope := Envelope{}
	env := envelope.New("order_created", order)
	jsonPayload, _ := json.Marshal(env)
	return string(jsonPayload)
}

func (order *OrderCreated) ToBytes() []byte {
	envelope := Envelope{}
	env := envelope.New("order_created", order)
	jsonPayload, _ := json.Marshal(env)
	return jsonPayload
}

type OrderConfirmed struct {
	Id        int     `json:"id"`
	Price     float64 `json:"price"`
	Timestamp uint64  `json:"timestamp"`
}

func (order *OrderConfirmed) ToJson() string {
	envelope := Envelope{}
	env := envelope.New("order_confirmed", order)
	jsonPayload, _ := json.Marshal(env)
	return string(jsonPayload)
}

func (order *OrderConfirmed) ToBytes() []byte {
	envelope := Envelope{}
	env := envelope.New("order_confirmed", order)
	jsonPayload, _ := json.Marshal(env)
	return jsonPayload
}

type OrderRejected struct {
	Id        int    `json:"id"`
	Timestamp uint64 `json:"timestamp"`
	Reason    string `json:"reason"`
}

func (order *OrderRejected) ToJson() string {
	envelope := Envelope{}
	env := envelope.New("order_rejected", order)
	jsonPayload, _ := json.Marshal(env)
	return string(jsonPayload)
}

func (order *OrderRejected) ToBytes() []byte {
	envelope := Envelope{}
	env := envelope.New("order_rejected", order)
	jsonPayload, _ := json.Marshal(env)
	return jsonPayload
}
