package iggcon

type ConsumerKind int

const (
	ConsumerSingle ConsumerKind = 1
	ConsumerGroup  ConsumerKind = 2
)

type Consumer struct {
	Kind ConsumerKind
	Id   Identifier
}
