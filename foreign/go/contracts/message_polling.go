package iggcon

type PollingStrategy struct {
	Kind  MessagePolling
	Value uint64
}

type MessagePolling int

const (
	POLLING_OFFSET    MessagePolling = 1
	POLLING_TIMESTAMP MessagePolling = 2
	POLLING_FIRST     MessagePolling = 3
	POLLING_LAST      MessagePolling = 4
	POLLING_NEXT      MessagePolling = 5
)

func NewPollingStrategy(kind MessagePolling, value uint64) PollingStrategy {
	return PollingStrategy{
		Kind:  kind,
		Value: value,
	}
}

func OffsetPollingStrategy(value uint64) PollingStrategy {
	return NewPollingStrategy(POLLING_OFFSET, value)
}

func TimestampPollingStrategy(value uint64) PollingStrategy {
	return NewPollingStrategy(POLLING_TIMESTAMP, value)
}

func FirstPollingStrategy() PollingStrategy {
	return NewPollingStrategy(POLLING_FIRST, 0)
}

func LastPollingStrategy() PollingStrategy {
	return NewPollingStrategy(POLLING_LAST, 0)
}

func NextPollingStrategy() PollingStrategy {
	return NewPollingStrategy(POLLING_NEXT, 0)
}
