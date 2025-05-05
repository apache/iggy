package iggcon

import "errors"

type HeaderValue struct {
	Kind  HeaderKind
	Value []byte
}

type HeaderKey struct {
	Value string
}

func NewHeaderKey(val string) (HeaderKey, error) {
	if len(val) == 0 || len(val) > 255 {
		return HeaderKey{}, errors.New("Value has incorrect size, must be between 1 and 255")
	}
	return HeaderKey{Value: val}, nil
}

type HeaderKind int

const (
	Raw     HeaderKind = 1
	String  HeaderKind = 2
	Bool    HeaderKind = 3
	Int8    HeaderKind = 4
	Int16   HeaderKind = 5
	Int32   HeaderKind = 6
	Int64   HeaderKind = 7
	Int128  HeaderKind = 8
	Uint8   HeaderKind = 9
	Uint16  HeaderKind = 10
	Uint32  HeaderKind = 11
	Uint64  HeaderKind = 12
	Uint128 HeaderKind = 13
	Float   HeaderKind = 14
	Double  HeaderKind = 15
)
