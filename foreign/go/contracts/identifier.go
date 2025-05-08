package iggcon

type Identifier struct {
	Kind   IdKind
	Length int
	Value  any
}

type IdKind int

const (
	NumericId IdKind = 1
	StringId  IdKind = 2
)

func NewIdentifier(id any) Identifier {
	var kind IdKind
	var length int

	switch v := id.(type) {
	case int:
		kind = NumericId
		length = 4
	case string:
		kind = StringId
		length = len(v)
	}

	return Identifier{
		Kind:   kind,
		Length: length,
		Value:  id,
	}
}
