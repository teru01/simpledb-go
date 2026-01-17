package dbconstant

type Constant interface {
	AsRaw() any
}

type IntConstant struct {
	value int
}

func (c *IntConstant) AsRaw() any {
	return c.value
}

func NewIntConstant(value int) *IntConstant {
	return &IntConstant{value: value}
}

type StringConstant struct {
	value string
}

func (c *StringConstant) AsRaw() any {
	return c.value
}

func NewStringConstant(value string) *StringConstant {
	return &StringConstant{value: value}
}
