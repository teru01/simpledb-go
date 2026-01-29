package dbconstant

import (
	"strconv"
	"strings"
)

type Constant interface {
	AsRaw() any
	String() string
	Compare(other Constant) int
	Equals(other Constant) bool
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

func (c *IntConstant) String() string {
	return strconv.Itoa(c.value)
}

func (c *IntConstant) Compare(other Constant) int {
	otherInt, ok := other.AsRaw().(int)
	if !ok {
		return -1
	}
	return c.value - otherInt
}

func (c *IntConstant) Equals(other Constant) bool {
	return c.Compare(other) == 0
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

func (c *StringConstant) String() string {
	return c.value
}

func (c *StringConstant) Compare(other Constant) int {
	otherString, ok := other.AsRaw().(string)
	if !ok {
		return 1
	}
	return strings.Compare(c.value, otherString)
}

func (c *StringConstant) Equals(other Constant) bool {
	return c.Compare(other) == 0
}
