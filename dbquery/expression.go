package dbquery

import (
	"context"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbrecord"
)

type Expression struct {
	value     dbconstant.Constant
	fieldName string
}

func NewExpressionFromValue(value dbconstant.Constant) *Expression {
	return &Expression{value: value}
}

func NewExpressionFromFieldName(fieldName string) *Expression {
	return &Expression{fieldName: fieldName}
}

func (e *Expression) IsFieldName() bool {
	return e.fieldName != ""
}

func (e *Expression) Evaluate(ctx context.Context, s Scan) (dbconstant.Constant, error) {
	if e.IsFieldName() {
		return s.GetVal(ctx, e.fieldName)
	}
	return e.value, nil
}

func (e *Expression) String() string {
	if e.IsFieldName() {
		return e.fieldName
	}
	return e.value.String()
}

func (e *Expression) AsConstant() dbconstant.Constant {
	return e.value
}

func (e *Expression) AsFieldName() string {
	return e.fieldName
}

func (e *Expression) AppliesTo(schema *dbrecord.Schema) bool {
	if e.value != nil {
		return true
	}
	return schema.HasField(e.fieldName)
}
