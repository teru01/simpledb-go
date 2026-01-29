package dbquery

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbrecord"
)

type Term struct {
	lhs Expression
	rhs Expression
}

func NewTerm(lhs Expression, rhs Expression) *Term {
	return &Term{lhs: lhs, rhs: rhs}
}

func (t *Term) IsSatisfied(ctx context.Context, s Scan) (bool, error) {
	lhs, err := t.lhs.Evaluate(ctx, s)
	if err != nil {
		return false, fmt.Errorf("evaluate lhs: %w", err)
	}
	rhs, err := t.rhs.Evaluate(ctx, s)
	if err != nil {
		return false, fmt.Errorf("evaluate rhs: %w", err)
	}
	return lhs.Equals(rhs), nil
}

func (t *Term) AppliesTo(schema *dbrecord.Schema) bool {
	return t.lhs.AppliesTo(schema) && t.rhs.AppliesTo(schema)
}

func (t *Term) EquatesWithConstant(fieldName string) (dbconstant.Constant, error) {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant(), nil
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.lhs.AsConstant(), nil
	}
	return nil, fmt.Errorf("field %q not found", fieldName)
}

func (t *Term) EquatesWithFieldName(fieldName string) (string, error) {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName(), nil
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && t.lhs.IsFieldName() {
		return t.lhs.AsFieldName(), nil
	}
	return "", fmt.Errorf("field %q not found", fieldName)
}

func (t *Term) String() string {
	return fmt.Sprintf("%s = %s", t.lhs.String(), t.rhs.String())
}
