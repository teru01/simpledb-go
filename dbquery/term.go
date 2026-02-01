package dbquery

import (
	"context"
	"fmt"
	"math"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbrecord"
)

type Term struct {
	lhs *Expression
	rhs *Expression
}

func NewTerm(lhs *Expression, rhs *Expression) *Term {
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

func (t *Term) ReductionFactor(plan Plan) int {
	if t.lhs.IsFieldName() && t.rhs.IsFieldName() {
		return int(math.Max(float64(plan.DistinctValues(t.lhs.AsFieldName())), float64(plan.DistinctValues(t.rhs.AsFieldName()))))
	}
	if t.lhs.IsFieldName() {
		return plan.DistinctValues(t.lhs.AsFieldName())
	}
	if t.rhs.IsFieldName() {
		return plan.DistinctValues(t.rhs.AsFieldName())
	}
	if t.lhs.AsConstant().Equals(t.rhs.AsConstant()) {
		return 1
	}
	return math.MaxInt
}

// 右辺か左辺がfieldNameと一致するときもう片方が定数ならそれを返す.それ以外はnil
func (t *Term) EquatesWithConstant(fieldName string) dbconstant.Constant {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && !t.rhs.IsFieldName() {
		return t.rhs.AsConstant()
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && !t.lhs.IsFieldName() {
		return t.lhs.AsConstant()
	}
	return nil
}

// 右辺か左辺がfieldNameと一致するときもう片方がfield nameならそれを返す.それ以外は空文字
func (t *Term) EquatesWithFieldName(fieldName string) string {
	if t.lhs.IsFieldName() && t.lhs.AsFieldName() == fieldName && t.rhs.IsFieldName() {
		return t.rhs.AsFieldName()
	}
	if t.rhs.IsFieldName() && t.rhs.AsFieldName() == fieldName && t.lhs.IsFieldName() {
		return t.lhs.AsFieldName()
	}
	return ""
}

func (t *Term) String() string {
	return fmt.Sprintf("%s = %s", t.lhs.String(), t.rhs.String())
}
