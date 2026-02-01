package dbquery

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbconstant"
	"github.com/teru01/simpledb-go/dbrecord"
)

type Predicate struct {
	terms []*Term
}

func NewPredicate(terms ...*Term) *Predicate {
	return &Predicate{terms: terms[:]}
}

// otherから条件を抜き出し結合する
func (p *Predicate) ConjoinWith(other *Predicate) {
	p.terms = append(p.terms, other.terms...)
}

// termはAND結合
func (p *Predicate) IsSatisfied(ctx context.Context, s Scan) (bool, error) {
	for _, term := range p.terms {
		satisfied, err := term.IsSatisfied(ctx, s)
		if err != nil {
			return false, fmt.Errorf("is satisfied: %w", err)
		}
		if !satisfied {
			return false, nil
		}
	}
	return true, nil
}

func (p *Predicate) ReductionFactor(plan Plan) int {
	factor := 1
	for _, term := range p.terms {
		factor *= term.ReductionFactor(plan)
	}
	return factor
}

func (p *Predicate) SelectSubPredicate(schema *dbrecord.Schema) *Predicate {
	result := NewPredicate()
	for _, term := range p.terms {
		if term.AppliesTo(schema) {
			result.terms = append(result.terms, term)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

func (p *Predicate) JoinSubPredicate(schema1 *dbrecord.Schema, schema2 *dbrecord.Schema) *Predicate {
	result := NewPredicate()
	newSchema := dbrecord.NewSchema()
	newSchema.AddAll(schema1)
	newSchema.AddAll(schema2)

	for _, term := range p.terms {
		if !term.AppliesTo(schema1) && !term.AppliesTo(schema2) && term.AppliesTo(newSchema) {
			result.terms = append(result.terms, term)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

func (p *Predicate) EquatesWithConstant(fieldName string) dbconstant.Constant {
	for _, term := range p.terms {
		constant := term.EquatesWithConstant(fieldName)
		if constant != nil {
			return constant
		}
	}
	return nil
}

func (p *Predicate) EquatesWithFieldName(fieldName string) string {
	for _, term := range p.terms {
		fieldName := term.EquatesWithFieldName(fieldName)
		if fieldName != "" {
			return fieldName
		}
	}
	return ""
}

func (p *Predicate) String() string {
	result := ""
	for _, term := range p.terms {
		result += term.String() + " AND "
	}
	return result[:len(result)-5]
}
