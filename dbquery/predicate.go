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

func (p *Predicate) ConjoinWith(other Predicate) {
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

func (p *Predicate) EquatesWithConstant(fieldName string) (dbconstant.Constant, error) {
	for _, term := range p.terms {
		constant, err := term.EquatesWithConstant(fieldName)
		if err != nil {
			return nil, fmt.Errorf("equates with constant: %w", err)
		}
		if constant != nil {
			return constant, nil
		}
	}
	return nil, fmt.Errorf("field %q not found", fieldName)
}

func (p *Predicate) EquatesWithFieldName(fieldName string) (string, error) {
	for _, term := range p.terms {
		fieldName, err := term.EquatesWithFieldName(fieldName)
		if err != nil {
			return "", fmt.Errorf("equates with field name: %w", err)
		}
		if fieldName != "" {
			return fieldName, nil
		}
	}
	return "", fmt.Errorf("field %q not found", fieldName)
}

func (p *Predicate) String() string {
	result := ""
	for _, term := range p.terms {
		result += term.String() + " AND "
	}
	return result[:len(result)-5]
}
