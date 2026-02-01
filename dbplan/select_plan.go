package dbplan

import (
	"context"
	"fmt"
	"math"

	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
)

type SelectPlan struct {
	child     dbquery.Plan
	predicate *dbquery.Predicate
}

func NewSelectPlan(child dbquery.Plan, pred *dbquery.Predicate) *SelectPlan {
	return &SelectPlan{child: child, predicate: pred}
}

func (s *SelectPlan) DistinctValues(fieldName string) int {
	if s.predicate.EquatesWithConstant(fieldName) != nil {
		return 1
	}
	fieldName2 := s.predicate.EquatesWithFieldName(fieldName)
	if fieldName2 != "" {
		return int(math.Min(float64(s.child.DistinctValues(fieldName)), float64(s.child.DistinctValues(fieldName2))))
	}
	// V(child, F) if F != fieldName 子の分布と同じ
	return s.child.DistinctValues(fieldName)
}

func (s *SelectPlan) Open(ctx context.Context) (dbquery.Scan, error) {
	scan, err := s.child.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open child: %w", err)
	}
	return dbquery.NewSelectScan(scan, s.predicate), nil
}

func (s *SelectPlan) BlockAccessed() int {
	return s.child.BlockAccessed()
}

func (s *SelectPlan) RecordsOutput() int {
	return s.child.RecordsOutput() / s.predicate.ReductionFactor(s.child)
}

func (s *SelectPlan) Schema() *dbrecord.Schema {
	return s.child.Schema()
}
