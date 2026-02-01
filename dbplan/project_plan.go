package dbplan

import (
	"context"
	"fmt"

	"github.com/teru01/simpledb-go/dbquery"
	"github.com/teru01/simpledb-go/dbrecord"
)

type ProjectPlan struct {
	child  dbquery.Plan
	schema *dbrecord.Schema
}

func NewProjectPlan(child dbquery.Plan, fieldList []string) *ProjectPlan {
	s := dbrecord.NewSchema()
	for _, fieldName := range fieldList {
		s.Add(fieldName, child.Schema())
	}
	return &ProjectPlan{child: child, schema: s}
}

func (p *ProjectPlan) Open(ctx context.Context) (dbquery.Scan, error) {
	scan, err := p.child.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("open child: %w", err)
	}
	return dbquery.NewProjectScan(scan, p.schema.Fields()), nil
}

func (p *ProjectPlan) BlockAccessed() int {
	return p.child.BlockAccessed()
}

func (p *ProjectPlan) RecordsOutput() int {
	return p.child.RecordsOutput()
}

func (p *ProjectPlan) DistinctValues(fieldName string) int {
	return p.child.DistinctValues(fieldName)
}

func (p *ProjectPlan) Schema() *dbrecord.Schema {
	return p.schema
}
