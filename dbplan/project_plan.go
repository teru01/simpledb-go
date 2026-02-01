package dbplan

import (
	"context"

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

func (p *ProjectPlan) Open(ctx context.Context) dbquery.Scan {
	return dbquery.NewProjectScan(p.child.Open(ctx), p.schema.Fields())
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
